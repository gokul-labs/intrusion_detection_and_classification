import configparser
from datetime import datetime
from typing import Tuple, List, Text

import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import translate, col
from pyspark.sql.functions import udf
from etl_v2.read_write_data import create_dataframe_from_input_dataset


def load_configurations():
    """
    Load the program configurations from config.ini file
    :return:
    """
    config_path = "config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    global S3_CREDS, S3_BUCKET, TIME_LOGS, time_logs
    S3_CREDS = config["S3"]
    S3_BUCKET = config["BUCKET"]
    TIME_LOGS = config["LOGS"]
    time_logs = True if TIME_LOGS["time_logs"] == "True" or TIME_LOGS["time_logs"] == "true" else False
    del config_path, config


def process_attack_column(dataframe: SparkDataFrame, spark: SparkSession, attack_threshold: int=10,
                          attack_stats: bool=False, ) -> SparkDataFrame:
    """
    Perform ETL wrt the attack column
    :param spark: Spark session object
    :param dataframe: input dataframe
    :param attack_threshold: threshold below with attack types will be dropped
    :param attack_stats: boolean flag to toggle creating attack statisitics
    :return: Modified dataframe with the attack column processed
    """
    load_configurations()
    NEPTUNE_ATTACK_MAX_COUNT = 20000
    NORMAL_MAX_COUNT = 100000
    ATTACK = "attack"
    NEPTUNE = "neptune"
    NORMAL = "normal"

    start_time = datetime.now()
    dataframe = dataframe.withColumn("attack", translate("attack", ".", "")).cache()
    dataframe = drop_selected_data(dataframe, ATTACK, NEPTUNE, spark, NEPTUNE_ATTACK_MAX_COUNT)
    # Normal attacks will be taken care in a separate function below
    # dataframe = drop_selected_data(dataframe, ATTACK, NORMAL, spark, NORMAL_MAX_COUNT)


    # The following steps take the long time. I have already run it and found that
    # certain attack types can be filtered in this process for the given dataset due to
    # low frequency and insufficient training records below a configurable threshold. Hence
    # we drop the feature directly. For a new dataset, we can use the functions to arrive at the result.

    # attacks, counts = get_attack_counts_pandas(dataframe)
    # drop_list = []
    # for i in range(len(counts)):
    #     if counts[i] < attack_threshold:
    #         drop_list.append(attacks[i])

    drop_list = ["perl", "multihop", "spy", "ftp_write", "loadmodule", "phf"]
    dataframe = dataframe.filter(~ col("attack").isin(drop_list))
    dataframe = map_attack_categories(dataframe).cache()

    # Attack statistics have separate analysis both in Tableau dashboard and in custom created
    # colab notebooks. Hence, we can ignore the below one for now.

    # if attack_stats:
    #     show_attack_counts(attacks, counts, console_display=False, artefact_name="before_attack_processing.jpg")

    end_time = datetime.now()

    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, process_attack_column, Perform all attack related"
                  "ETL operations, {} \n".format((end_time - start_time).total_seconds()))
    del start_time, end_time, drop_list, attack_stats
    return dataframe


def drop_selected_data(dataframe: SparkDataFrame, column_name: Text, column_value: Text, spark: SparkSession,
                       keep: int = 3000) -> SparkDataFrame:
    """
    Drop data to reduce skewed data distribution
    :param spark: SparkSession object
    :param dataframe: input dataframe
    :param column_name: Column name to apply transformation
    :param column_value: column value / type to filter
    :param keep: retain all but "keep" records
    :return: Modified dataframe with the intended data dropped
    """
    start_time = datetime.now()
    dataframe.createOrReplaceTempView("data_view")
    orig = spark.sql("select * from data_view where {}!='{}' ".format(column_name, column_value))
    selected = spark.sql("select * from data_view where {}='{}' limit {}".format(column_name, column_value, keep))
    final = orig.union(selected)
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, drop_selected_data, Drop data to reduce data skew, "
                  "{} \n".format((end_time - start_time).total_seconds()))
    del start_time, end_time, orig, selected
    return final


def get_attack_counts_pandas(dataframe: SparkDataFrame) -> Tuple[List[str], List[int]]:
    """
    Returns the attack counts for each attack type using pandas
    :param dataframe: input dataframe
    :return: Lists of attacks and corresponding counts
    """
    start_time = datetime.now()
    attack_count_df = dataframe.select("attack").groupby("attack").count().toPandas()
    attacks = list(np.array(attack_count_df.iloc[:, 0]))
    counts = list(np.array(attack_count_df.iloc[:, 1]))
    end_time = datetime.now()
    print((end_time-start_time).total_seconds())
    if time_logs:
        with open("performance_analysis_old.txt", "a") as file:
            file.write("process_data, get_attack_counts_pandas, Get the count of attacks by type, "
                  "{} \n".format((end_time - start_time).total_seconds()))
    return attacks, counts


def get_attack_counts(dataframe: SparkDataFrame) -> Tuple[List[str], List[int]]:
    """
    Returns the attack counts for each attack type
    :param dataframe: input dataframe
    :return: Lists of attacks and corresponding counts
    """
    attacks = []
    counts = []
    start_time = datetime.now()
    attack_count_df = dataframe.select("attack").groupby("attack").count().collect()
    end_time = datetime.now()
    for attack in attack_count_df:
        attacks.append(attack[0])
        counts.append(attack[1])
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, get_attack_counts, Get the count of attacks by type, "
                  "{} \n".format((end_time - start_time).total_seconds()))
    return attacks, counts


def show_attack_counts(attacks: List[str], counts: List[int], console_display: bool = False,
                       artefact_name: Text = "artefact-{}.jpg".format(datetime.now())):
    """
    Creates analysis or displays the attack statistics
    :param attacks: List of attacks
    :param counts: List of counts per attack
    :param console_display: Toggle to print stats to console
    :param artefact_name: Artefact name to save analysis
    :return:
    """
    if console_display:
        for i in range(len(attacks)):
            print("Attack: ", attacks[i], " Count: ", counts[i])
    else:
        plt.figure(figsize=(10, 5))
        plt.bar(attacks, counts, color='red')
        plt.savefig("analysis/{}".format(artefact_name))


def clean_data_without_distinct(dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Method to drop feature where only value is present in the entire dataset
    without using distinct
    :param dataframe: input dataframe
    :return: Modified dataframe without features with only one value
    """
    start_time = datetime.now()
    drop_list = []
    for column in dataframe.columns:
        distinct_count_of_column = len(dataframe.where(col(column) != dataframe.take(1)[0][column]).take(1))
        if distinct_count_of_column == 0:
            drop_list.append(column)
        del distinct_count_of_column
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, clean_data_without_distinct, Remove columns with single "
                       "value without using distinct, {} \n".format((end_time - start_time).total_seconds()))
    drop_list = ",".join(drop_list)
    result = dataframe.drop(drop_list)
    del start_time, end_time, drop_list
    return result


def remove_duplicate_records_distinct(dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Remove duplicates by using the distinct() method
    :param dataframe: input dataframe
    :return: Modified dataframe without duplicates
    """
    start_time = datetime.now()
    dataframe = dataframe.distinct()
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, remove_duplicate_records_distinct, Remove duplicate records,"
                       " {} \n".format((end_time - start_time).total_seconds()))
    del start_time, end_time
    return dataframe


def remove_duplicate_records_drop_duplicates(dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Remove duplicates by using the dropDuplicates() method
    :param dataframe: input dataframe
    :return: Modified dataframe without duplicates
    """
    start_time = datetime.now()
    dataframe = dataframe.dropDuplicates()
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, remove_duplicate_records_remove_duplicates, Remove duplicates using"
                  " drop duplicate method, {} \n".format((end_time - start_time).total_seconds()))
    del start_time, end_time
    return dataframe


def clean_data_distinct(dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Method to drop feature where only value is present in the entire dataset
    using distinct
    :param dataframe: input dataframe
    :return: Modified dataframe without features with only one value
    """
    drop_list = []
    start_time = datetime.now()
    for column in dataframe.columns:
        delta_df_count = dataframe.select(column).distinct().count()
        if delta_df_count == 1:
            drop_list.append(column)
        del delta_df_count
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, clean_data_distinct, Remove columns with single value"
                  "using distinct, {} \n".format((end_time - start_time).total_seconds()))
    drop_list = ",".join(drop_list)
    result = dataframe.drop(drop_list)
    del start_time, end_time, drop_list
    return result


def udf_categories(attack: Text) -> Text:
    """
    Returns the category of attack
    :param attack: attack name
    :return: attack type
    """
    attack_type_mapping = {
        'dos_attacks': ['apache2', 'back', 'land', 'neptune', 'mailbomb', 'pod', 'processtable', 'smurf', 'teardrop',
                        'udpstorm', 'worm'],
        'probe_attacks': ['ipsweep', 'mscan', 'nmap', 'portsweep', 'saint', 'satan'],
        'privilege_attacks': ['buffer_overflow', 'loadmdoule', 'perl', 'ps', 'rootkit', 'sqlattack', 'xterm'],
        'access_attacks': ['ftp_write', 'guess_passwd', 'http_tunnel', 'imap', 'multihop', 'named', 'phf', 'sendmail',
                           'snmpgetattack', 'snmpguess', 'spy', 'warezclient', 'warezmaster', 'xclock', 'xsnoop'],
        'normal': ['normal']}

    for attack_type, list_of_attacks_in_type in attack_type_mapping.items():
        if attack.strip().lower() in list_of_attacks_in_type:
            return attack_type


def find_categories(df):
    """
    Specific function for mapping a udf to find broad attack categories
    :param df: Input data
    :return: Data with broad categories in a column name "categories"
    """
    start_time = datetime.now()
    xdf = udf(lambda x: udf_categories(x), types.StringType())
    df = df.withColumn('category', xdf(df['attack']))
    df = df.where(df['category'].isNotNull())
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, find_categories, Find the attack category from attack type"
                  ", {} \n".format((end_time - start_time).total_seconds()))
    return df


def map_attack_categories(dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Map each attack to its category
    :param dataframe: input dataframe
    :return: Modified dataframe with category field
    """
    start_time = datetime.now()
    xdf = udf(lambda x: udf_categories(x))
    dataframe = dataframe.withColumn('category', xdf(col('attack')))
    dataframe = dataframe.where(dataframe['category'].isNotNull())
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, map_attack_categories, Find the attack category from attack type,"
                       " {} \n".format((end_time - start_time).total_seconds()))
    del start_time, xdf, end_time
    return dataframe


def additional_processing(dataframe: SparkDataFrame, spark: SparkSession) -> SparkDataFrame:
    """
    Returns the processed data
    :param spark: SparkSession object
    :type dataframe: Input dataset dataframe
    :return: Modified Dataframe after processing the raw input dataset
    """
    start_time = datetime.now()
    load_configurations()

    global time_logs

    # dataframe = create_dataframe_from_input_dataset(spark)
    dataframe = remove_duplicate_records_drop_duplicates(dataframe)

    # The following steps take the longest time. I have already run it and found that
    # one feature gets filtered in this process for the given dataset. Hence we drop the feature
    # directly. For a new dataset, we can use the functions to arrive at the result

    # dataframe = clean_data_without_distinct(dataframe)
    # dataframe = clean_data_distinct(dataframe)

    dataframe = dataframe.drop("num_outbound_cmds")
    end_time = datetime.now()

    time_logs = True if TIME_LOGS["time_logs"] == "True" or TIME_LOGS["time_logs"] == "true" else False
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("process_data, additional_processing, Perform initial"
                  "ETL processing steps, {} \n".format((end_time - start_time).total_seconds()))
    del start_time, end_time, time_logs
    return dataframe


def augment_and_process_data(df, spark):
    """
    Does data processing:
    1. Limits the number of neptune attacks
    2. Broadly categorizes attacks to categories for classification
    3. Selects top x rows of normal entries and randomly puts them as attacks for class balance
    4. Uses the remaining some y rows from total-x rows for normal entries
    :param dataframe: Input data to be processed
    :param spark: Spark session object
    :return: Processed dataframe
    """
    df = additional_processing(df, spark)
    df = process_attack_column(df, spark)
    df.createOrReplaceTempView("data_view")
    some_entries_df = spark.sql("select * from data_view where category='normal' limit 100000 ")
    remaining = df.withColumn("index", functions.monotonically_increasing_id())
    remaining = remaining.orderBy(functions.desc("index")).drop("index").limit(80000)
    no_normal = df.filter(df['category'] != 'normal')
    some_entries_df = some_entries_df.drop(some_entries_df["category"])
    some_entries_df = some_entries_df.withColumn(
        "category",
        functions.array(
            functions.lit("privilege_attacks"),
            functions.lit("probe_attacks"),
            functions.lit("access_attacks"),
        ).getItem(
            (functions.rand() * 3).cast("int")
        )
    )
    total = some_entries_df.union(remaining)
    final = no_normal.union(total)
    # analysis = final.groupby(final["category"]).agg(functions.count(final["category"]).alias("count"))
    del df, some_entries_df, remaining, no_normal, total
    return final


if __name__ == '__main__':
    spark = SparkSession.builder.appName("process dataset").getOrCreate()
    load_configurations()
    global S3_CREDS, S3_BUCKET, TIME_LOGS, time_logs
    dataframe = create_dataframe_from_input_dataset("kddcup.data.gz", spark)
    dataframe = augment_and_process_data(dataframe, spark)
    print(dataframe.count())
    del dataframe, S3_CREDS, S3_BUCKET, TIME_LOGS

# time spark-submit --packages=org.apache.hadoop:hadoop-aws:3.3.1 etl_v2/process_data.py
