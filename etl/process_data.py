from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import udf


def find_attack_names(df):
    """
    Function to find unique attack names
    :param df: Total data
    :return: Unique attacks
    """
    distinct_attacks = [x.attack for x in df.select('attack').distinct().collect()]
    return distinct_attacks


def write_attack_counts(df, output, col):
    """
    Writes down unique attacks with counts as csv
    :param df: Input data
    :param output: The output folder to save in
    :param col: input column name
    :return: Writes the output to csv with the output column
    """
    counts_attack = df.groupby(df[col]).agg(functions.count(df[col])).orderBy(df[col]).coalesce(1)
    counts_attack.write.format("com.databricks.spark.csv").option("header", "true").save(output,
                                                                                         mode="overwrite")


def drop_selected_data(df, col, data_pt, spark, keep=3000):
    """
    Removing extra entries for balancing the data (keeps first "keep" entries)
    :param df: Input data
    :param col: column with extra entries
    :param data_pt: name of the item which has extra entries
    :param keep: number of records to keep
    :return: output data with less records
    """
    df.createOrReplaceTempView("data_view")
    orig = spark.sql("select * from data_view where {}!='{}' ".format(col, data_pt))
    selected = spark.sql("select * from data_view where {}='{}' limit {}".format(col, data_pt, keep))
    final = orig.union(selected)
    return final


def udf_categories(x):
    """
    User defined functions to filter categories based on different kinds of attacks
    Maps to the dictionary in python
    :param x: Input
    :return: mapped output
    """
    d = {'dos_attacks': ['apache2', 'back', 'land', 'neptune', 'mailbomb', 'pod', 'processtable', 'smurf', 'teardrop',
                         'udpstorm', 'worm'],
         'probe_attacks': ['ipsweep', 'mscan', 'nmap', 'portsweep', 'saint', 'satan'],
         'privilege_attacks': ['buffer_overflow', 'loadmdoule', 'perl', 'ps', 'rootkit', 'sqlattack', 'xterm'],
         'access_attacks': ['ftp_write', 'guess_passwd', 'http_tunnel', 'imap', 'multihop', 'named', 'phf', 'sendmail',
                            'snmpgetattack', 'snmpguess', 'spy', 'warezclient', 'warezmaster', 'xclock', 'xsnoop'],
         'normal': ['normal']}
    for k, v in d.items():
        if x.strip().lower() in v:
            return k


def find_categories(df):
    """
    Specific function for mapping a udf to find broad attack categories
    :param df: Input data
    :return: Data with broad categories in a column name "categories"
    """
    xdf = udf(lambda x: udf_categories(x), types.StringType())
    df = df.withColumn('category', xdf(df['attack']))
    df = df.where(df['category'].isNotNull())
    return df


def augment_and_process_data(data, spark):
    """
    Does data processing:
    1. Limits the number of neptune attacks
    2. Broadly categorizes attacks to categories for classification
    3. Selects top x rows of normal entries and randomly puts them as attacks for class balance
    4. Uses the remaining some y rows from total-x rows for normal entries
    :param data: Input data to be processed
    :param spark: Spark session object
    :return: Processed dataframe
    """
    df = data.dropDuplicates()
    df = df.withColumn("attack", functions.regexp_replace(df['attack'], "\.", ""))
    df = drop_selected_data(df, "attack", 'neptune', spark, keep=20000)
    df = find_categories(df).cache()
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
    return final


def processing_pipeline(data, spark):
    """
    A simple processing pipeline to be called during training
    :param data: Input data
    :return: Processed data with broad categories
    """
    df = augment_and_process_data(data, spark)
    return df
