import configparser
import os
from datetime import datetime

import boto3
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession, types

db_properties = {}
db_url = 'jdbc:postgresql://localhost:5432/postgres'
db_properties['url'] = db_url
db_properties['driver'] = 'org.postgresql.Driver'


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


def get_data_schema() -> types.StructType:
    """
    Schema to read the input dataset CSV file
    :return: Data Schema
    """
    data_schema = types.StructType([
        types.StructField('duration', types.FloatType(), nullable=True),
        types.StructField('protocol_type', types.StringType(), nullable=True),
        types.StructField('service', types.StringType(), nullable=True),
        types.StructField('flag', types.StringType(), nullable=True),
        types.StructField('src_bytes', types.FloatType(), nullable=True),
        types.StructField('dst_bytes', types.FloatType(), nullable=True),
        types.StructField('land', types.FloatType(), nullable=True),
        types.StructField('wrong_fragment', types.FloatType(), nullable=True),
        types.StructField('urgent', types.FloatType(), nullable=True),
        types.StructField('hot', types.FloatType(), nullable=True),
        types.StructField('num_failed_logins', types.FloatType(), nullable=True),
        types.StructField('logged_in', types.FloatType(), nullable=True),
        types.StructField('num_compromised', types.FloatType(), nullable=True),
        types.StructField('root_shell', types.FloatType(), nullable=True),
        types.StructField('su_attempted', types.FloatType(), nullable=True),
        types.StructField('num_root', types.LongType(), nullable=True),
        types.StructField('num_file_creations', types.LongType(), nullable=True),
        types.StructField('num_shells', types.LongType(), nullable=True),
        types.StructField('num_access_files', types.LongType(), nullable=True),
        types.StructField('num_outbound_cmds', types.LongType(), nullable=True),
        types.StructField('is_host_login', types.FloatType(), nullable=True),
        types.StructField('is_guest_login', types.FloatType(), nullable=True),
        types.StructField('count', types.LongType(), nullable=True),
        types.StructField('srv_count', types.LongType(), nullable=True),
        types.StructField('serror_rate', types.FloatType(), nullable=True),
        types.StructField('srv_serror_rate', types.FloatType(), nullable=True),
        types.StructField('rerror_rate', types.FloatType(), nullable=True),
        types.StructField('srv_rerror_rate', types.FloatType(), nullable=True),
        types.StructField('same_srv_rate', types.FloatType(), nullable=True),
        types.StructField('diff_srv_rate', types.FloatType(), nullable=True),
        types.StructField('srv_diff_host_rate', types.FloatType(), nullable=True),
        types.StructField('dst_host_count', types.FloatType(), nullable=True),
        types.StructField('dst_host_srv_count', types.FloatType(), nullable=True),
        types.StructField('dst_host_same_srv_rate', types.FloatType(), nullable=True),
        types.StructField('dst_host_diff_srv_rate', types.FloatType(), nullable=True),
        types.StructField('dst_host_same_src_port_rate', types.FloatType(), nullable=True),
        types.StructField('dst_host_srv_diff_host_rate', types.FloatType(), nullable=True),
        types.StructField('dst_host_serror_rate', types.FloatType(), nullable=True),
        types.StructField('dst_host_srv_serror_rate', types.FloatType(), nullable=True),
        types.StructField('dst_host_rerror_rate', types.FloatType(), nullable=True),
        types.StructField('dst_host_srv_rerror_rate', types.FloatType(), nullable=True),
        types.StructField('attack', types.StringType(), nullable=True)
    ])
    return data_schema


def read_data_from_s3(path, schema=get_data_schema()):
    """
    Reads data from s3
    Takes in the configurations from Config file and reads data from s3.
    Make sure to pass --packages=org.apache.hadoop:hadoop-aws:3.1.2 in args
    :param path: s3 bucket path
    :param schema: schema of data (if reading some other data)
    :return: Spark Data Frame data
    """
    # config_path = sys.argv[1]
    # config = configparser.ConfigParser()
    # config.read(config_path)
    # KEYS = config["DEFAULT"]
    # spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", KEYS["access_key"])
    # spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", KEYS["secret_key"])
    # spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    # data = spark.read.csv(header=False, path=path, schema=schema)
    start_time = datetime.now()
    dataframe = create_dataframe_from_input_dataset(path)
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("read_write_data, read_data_from_s3, Function to initiate data reading from S3,"
                       " {} \n".format((end_time - start_time).total_seconds()))
    return dataframe


def read_data_csv(path, spark, schema=get_data_schema()):
    """
    Reads data from a csv/text file
    :param path: input path of the file
    :param schema: Data schema to be followed
    :return: Data as a Spark Dataframe
    """
    load_configurations()
    start_time = datetime.now()
    data = spark.read.csv(header=False, sep=',', path=path, schema=schema)
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("read_write_data, read_data_csv, Load dataframe from the input file,"
                       " {} \n".format((end_time - start_time).total_seconds()))
    return data


def read_data_db_chunks(table_name, spark):
    """
    If there is a memory error while reading the file, you can use this function.
    Have hardcoded limit and offset end point to avoid python calculations on spark dataframes
    :param table_name: Name of the table to fetch data from
    :return: Data in a spark dataframe
    """
    start_time = datetime.now()
    limit = 100000
    offset = 0
    emptyRDD = spark.sparkContext.emptyRDD()
    df = spark.createDataFrame(emptyRDD, get_data_schema())
    while offset <= 4900000:
        temp = spark.read.format("jdbc").option("url", db_url).option("query",
                                                                      "select * from {} limit {} offset {}".format(
                                                                          table_name, limit, offset)).load()
        offset = offset + limit
        df = df.union(temp)
        del temp
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("read_write_data, read_data_db_chunks, To avoid memory error, read db from chunks,"
                       " {} \n".format((end_time - start_time).total_seconds()))
    return df


def read_data_db(table_name, spark, partitions=10):
    """
    To read the data from database
    :param table_name: Name of the table
    :param partitions: (optional) if you want to partition your input
    :return: Spark Dataframe
    """
    start_time = datetime.now()
    df = spark.read.jdbc(table=table_name, url=db_url, numPartitions=partitions)
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("read_write_data, read_data_db, Read the data from database, "
                       "{} \n".format((end_time - start_time).total_seconds()))
    return df


def write_data_db(df, table_name, mode='append'):
    """
     To write data to a table in the DB
    :param df: Input Data to be written
    :param table_name: Table name
    :param mode: If you want it "append" or "overwrite"
    """
    start_time = datetime.now()
    df.write.jdbc(url=db_url, table=table_name, mode=mode, properties=db_properties)
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("read_write_data, write_data_db({}), Write data to db table,"
                       " {} \n".format(table_name, (end_time - start_time).total_seconds()))


def read_dataset_s3_compressed(path) -> SparkDataFrame:
    """
    Read input dataset from S3 in compressed format and store locally
    :return: Input dataset dataframe
    """
    start_time = datetime.now()
    s3 = boto3.resource('s3', aws_access_key_id=S3_CREDS["access_key"],
                        aws_secret_access_key=S3_CREDS["secret_key"])
    bucket = s3.Bucket(S3_BUCKET["bucket_name"])
    del s3
    obj = bucket.Object(key=S3_BUCKET["object_path"])
    del bucket
    response = obj.get()
    del obj
    gz_file = response['Body'].read()
    del response
    with open(path, 'wb') as f:
        f.write(gz_file)
    del gz_file
    dataframe = read_data_csv(path)
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("read_write_data, read_dataset_s3_compressed, Read compressed data from S3 "
                       ",{} \n".format((end_time-start_time).total_seconds()))
    del start_time, end_time
    return dataframe


def if_local_dataset_exists(path) -> bool:
    """
    Check if the local dataset copy exists
    :return: Boolean exists
    """
    return os.path.exists(path)


def read_data_from_local(path) -> SparkDataFrame:
    """
    Load the local dataset to a dataframe
    :return: Input dataset dataframe
    """
    start_time = datetime.now()
    dataframe = read_data_csv(path)
    end_time = datetime.now()
    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("read_write_data, read_data_from_local, Read data from local copy,"
                       " {} \n".format((end_time-start_time).total_seconds()))
    del start_time, end_time
    return dataframe


def create_dataframe_from_input_dataset(path, force: bool=False):
    """
    Returns a dataframe loaded with input dataset
    :param force: Overwrite existing local copy
    :return: Input Dataset Dataframe
    """
    global spark
    spark = SparkSession.builder.appName("load input dataset").getOrCreate()
    start_time = datetime.now()
    load_configurations()
    global time_logs

    if if_local_dataset_exists(path) and not force:
        dataframe = read_data_from_local(path)
    else:
        dataframe = read_dataset_s3_compressed(path)
    end_time = datetime.now()

    if time_logs:
        with open("performance_analysis.txt", "a") as file:
            file.write("read_write_data, create_dataframe_from_input_dataset, "
                       "Create dataframe from the input dataset, {} \n".format((end_time - start_time).total_seconds()))

    del start_time, end_time
    return dataframe


# if __name__ == '__main__':
#     spark = SparkSession.builder.appName("load input dataset").getOrCreate()
#     global S3_CREDS, S3_BUCKET, TIME_LOGS, time_logs
#     load_configurations()
#     dataframe = read_data_from_s3("kddcup.data.gz")
#     del spark, dataframe, S3_CREDS, S3_BUCKET, TIME_LOGS, time_logs

# time spark-submit --packages=org.apache.hadoop:hadoop-aws:3.3.1 etl_v2/read_write_data.py
