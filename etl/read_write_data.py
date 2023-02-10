import configparser
import sys
from pyspark.sql import SparkSession, types

# Overview:
# Code to read data

db_properties = {}
db_url = 'jdbc:postgresql://localhost:5432/postgres'
db_properties['url'] = db_url
db_properties['driver'] = 'org.postgresql.Driver'

spark = SparkSession.builder.appName("Read Data").getOrCreate()

# The KDD data schema
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
    types.StructField('attack', types.StringType(), nullable=True),
])


def read_data_from_s3(path, schema=data_schema):
    """
    Reads data from s3
    Takes in the configurations from Config file and reads data from s3.
    Make sure to pass --packages=org.apache.hadoop:hadoop-aws:3.1.2 in args
    :param path: s3 bucket path
    :param schema: schema of data (if reading some other data)
    :return: Spark Data Frame data
    """
    config_path = sys.argv[1]
    config = configparser.ConfigParser()
    config.read(config_path)
    KEYS = config["DEFAULT"]
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", KEYS["access_key"])
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", KEYS["secret_key"])
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    data = spark.read.csv(header=False, path=path, schema=schema)
    return data


def read_data_csv(path, schema=data_schema):
    """
    Reads data from a csv/text file
    :param path: input path of the file
    :param schema: Data schema to be followed
    :return: Data as a Spark Dataframe
    """
    data = spark.read.csv(header=False, path=path, schema=schema)
    return data


def read_data_db_chunks(table_name):
    """
    If there is a memory error while reading the file, you can use this function.
    Have hardcoded limit and offset end point to avoid python calculations on spark dataframes
    :param table_name: Name of the table to fetch data from
    :return: Data in a spark dataframe
    """
    limit = 100000
    offset = 0
    emptyRDD = spark.sparkContext.emptyRDD()
    df = spark.createDataFrame(emptyRDD, data_schema)
    while offset <= 4900000:
        temp = spark.read.format("jdbc").option("url", db_url).option("query",
                                                                      "select * from {} limit {} offset {}".format(
                                                                          table_name, limit, offset)).load()
        offset = offset + limit
        df = df.union(temp)
        del temp


def read_data_db(table_name, partitions=10):
    """
    To read the data from database
    :param table_name: Name of the table
    :param partitions: (optional) if you want to partition your input
    :return: Spark Dataframe
    """
    df = spark.read.jdbc(table=table_name, url=db_url, numPartitions=partitions)
    return df


def write_data_db(df, table_name, mode='append'):
    """
     To write data to a table in the DB
    :param df: Input Data to be written
    :param table_name: Table name
    :param mode: If you want it "append" or "overwrite"
    """
    df.write.jdbc(url=db_url, table=table_name, mode=mode, properties=db_properties)
