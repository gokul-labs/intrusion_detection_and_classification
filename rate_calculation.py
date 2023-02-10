from pyspark.sql import SparkSession,functions as F
import sys
db_properties = {}
db_url = 'jdbc:postgresql://localhost:5432/postgres'
db_properties['url'] = db_url
db_properties['driver'] = 'org.postgresql.Driver'

#  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --driver-class-path etl/postgresql-42.3.1.jar dynamic_predictions.py cyberproduce

def write_function(df, dfid):
    df.write.jdbc(url='jdbc:postgresql://localhost:5432/postgres', table='prediction_rate', mode='overwrite',
                  properties=db_properties)

def main(inputs):
    rate=0
    packets =   spark.readStream.format('kafka').option('kafka.bootstrap.servers','localhost:9092').option('subscribe',inputs).load()
    line    =   packets.select(packets['value'].cast('string'))
    split_col   =   F.split(line['value'],',')
    df  =   line.select(split_col.getItem(0).cast('float').alias("duration"),\
                        split_col.getItem(1).alias("protocol_type"),\
                        split_col.getItem(2).alias("service"),\
                        split_col.getItem(3).alias("flag"),\
                        split_col.getItem(4).cast('float').alias("src_bytes"),\
                        split_col.getItem(5).cast('float').alias("dst_bytes"),\
                        split_col.getItem(6).cast('float').alias("land"),\
                        split_col.getItem(7).cast('float').alias("wrong_fragment"),\
                        split_col.getItem(8).cast('float').alias("urgent"),\
                        split_col.getItem(9).cast('float').alias("hot"),\
                        split_col.getItem(10).cast('float').alias("num_failed_logins"),\
                        split_col.getItem(11).cast('float').alias("logged_in"),\
                        split_col.getItem(12).cast('float').alias("num_compromised"),\
                        split_col.getItem(13).cast('float').alias("root_shell"),\
                        split_col.getItem(14).cast('float').alias("su_attempted"),\
                        split_col.getItem(15).cast('long').alias("num_root"),\
                        split_col.getItem(16).cast('long').alias("num_file_creations"),\
                        split_col.getItem(17).cast('long').alias("num_shells"),\
                        split_col.getItem(18).cast('long').alias("num_access_files"),\
                        split_col.getItem(19).cast('long').alias("num_outbound_cmds"),\
                        split_col.getItem(20).cast('float').alias("is_host_login"),\
                        split_col.getItem(21).cast('float').alias("is_guest_login"),\
                        split_col.getItem(22).cast('long').alias("count"),\
                        split_col.getItem(23).cast('long').alias("srv_count"),\
                        split_col.getItem(24).cast('float').alias("serror_rate"),\
                        split_col.getItem(25).cast('float').alias("srv_serror_rate"),\
                        split_col.getItem(26).cast('float').alias("rerror_rate"),\
                        split_col.getItem(27).cast('float').alias("srv_rerror_rate"),\
                        split_col.getItem(28).cast('float').alias("same_srv_rate"),\
                        split_col.getItem(29).cast('float').alias("diff_srv_rate"),\
                        split_col.getItem(30).cast('float').alias("srv_diff_host_rate"),\
                        split_col.getItem(31).cast('float').alias("dst_host_count"),\
                        split_col.getItem(32).cast('float').alias("dst_host_srv_count"),\
                        split_col.getItem(33).cast('float').alias("dst_host_same_srv_rate"),\
                        split_col.getItem(34).cast('float').alias("dst_host_diff_srv_rate"),\
                        split_col.getItem(35).cast('float').alias("dst_host_same_src_port_rate"),\
                        split_col.getItem(36).cast('float').alias("dst_host_srv_diff_host_rate"),\
                        split_col.getItem(37).cast('float').alias("dst_host_serror_rate"),\
                        split_col.getItem(38).cast('float').alias("dst_host_srv_serror_rate"),\
                        split_col.getItem(39).cast('float').alias("dst_host_rerror_rate"),\
                        split_col.getItem(40).cast('float').alias("dst_host_srv_rerror_rate"),\
                        split_col.getItem(41).cast('int').alias("time_stamp"))
    time_grouped=df.groupBy(df["time_stamp"]).agg((F.count("time_stamp")).alias("rate"))
    # maxtime=time_grouped.select(F.max("time_stamp").alias("maxTime"))
    # joined=time_grouped.join(maxtime,[time_grouped["time_stamp"]==maxtime["maxTime"]])
    # time_grouped.writeStream.outputMode("Complete").format("console").start().awaitTermination(100)
    time_grouped.writeStream.outputMode("Complete").foreachBatch(write_function).start().awaitTermination(600)


if __name__=='__main__':
    inputs=sys.argv[1]
    spark = SparkSession.builder.appName('rate calc').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main(inputs)