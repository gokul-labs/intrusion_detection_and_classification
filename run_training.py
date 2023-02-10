from datetime import datetime
from modeling.predict_attack import predict
from modeling.training import train_and_save_model, calculate_metrics
from etl_v2.process_data import augment_and_process_data
from etl_v2.read_write_data import read_data_csv, write_data_db, read_data_db, read_data_from_s3
from pyspark.sql import SparkSession, functions
from pyspark.sql import SQLContext
import gc

spark = SparkSession.builder.appName("Training").getOrCreate()


# spark.sparkContext.setSystemProperty('spark.executor.memory', '4g')


# Run using:
# spark-submit --packages=org.apache.hadoop:hadoop-aws:3.1.2 run_training.py
# spark-submit run_training.py
# spark-submit --driver-class-path tableau_driver/postgresql-42.3.1.jar run_training.py

def training_model_full_data(data, path='./final_model'):
    """
    Script to train the model
    :param data: Input data to train on
    :param path: Path to store the model
    """
    write_data_db(data, "raw_data", 'overwrite')
    start_time = datetime.now()
    df = augment_and_process_data(data, spark).cache()
    augment_data_end_time = datetime.now()
    write_data_db(df, "processed_data", 'overwrite')
    train_and_save_model(df, path)
    save_model_end_time = datetime.now()
    with open("performance_analysis.txt", "a") as file:
        file.write("run_training, training_model_full_data, "
                   "Create model from full training data, {} \n".format(
            (save_model_end_time - start_time).total_seconds()))
        file.write("run_training, augment_and_process_data, "
                   "Augment and process data for training, {} \n".format(
            (augment_data_end_time - start_time).total_seconds()))
        file.write("run_training, save_model, "
                   "Run the training on full data and save the generated model, {} \n".format(
            (save_model_end_time - augment_data_end_time).total_seconds()))


def finding_accuracy(data):
    """
    Splitting the input data into train/test and calculating accuracy. Data not saved.
    :param data: Input data
    """
    start_time = datetime.now()
    calculate_metrics(data)
    end_time = datetime.now()
    with open("performance_analysis.txt", "a") as file:
        file.write("run_training, finding_accuracy, "
                   "Generate new model with test train split and calc accuracy, {} \n".format(
            (end_time - start_time).total_seconds()))


def make_predictions(data, path_of_existing_model):
    """
    Saves the raw data in db
    Processes and make predictions
    Saves the predictions in db
    :param data: Input real data
    :param path_of_existing_model: Existing Model
    """
    write_data_db(data, "raw_data")
    start_time = datetime.now()
    predictions = predict(path_of_existing_model, data)
    end_time = datetime.now()
    with open("performance_analysis.txt", "a") as file:
        file.write("run_training, make_predictions, "
                   "Make inference from new data, {} \n".format((end_time - start_time).total_seconds()))
    write_data_db(predictions, "processed_data")


def free_spark_memory():
    spark.catalog.clearCache()
    SQLContext.getOrCreate(spark.sparkContext.getOrCreate()).clearCache()
    gc.collect()


if __name__ == '__main__':
    # df = read_data_db("processed_d ata", spark) # Use this with crontab
    df = read_data_csv('./kddcup.data.corrected', spark)  # Use this instead for day0 model
    # df = augment_and_process_data(df, spark)
    free_spark_memory()
    # df = read_data_from_s3("kddcup.data.gz")
    training_model_full_data(df, './final_model')
