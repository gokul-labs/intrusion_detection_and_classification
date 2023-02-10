from datetime import datetime

from modeling.predict_attack import predict
from modeling.training import train_and_save_model, calculate_metrics
from etl.process_data import augment_and_process_data
from etl.read_write_data import read_data_csv, write_data_db, read_data_db
from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.appName("Training").getOrCreate()


# Run using:
# spark-submit --packages=org.apache.hadoop:hadoop-aws:3.1.2 alternate_run_training.py
# spark-submit alternate_run_training.py
# spark-submit --driver-class-path tableau_driver/postgresql-42.3.1.jar alternate_run_training.py

def training_model_full_data(data, path='./final_model'):
    """
    Script to train the model
    :param data: Input data to train on
    :param path: Path to store the model
    """
    data = data.cache()
    write_data_db(data, "raw_data", 'overwrite')
    df = augment_and_process_data(data, spark).cache()
    write_data_db(df, "processed_data", 'overwrite')
    train_and_save_model(df, path)


def make_predictions(data, path_of_existing_model):
    """
    Saves the raw data in db
    Processes and make predictions
    Saves the predictions in db
    :param data: Input real data
    :param path_of_existing_model: Existing Model
    """
    data = data.cache()
    write_data_db(data, "raw_data")
    predictions = predict(path_of_existing_model, data)
    write_data_db(predictions, "processed_data")


if __name__ == '__main__':
    # df = read_data_db("processed_d ata", spark) # Use this with crontab
    df = read_data_csv('./kddcup.data.corrected') # Use this instead for day0 model
    # df=augment_and_process_data(df,spark)
    training_model_full_data(df, './final_model')
    # calculate_metrics(df)
