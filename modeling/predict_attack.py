from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.feature import IndexToString


def predict(path, data):
    """
    Make predictions on the data using an existing model
    :param path: Path where the trained model resides
    :param data: Input data (test)
    :return: Predictions (entire row entries+predicted labels)
    """
    model = PipelineModel.load(path)
    predictions = model.transform(data)
    ind_str = IndexToString(inputCol='prediction', outputCol='category', labels=model.stages[0].labels)
    predictions = ind_str.transform(predictions)
    predictions = predictions.drop("protocol_new", "service_new", "flag_new", "features", "rawPrediction",
                                   "probability", "prediction")
    return predictions
