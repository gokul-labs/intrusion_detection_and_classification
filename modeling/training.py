from pyspark.ml.classification import RandomForestClassifier, MultilayerPerceptronClassifier, LogisticRegression, \
    OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import when

def calculate_metrics(data):
    """
    Splits data into train and test and produces a confusion matrix for the created model
    :param data: Input data
    :return: Prints the accuracy score
    """
    training, validation = data.randomSplit([0.75, 0.25], seed=7)
    to_categorical = StringIndexer(inputCol="category", outputCol="category_new")
    proto_index = StringIndexer(inputCol="protocol_type", outputCol="protocol_new")
    service_index = StringIndexer(inputCol="service", outputCol="service_new")
    flag_index = StringIndexer(inputCol="flag", outputCol="flag_new")
    features_to_drop = ["category", "protocol_type", "service", "flag", "attack"]
    input_cols = list(set(training.columns).symmetric_difference(set(features_to_drop)))
    assemble_features = VectorAssembler(inputCols=input_cols, outputCol="features", handleInvalid="skip")
    classifier = RandomForestClassifier(featuresCol='features', labelCol='category_new')
    # classifier = MultilayerPerceptronClassifier(featuresCol='features', labelCol='category_new',layers=[len(list(no_pred.columns)),7,5,5])
    pipeline = Pipeline(stages=[to_categorical, proto_index, service_index, flag_index, assemble_features, classifier])
    model = pipeline.fit(training)
    predictions = model.transform(validation)
    prediction2=predictions.withColumn("mod_category_new",when(predictions['category_new']!=0.0,1.0).otherwise(predictions['category_new'])).withColumn("mod_prediction",when(predictions['prediction']!=0.0,1.0).otherwise(predictions['prediction']))
    pred_label=prediction2.select(prediction2['mod_prediction'],prediction2['mod_category_new']).orderBy('mod_prediction')
    metrics = MulticlassMetrics(pred_label.rdd.map(tuple))
    print('---------------')
    print("false positives : ",metrics.falsePositiveRate(1.0))
    print("precision : ",metrics.precision(1.0))
    print("recall", metrics.recall(1.0))
    print("accuracy : ",metrics.accuracy)
    print('---------------')


def train_and_save_model(training, path):
    """
    Uses the entire data you send as training data and saves the model in the given location
    :param training: Input data to train on (Note: 100% of the data will be used for training and saving the model)
    :param path: Path to store the model
    :return: Saves the model at the location
    """
    to_categorical = StringIndexer(inputCol="category", outputCol="category_new")
    proto_index = StringIndexer(inputCol="protocol_type", outputCol="protocol_new")
    service_index = StringIndexer(inputCol="service", outputCol="service_new")
    flag_index = StringIndexer(inputCol="flag", outputCol="flag_new")
    features_to_drop = ["category", "protocol_type", "service", "flag", "attack"]
    input_cols = list(set(training.columns).symmetric_difference(set(features_to_drop)))
    assemble_features = VectorAssembler(inputCols=input_cols, outputCol="features", handleInvalid="skip")
    classifier = RandomForestClassifier(featuresCol='features', labelCol='category_new')
    pipeline = Pipeline(stages=[to_categorical, proto_index, service_index, flag_index, assemble_features, classifier])
    model = pipeline.fit(training)
    model.write().overwrite().save(path)
