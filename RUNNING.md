## Project Setup

Our project requires the setup of Spark, Postgres, and Kafka.

### Setting up Spark

From the Spark download page, get Spark version 3.1.2 (the version we'll be using on the cluster), “Pre-built for Hadoop
3.2 and later”, and click the “download Spark” link. Unpack that somewhere you like. Set an environment variable so you
can find it easily later:

<pre><code>export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/home/you/spark-3.1.2-bin-hadoop3.2/
export PYSPARK_PYTHON=python3
</code></pre>


### Setting up postgres

For Mac:
<pre><code>brew install postgresql</code></pre>

For Ubuntu:
<pre><code>sudo apt install postgresql</code></pre>

We have broadly used 3 tables, and so it will be best if you create those tables locally.

The create scripts with schemas are in file "postgresql_query.txt"

The idea is that the raw data will be stored in "raw_data". This is of size 5m (aproxx)
The training ready data will be stored in "processed_data"
The rate of streams incoming from Kafka will be stored in "prediction_rate"

### Setting up kafka and producing streaming data
1. You can either install Kafka in its base by downloading the binaries from [kafka](https://www.apache.org/dyn/closer.cgi?path=/kafka/3.0.0/kafka_2.13-3.0.0.tgz) or go with confluent which is a fullfledged platform built around kafka and makes setting up and managing kafka much easier [Confluent](https://packages.confluent.io/archive/7.0/).
2. Once you downloaded and extrancted the file, export the following 1. CONFLUENT_HOME=confluent-7.0.0 2. PATH=$PATH:$CONFLUENT_HOME/bin
3. If you want to experiment with the kafka s3 plugin you can install it with the following command- confluent-hub install confluentinc/kafka-connect-s3:latest
4. Run confluent with the command- confluent local services start
5. run the kafka producer script with the folder which contains data to be streamed as the argument
6. you can test if the producer is working by reading the topic from the command line with the following command- kafka-console-consumer --topic cyberproduce --from-beginning --bootstrap-server localhost:9092

However, to install Kafka locally: 

1. Download the latest Kafka and extract it.
<pre><code>tar -xzf kafka_2.13-3.0.0.tgz
cd kafka_2.13-3.0.0</code></pre>

Make sure you have both config/zookeeper.properties and config/server.properties

### Once the entire setup is done, lets start training

The flow is in the manner (the steps separated by "and" are happening parallely):
1. Load data
2. Save "and" process data
3. Save the processed data "and" the model

Once the model is saved, we can use this to predict some test data dynamically.

Let's start with training.

After taking the clone, go inside the project and setup all environment variables.

Make sure to install the requirements using:
<pre><code>pip install -r requirements.txt</code></pre>

and uncompress the data "kddcup.data.corrected.zip"

Once done, try running:
<pre><code>spark-submit --driver-class-path tableau_driver/postgresql-42.3.1.jar run_training.py</code></pre>

This should run, load the data in the tables and save the model by the name of "final_model" in the current directory.
(For the ease of configurations, we have hardcoded the driver and other things, which could be moved to a CONFIG file of some sort later)

The idea of the current code is to also analyse the time taken for each step so that the same can be shown as analysis. However, if you are getting memory errors, please run:

<pre><code>spark-submit --driver-class-path tableau_driver/postgresql-42.3.1.jar alternate_run_training.py</code></pre>

For specific mac m1 related errors, you could try exporting this:
<pre><code>export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES</code></pre>

Once the model is sucessfully saved, you will be able to move to next step.

Now in the terminal keep the following tabs opened:

1. Postgresql db to check the data is getting updated since you won't be able to recreate the tableau dashboard and our free trail would be over by the time you will check.
2. Run the Kafka Zookeeper
3. Run the Kafka Server
4. Run the dynamic_predictions.py
5. Run the rate_calculation.py
6. Run the cyberproducer.py

Ok so here we have 2 listeners the "dynamic_predictions" and "rate_calculation"
and one producer which simulates stream.

We have added sample test_cases folder in a zip format which contains some files with the data to test upon.

Unzip the folder and run in following order:

From kafka installed folder:
<pre><code>bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
</code></pre>

From project folder:
<pre><code>spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --driver-class-path tableau_driver/postgresql-42.3.1.jar rate_calculation.py cyberproduce 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --driver-class-path tableau_driver/postgresql-42.3.1.jar dynamic_predictions.py cyberproduce
</code></pre>

3. Once you are sure they are listening:

<pre><code>python cyberproducer.py test_cases</code></pre>

If everything worked fine, you will be able to see the database get updated dynamically.

You will basically be able to see requests being classified as normal or attacked.

Try to run the following query:
<pre><code>select category,count(category) as counts from processed_data group by category;</code></pre>

Try this multiple times while the kafka stream is coming and you should be able to see the values changing.

All this gets dynamically updated in tableau dashboards too and you will be able to see them in the video:

(ADD VIDEO)


