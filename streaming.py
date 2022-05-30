import pyspark
import time
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

kafka_topic_name = "test-topic"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Welcome to Data PreProcessing !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of icu.csv: ")
    df.printSchema()

    df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")
    df1_ = df.selectExpr("CAST(value AS STRING)", "timestamp")

    schema_string = "subject_id INT, seq INT, begintime STRING, endtime STRING, first_day_flg STRING, last_day_flg STRING, final_flag INT,los INT, first_careunit INT, last_careunit INT "

    df2 = df1\
        .select(from_csv(col("value"), schema_string)\
        .alias("stays"), "timestamp")

    df3 = df2.select("stays.*", "timestamp")
    df3.printSchema()
    final_df = df3.filter("seq is not NULL")
    preds = final_df.select("final_flag").withColumnRenamed("final_flag","predictions")
    final_df = final_df.drop('timestamp')

    spark = SparkSession.builder.appName("Anomaly Detection").getOrCreate()
    df=spark.read.csv('icu.csv', header=True, inferSchema=True)
    df=df.drop('subject_id')
    df=df.drop('begintime')
    df=df.drop('endtime')
    df=df.drop('los')
    df=df.drop("first_careunit")
    df=df.drop("last_careunit")
    idx = StringIndexer(inputCol="first_day_flg",outputCol="first_day")
    df=idx.fit(df).transform(df)
    idx = StringIndexer(inputCol="last_day_flg",outputCol="last_day")
    df=idx.fit(df).transform(df)
    df=df.drop('first_day_flg')
    df=df.drop('last_day_flg')
    assembler=VectorAssembler(inputCols=['seq','first_day','last_day'], outputCol='features')
    df=assembler.transform(df)
    df=df.drop('seq')
    df=df.drop('first_day')
    df=df.drop('last_day')

    evaluator = ClusteringEvaluator(predictionCol='prediction', \
                        metricName='silhouette', distanceMeasure='squaredEuclidean')

    model=BisectingKMeans()
    model_fit=model.fit(df)
    output=model_fit.transform(df)
    
    agg_write_stream_preprocess = final_df \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    predictions = preds \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    agg_write_stream_preprocess.awaitTermination()
    predictions.awaitTermination()

