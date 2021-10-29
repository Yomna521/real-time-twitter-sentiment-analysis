'''
#Stream Consumer
In this code, spark streaming service is intialized. tweets are then processed and results are saved in a pie chart for human-friendly reading of information.
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql.functions import udf, from_json, col
from pyspark.ml import PipelineModel
import re
from datetime import datetime
from pathlib import Path
import matplotlib.pyplot as plt

SRC_DIR = Path(__file__).resolve().parent
#The topic previously created in stream_producer.py
kafka_topic = 'Movie'

#start a spark session
spark = SparkSession \
    .builder \
    .appName("streamingService") \
    .getOrCreate()

#Build a schema for the incoming tweets
schema = StructType([StructField("time", StringType()), StructField("text", StringType())])

#Now we can build a dataframe using the schema 
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", 'Movie') \
    .option("startingOffsets", "latest") \
    .option("header", "true") \
    .load() \
    .selectExpr("CAST(timestamp AS TIMESTAMP) as timestamp", "CAST(value AS STRING) as text")

df = df.withColumn("value", from_json("text", schema)).select('timestamp', 'value.*')

# then change the timestamp format to string
date_process = udf(
    lambda x: datetime.strftime(
        datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'
        )
    )
df = df.withColumn("time", date_process(df.time))


#Pre-processing of incoming tweets
pre_process = udf(
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', \
        x.lower().strip()).split(), ArrayType(StringType())
    )
df = df.withColumn("words", pre_process(df.text)).dropna()

#Getting the trained model
model_path = str(SRC_DIR.joinpath('ml_model'))
ml_model = PipelineModel.load(model_path)

#getting predictions foir the sentiment in fetched tweets
prediction  = ml_model.transform(df)
prediction = prediction.select(prediction.words, prediction.time, prediction.timestamp, prediction.text, prediction.prediction)

#save dataframe in parquet
parquet_path = str(SRC_DIR.joinpath('parquet/events/_checkpoints/twitter_predictions'))
checkpoint = str(SRC_DIR.joinpath('parquet/events'))

query = prediction \
        .writeStream \
        .queryName("sentiment_prediction") \
        .outputMode("append") \
        .format("parquet")\
        .option("path", parquet_path)\
        .option("checkpointLocation", checkpoint)\
        .trigger(processingTime='60 seconds').start()

query.awaitTermination()




