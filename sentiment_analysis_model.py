
"""# Import Modules"""

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml.feature import StopWordsRemover, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pathlib import Path
import re

#create spark session
spark = SparkSession \
    .builder \
    .appName("sentimentAnalysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

#path to dataset
src_dir = Path(__file__).resolve().parent
path = "tweets.csv"

#read data into dataframe
df = spark.read.csv(path, inferSchema=True, header=False)


"""# Clean The Data
Keep the relavent columns only:
	c0: Sentiment
	c5: Tweet Text
"""

df= df.selectExpr("_c0 as sentiment", "_c5 as text")

# Preprocessing of data: 
pre_process = udf(
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', \
        x.lower().strip()).split(), ArrayType(StringType())
    )
df = df.withColumn("words", pre_process(df.text))

# Splitting DataFrame into Training and Testing DataFrames
dividedData = df.randomSplit([0.7, 0.3]) 
trainingData = dividedData[0] 
testingData = dividedData[1] 
print ("Training data:", trainingData.count(), "; Testing data:", testingData.count())

#ML pipeline steps:
#1. remove stop words
swr = StopWordsRemover(inputCol="words", outputCol="meaningful words")
#2. vectorize the meaningful words, TF: term frequency
vectorizer = CountVectorizer(inputCol="meaningful words", outputCol="tf")
#3. IDF: inverse document frequency 
idf = IDF(inputCol="tf", outputCol="features", minDocFreq=3)
#4. label the sentiments: 0 -> (4) positive, 1 -> (0) negative
indexer = StringIndexer(inputCol = "sentiment", outputCol = "label")
#5. logistic regression
lr= LogisticRegression(maxIter=100)
#create the pipeline
pipeline = Pipeline(stages=[swr, vectorizer, idf, indexer, lr])

#fit the training data into the pipeline
trained_model = pipeline.fit(trainingData)

#Prediction
prediction_df = trained_model.transform(testingData)

#Evaluate Accuracy
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
accuracy = evaluator.evaluate(prediction_df)
print(accuracy)


# Save the trained model
model_path= str(src_dir.joinpath('ml_model'))
trained_model.write().overwrite().save(model_path)
print('Done')
