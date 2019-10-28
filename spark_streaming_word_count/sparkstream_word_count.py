import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

brokers, topic = sys.argv[1:]
sc = SparkContext("local[2]", "WordCount")
ssc = StreamingContext(sc, 1)
model = PipelineModel.load('model')

lines = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list" : brokers})

words = lines.flatMap(lambda line : line[1].split(" "))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y : x + y)

wordCounts.pprint()
ssc.start()
ssc.awaitTermination()
