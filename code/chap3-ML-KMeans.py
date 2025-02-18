from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col, date_format
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator 




# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Basic") \
    .getOrCreate()

# set number of partition to 5 in local machines
spark.conf.set("spark.sql.shuffle.partitions", "5")

# to read multiple files send the directory address
staticDataFrame = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("../data/retail-data/by-day/*.csv")

# coalesce reduces the number of partitions
preppedDataFrame = staticDataFrame\
.na.fill(0)\
.withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
.coalesce(5)

# train-test split
trainDataFrame = preppedDataFrame\
.where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame\
.where("InvoiceDate >= '2011-07-01'")

print(trainDataFrame.count())
print(testDataFrame.count())

# turn the days of weeks into corresponding numerical values
indexer = StringIndexer()\
.setInputCol("day_of_week")\
.setOutputCol("day_of_week_index")

# implement OneHotEncoder
encoder = OneHotEncoder()\
.setInputCol("day_of_week_index")\
.setOutputCol("day_of_week_encoded")

# vector type
vectorAssembler = VectorAssembler()\
.setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
.setOutputCol("features")

# set up pipline
transformationPipeline = Pipeline()\
.setStages([indexer, encoder, vectorAssembler])

# fit the pipeline to figure out how many unique values the data have
fittedPipeline = transformationPipeline.fit(trainDataFrame)

# transform all data
transformedTraining = fittedPipeline.transform(trainDataFrame)
transformedTesting = fittedPipeline.transform(testDataFrame)

# This will put a copy of the intermediately transformed dataset into memory, 
# allowing us to repeatedly access it at much lower cost than running the entire pipeline again.
transformedTraining.cache()

# initialize an untrained model
kmeans = KMeans()\
.setK(20)\
.setSeed(1)

evaluator = ClusteringEvaluator()

# train the model
kmModel = kmeans.fit(transformedTraining)

# cost cumputation for train dataset
transformedTrain = kmModel.transform(transformedTraining)
trainCost = evaluator.evaluate(transformedTrain)

# test phase
transformedTest = kmModel.transform(transformedTesting)
testCost = evaluator.evaluate(transformedTest)

print(trainCost)
print(testCost)