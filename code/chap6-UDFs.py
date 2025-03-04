from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, DoubleType


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Intermediate") \
    .getOrCreate()


# create simple function
udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
    return double_value ** 3
print(power3(2.0))

# convert it to udf the function
power3udf = udf(power3)

udfExampleDF.select(power3udf(col("num"))).show()

# register the function
spark.udf.register("power3", power3)

# use the udf function in sql query
udfExampleDF.selectExpr("power3(num)").show()

# sepcifying return type
spark.udf.register("power3py", power3, DoubleType())

udfExampleDF.selectExpr("power3py(num)").show() # returns null, the range creates integers

