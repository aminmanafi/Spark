from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import col, expr, to_date, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, max, dense_rank, rank
from pyspark.sql.functions import sum, grouping_id




# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Intermediate") \
    .getOrCreate()



person = spark.createDataFrame([
(0, "Bill Chambers", 0, [100]),
(1, "Matei Zaharia", 1, [500, 250, 100]),
(2, "Michael Armbrust", 1, [250, 100])])\
.toDF("id", "name", "graduate_program", "spark_status")

graduateProgram = spark.createDataFrame([
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley")])\
.toDF("id", "degree", "department", "school")

sparkStatus = spark.createDataFrame([
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor")])\
.toDF("id", "status")



person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

# define join condition
joinExpression = person["graduate_program"] == graduateProgram['id']

# join
person.join(graduateProgram, joinExpression).show()

# define join type
# inner join
joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show()

# outer join
joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()

# left-outer join
joinType = "left_outer"
graduateProgram.join(person, joinExpression, joinType).show()

# right-outer join
joinType = "right_outer"
person.join(graduateProgram, joinExpression, joinType).show()

# left-semi join
joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show()


gradProgram2 = graduateProgram.union(spark.createDataFrame([
(0, "Masters", "Duplicated Row", "Duplicated School")]))

gradProgram2.createOrReplaceTempView("gradProgram2")

gradProgram2.join(person, joinExpression, joinType).show()

# left-anti join
joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show()

# cross join
joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

# main cross join
person.crossJoin(graduateProgram).show()