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

# join on complex types
person.withColumnRenamed("id", "personId")\
.join(sparkStatus, expr("array_contains(spark_status, id)")).show()

# duplicate column names
gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
joinExpr = gradProgramDupe["graduate_program"] == person["graduate_program"]

person.join(gradProgramDupe, joinExpr).show()
# person.join(gradProgramDupe, joinExpr).select("graduate_program").show() # returns an error

# approach 1
person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()

#approach 2
person.join(gradProgramDupe, joinExpr).drop(person["graduate_program"])\
.select("graduate_program").show()

# approach 3
gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
joinExpr = person["graduate_program"] == gradProgram3["grad_id"]
person.join(gradProgram3, joinExpr).show()

# join type
joinExpr = person["graduate_program"] == graduateProgram["id"]
person.join(graduateProgram, joinExpr).explain()