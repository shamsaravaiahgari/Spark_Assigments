from pyspark import SparkContext
from pyspark.sql import SparkSession

# Create a SparkContext
sc = SparkContext("local", "PySparkExample")

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Sample dataset 1
data1 = [1, 2, 3, 4, 5]

# Sample dataset 2
data2 = [2, 4, 6, 8, 10]

# Create RDDs from the datasets
rdd1 = sc.parallelize(data1)
rdd2 = sc.parallelize(data2)

# Use map() to square each element of rdd1
squared_rdd = rdd1.map(lambda x: x ** 2)
print("Squared RDD:")
print(squared_rdd.collect())

# Use reduce() to sum the elements of squared_rdd
sum_result = squared_rdd.reduce(lambda x, y: x + y)
print("Sum of squared RDD:")
print(sum_result)

# Use join() to join the two RDDs
joined_rdd = rdd1.join(rdd2)
print("Joined RDD:")
print(joined_rdd.collect())

# Stop the SparkContext
sc.stop()
