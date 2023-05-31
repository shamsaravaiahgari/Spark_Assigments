from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("FunctionsExample").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("Life_Expectancy_Data.csv", header=True, inferSchema=True)

# Apply different functions on the DataFrame

# Map function - transform the "Country" column to uppercase
mapped_df = df.rdd.map(lambda row: (row[0].upper(), row[1], row[2], row[3])).toDF(["Country", "Year", "Status", "Life_Expectancy"])
print("Map Function:")
mapped_df.show()

# Filter function - filter rows with life expectancy greater than 60
filtered_df = df.filter(col("Life_Expectancy") > 60)
print("Filter Function:")
filtered_df.show()

# Reduce function - calculate the average life expectancy
reduce_result = df.rdd.map(lambda row: row[3]).reduce(lambda acc, val: acc + val) / df.count()
print("Reduce Function - Average Life Expectancy:")
print(reduce_result)

# Stop the SparkSession
spark.stop()
