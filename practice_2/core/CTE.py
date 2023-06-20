from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("CTE Example") \
    .getOrCreate()

# Create a DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Register the DataFrame as a temporary table
df.createOrReplaceTempView("my_table")

# Define the CTE query
cte_query = """
    WITH cte AS (
        SELECT name, age
        FROM my_table
        WHERE age > 30
    )
    SELECT *
    FROM cte
"""

# Execute the CTE query
cte_df = spark.sql(cte_query)

# Show the result
cte_df.show()
