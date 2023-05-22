from pyspark.sql import SparkSession
from practice_1.core.utils import *

# Creating a SparkSession
spark = SparkSession.builder.appName("practice_1").getOrCreate()

# sample data i have created
data = [
    ("Alice", 25, 50000),
    ("Bob", 30, 60000),
    ("Charlie", 35, 70000),
    ("David", 40, 80000),
    ("Eve", 45, 90000)
]

# i am definng the schema for the DataFrame
schema = create_sample_schema()

# Create a DataFrame from the sample data and schema
df = create_dataframe(spark, data, schema)

# Print the DataFrame
df.show()

# Example usage of the functions
df_select(df)
df_filter(df)
df_groupby(df)
df_aggregate(df)
df_orderby(df)
df_join(df)
df_withcolumn(df)
df_distinct(df)
df_count(df)

# Stop the SparkSession
spark.stop()
