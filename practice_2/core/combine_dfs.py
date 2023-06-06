from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("CombineDataFramesExample").getOrCreate()

# Sample dataset with 50 records
data1 = [(1, "John", 25), (2, "Alice", 30), (3, "Bob", 35), (4, "Emma", 40)]
data2 = [(1, "Manager"), (2, "Engineer"), (3, "Analyst"), (5, "Developer")]

# Create DataFrames from the sample dataset
df1 = spark.createDataFrame(data1, ["id", "name", "age"])
df2 = spark.createDataFrame(data2, ["id", "designation"])

# Print the original DataFrames
print("DataFrame 1:")
df1.show()
print("DataFrame 2:")
df2.show()

# Combine DataFrames using different methods

# Method 1: Union
combined_df_union = df1.union(df2)
print("Combined DataFrame (Union):")
combined_df_union.show()

# Method 2: Inner Join
combined_df_inner = df1.join(df2, on="id", how="inner")
print("Combined DataFrame (Inner Join):")
combined_df_inner.show()

# Method 3: Left Join
combined_df_left = df1.join(df2, on="id", how="left")
print("Combined DataFrame (Left Join):")
combined_df_left.show()

# Method 4: Right Join
combined_df_right = df1.join(df2, on="id", how="right")
print("Combined DataFrame (Right Join):")
combined_df_right.show()

# Method 5: Cross Join 

combined_df_cross = df1.crossJoin(df2)
print("Combined DataFrame (Cross Join):")
combined_df_cross.show()

# Method 6: Concatenate columns
combined_df_concat = df1.select(col("id"), col("name"), col("age"), col("designation"))
print("Combined DataFrame (Concatenated columns):")
combined_df_concat.show()

# Stop the SparkSession
spark.stop()
