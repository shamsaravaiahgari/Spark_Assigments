from pyspark.sql.types import *
from pyspark.sql.functions import *

def create_sample_schema():
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("salary", IntegerType(), nullable=True)
    ])
    return schema

def create_dataframe(spark, data, schema):
    # Create a DataFrame from the sample data and schema
    df = spark.createDataFrame(data, schema)
    return df

def df_select(df):
    # Select specific columns
    df.select("name", "age").show()

def df_filter(df):
    # Filter rows based on a condition
    df.filter(df.age > 30).show()

def df_groupby(df):
    # Group by a column and calculate the sum of another column
    df.groupBy("name").agg({"salary": "sum"}).show()

def df_aggregate(df):
    # Perform aggregation operations
    df.agg({"age": "max", "salary": "avg"}).show()

def df_orderby(df):
    # Sort the DataFrame by a column
    df.orderBy("salary").show()

def df_join(df):
    # Join with another DataFrame
    other_data = [
        ("Alice", "Marketing"),
        ("Bob", "Sales"),
        ("Charlie", "Finance")
    ]

    other_schema = StructType([
        StructField("name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True)
    ])

    other_df = spark.createDataFrame(other_data, other_schema)

    joined_df = df.join(other_df, "name")
    joined_df.show()

def df_withcolumn(df):
    # Add a new column
    df.withColumn("age_plus_10", df.age + 10).show()

def df_distinct(df):
    # Use distinct to get unique rows
    df.distinct().show()

def df_count(df):
    # Count the number of rows
    print("Number of rows:", df.count())
