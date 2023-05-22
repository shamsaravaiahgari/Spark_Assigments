from pyspark.sql import SparkSession

def read_csv(spark, file_path):
    # Read the CSV file into a DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def select_columns(df, columns):
    # Select specific columns from the DataFrame
    selected_df = df.select(columns)
    return selected_df

def filter_rows(df, condition):
    # Filter rows based on a condition
    filtered_df = df.filter(condition)
    return filtered_df

def add_column(df, column_name, column_expression):
    # Add a new column to the DataFrame
    new_df = df.withColumn(column_name, column_expression)
    return new_df

def aggregate(df, aggregations):
    # Perform aggregation operations on the DataFrame
    aggregated_df = df.agg(aggregations)
    return aggregated_df

def sort_dataframe(df, column):
    # Sort the DataFrame by a column
    sorted_df = df.orderBy(column)
    return sorted_df

def create_dataframe(spark, data, columns):
    # Create a DataFrame from data and columns
    other_df = spark.createDataFrame(data, columns)
    return other_df

def join_dataframes(df1, df2, join_column):
    # Join two DataFrames based on a common column
    joined_df = df1.join(df2, join_column)
    return joined_df

def get_distinct_rows(df):
    # Get unique rows from the DataFrame
    distinct_df = df.distinct()
    return distinct_df

def count_rows(df):
    # Count the number of rows in the DataFrame
    row_count = df.count()
    return row_count
