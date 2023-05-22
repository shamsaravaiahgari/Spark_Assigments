from pyspark.sql import SparkSession
from practice_2.core.utils import *


# Creating a SparkSession
spark = SparkSession.builder.appName("Spark_Assignment_2").getOrCreate()

# Read the CSV file into a DataFrame
df = read_csv(spark, "sample_data.csv")

# Display the DataFrame
df.show()

# Select specific columns
selected_df = select_columns(df, ["Sales", "Price", "ShelveLoc"])
selected_df.show()

# Filter rows based on a condition
filtered_df = filter_rows(df, df.Age > 50)
filtered_df.show()

# Add a new column
new_df = add_column(df, "AgePlus10", df.Age + 10)
new_df.show()

# Perform aggregation operations
aggregated_df = aggregate(df, {"Sales": "sum", "Price": "avg"})
aggregated_df.show()

# Sort the DataFrame by a column
sorted_df = sort_dataframe(df, "Price")
sorted_df.show()

# Join with another DataFrame
other_data = [
    (42, "Low"),
    (65, "High"),
    (59, "Medium")
]

other_df = create_dataframe(spark, other_data, ["Age", "Category"])

joined_df = join_dataframes(df, other_df, "Age")
joined_df.show()

# Get unique rows
distinct_df = get_distinct_rows(df)
distinct_df.show()

# Count the number of rows
row_count = count_rows(df)
print("Number of rows:", row_count)

# Stop the SparkSession
spark.stop()
