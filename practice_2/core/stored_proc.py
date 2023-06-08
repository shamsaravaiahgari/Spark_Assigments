from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()

# Creating a temporary view for the "Person.Contact" table
spark.sql("CREATE OR REPLACE TEMPORARY VIEW contact_view AS SELECT * FROM Person.Contact")

# Executing the stored procedure-like query without parameters
df_result = spark.sql("SELECT ContactID, FirstName, LastName FROM contact_view LIMIT 1")
df_result.show()

# Altering the query to include parameters
last_name = "Alberts"
df_result = spark.sql("SELECT ContactID, FirstName, LastName FROM contact_view WHERE LastName = '{}' LIMIT 1".format(last_name))
df_result.show()
