
#-----------X------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#creating a SparkSession
def create_spark_session():
    spark = SparkSession.builder \
            .appName("Spark Assignment 1") \
            .getOrCreate()
    return spark

#creating a DataFrame with contents of user.csv file
def create_user_dataframe(spark_session):
    user_df = spark_session.read \
              .csv("../resource/user.csv", header="true", inferSchema="true")
    return user_df

#creating a DataFrame with contents of transaction.csv file
def create_transaction_dataframe(spark_session):
    transaction_df = spark_session.read \
                     .csv("../resource/transaction.csv", header="true", inferSchema="true")
    return transaction_df

# joining the transaction and user data frames using inner join
def join_dataframes(user_df, transaction_df):
    joined_df = transaction_df.join(user_df, user_df.user_id == transaction_df.userid, "inner")
    return joined_df

# count of unique locations with product sold in that location
def count_unique_locations(joined_df):
    loc_count = joined_df.groupby("location", "product_description").agg(count("location").alias("count_unique_loc"))
    return loc_count

# products bought by each user
def products_bought_by_user(joined_df):
    user_bought = joined_df.groupby("userid").agg(count("product_description").alias("product_count"))
    return user_bought

# total spending done by each user on each product.
def total_spending_by_user_on_product(joined_df):
    total_spend = joined_df.groupby("userid", "product_id").agg(sum("price").alias("total_spend"))
    return total_spend
