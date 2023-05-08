
#---------------X-----------------------------
from assignment_1.Core.Utils import *

# create a SparkSession
spark_session = create_spark_session()

# create transaction DataFrame
transaction_df = create_transaction_dataframe(spark_session)
print("Transaction Table")
transaction_df.show()

# create user DataFrame
user_df = create_user_dataframe(spark_session)
print("User Table")
user_df.show()

# join transaction and user DataFrames
joined_df = join_dataframes(user_df, transaction_df)
print("Tables after Joining")
joined_df.show()

# count of unique locations with product sold in that particular location
unique_loc_count_df = count_unique_locations(joined_df)
print("Count of Unique Locations with Product Sold in That Location")
unique_loc_count_df.show()

# products bought by each user
prod_bought_user_df = products_bought_by_user(joined_df)
print("Products Bought by Each User")
prod_bought_user_df.show()

# total amount of spending by each user on each product
exp_user_prod_df = total_spending_by_user_on_product(joined_df)
print("Total Spending Done by Each User on Each Product")
exp_user_prod_df.show()
