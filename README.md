# Spark_Assigments
spark assignmnents
# Spark_Assignments
contains all the spark assignmnets 
Spark-Assignments
Spark Assignment 1
Given two data files transaction.csv, user.csv

user file consists of the following fields:
user_Id
EmailId
NativeLanguage
Location
transaction file consists of the following fields:
Transaction_id
product_id
userId
price
product_description
userId in both the files denotes the same.

Tasks to be performed with above files are as follows:
1.Count of unique locations where each product is sold.
2.Find out products bought by each user.
3.Total spending done by each user on each product.
Code snippet consists of functions as below :
user_DataFrame : This function reads the user.csv file and data is been stored into a dataframe.
transaction_DataFrame : This function reds the transaction.csv file and data is been stored into another dataframe.
join_DataFrame : This function joins the two dataframes based on inner join condition with userid column.
unique_loc_count : This functions accepts the joined result of two dataframes as an argument and computes the count of unique locations where each product is sold.
user_prod : This function accepts the joined result of two dataframes as an argument and computes the products bought by each user. -tot_spend : This function accepst the joined result of two dataframes as an argument and computes the total amount of spending done by each user on each product.
core directory has two files

driver : We call a function from driver and the result expected from that function is seen in the driver
util : body of the fucntion is described in util Whenever we call a function from driver, the function is been executed in util and result is been displayed in driver.
resource directory has two files transaction.csv, user.csv in it.

test directory has the test case in it.

Spark Assignment 2
Each log line comprises of a standard part (up to .rb:) and an operation-specific part. The standard part fields are like so:

Logging level, one of DEBUG, INFO, WARN, ERROR (separated by ,)
A timestamp (separated by ,)
The downloader id, denoting the downloader instance (separated by --)
The retrieval stage, denoted by the Ruby class name, one of:

event_processing
ght_data_retrieval
api_client
retriever
ghtorrent
Tasks to be performed with above files are as follows:
1.Write a function to load it in an Data Frame.
2.How many lines does the Data Frame contain?
3.Count the number of WARNing messages
4.How many repositories where processed in total? Use the api_client lines only.
5.Which client did most HTTP requests?
6.Which client did most FAILED HTTP requests? Use group_by to provide an answer.
7.What is the most active hour of day?
8.What is the most active repository (hint: use messages from the ghtorrent.rb layer only)?
Code snippet consists of functions as below :
create_torrent_DataFrame() This function separates the fields(log_level,timeStamp,ghTorrent_details) in textFile with the given delimiter ","

ghtorrent_fields() This function accepts the dataFrame created by create_torent_DatFrame as a parameter. This function extracts the various fields as per the requirements

total_line() This function accepts the dataFrame created by ghtorrent_fields as a parameter. This function result will give the count of the total number of lines in that dataFrame which has the text file in it.

warn_count() This function accepts the dataFrame created by ghtorrent_fields as a parameter This function result will give the count of the total number of lines in textFile which has loglevel as "WARN"

api_client_repo() This function accepts the dataFrame created by ghtorrent_fields as a parameter This function result will give the count on how many repositories where processed under "api_client"

most_http() This function accepts the dataFrame created by ghtorrent_fields as a parameter
This function gives the client who gave most http request

failed_request() This function accepts the dataFrame created by ghtorrent_fields as a parameter
This function gives the client who had most failed http request

active_hour() This function accepts the dataFrame created by create_torrent_DataFrame as a parameter
This function gives the most active hour of the day

active_repo() This function accepts the dataFrame created by ghtorrent_fields as a parameter
This function gives the most active repository

core directory has two files

driver : We call a function from driver and the result expected from that function is seen in the driver
util : body of the fucntion is described in util Whenever we call a function from driver, the function is been executed in util and result is been displayed in driver.
resource directory has ghtorrent-logs text file in it.

test directory has the test case in it.
