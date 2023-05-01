#Grace Meredith
#CS433 HW2 Q2
#1 May 2023

#Find Top ten tweeted users from 9/16/2009-9/20/2009

#Library to find running spark session from any working directory
import findspark
findspark.init("/opt/spark")

import pyspark
import pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, explode, split, desc

sc =SparkContext()
spark = SparkSession.builder.appName("DataFrame").getOrCreate()

#Read training set and split into columns
df = spark.read.text("training_set_tweets.txt")
split_cols = pyspark.sql.functions.split(df['value'], "\t")
df1 = df.withColumn('userID', split_cols.getItem(0)) \
    .withColumn('tweetID', split_cols.getItem(1)) \
    .withColumn('tweet', split_cols.getItem(2)) \
    .withColumn('when', split_cols.getItem(3))

#split date & time into separate columns
split_cols3 = pyspark.sql.functions.split(df1['when'], " ")
df4 = df1.withColumn('date', split_cols3.getItem(0)) \
    .withColumn('time', split_cols3.getItem(1))

#filter dataframe by dates btw 9/16/09 & 9/20/09
filtered_dates = df4.filter((col("date") >= "2009-09-16") & (col("date") <= "2009-09-20"))

#find userIDs in btw those dates and count them, sort from max-min and save top ten
numbers_converted = filtered_dates.select(explode(split(col("userID"), " ")).alias("userID"))
user_ids = numbers_converted.filter(col("userID").rlike("^[0-9]+$")).groupBy("userID")
top_ten = user_ids.count().sort(desc("count")).limit(10)
top_ten.show()