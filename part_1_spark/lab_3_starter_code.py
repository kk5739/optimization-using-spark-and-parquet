#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py
'''
import os

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main(spark, userID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/{userID}/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/{userID}/sailors.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    # Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    # Construct a query
    print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    ## PART 1

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!

    # Loading without schemas
    reserves = spark.read.json(f'hdfs:/user/{userID}/reserves.json')
    

    print('Printing reserves inferred schema')
    reserves.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    reserves.createOrReplaceTempView('reserves')

    # Construct a query and show results
    print('Question 1')
    question_1_query = spark.sql('SELECT sid, sname, age FROM sailors WHERE age > 40')
    question_1_query.show()

    print('Question 2')
    question_2_query = reserves.filter(reserves.bid != 101).groupBy("sid").agg(F.count("bid").alias("count(bid)"))
    question_2_query.show()

    print('Question 3')
    question_3_query = spark.sql('''SELECT
                                        r.sid,
                                        FIRST(s.sname) AS sname,
                                        COUNT(DISTINCT r.bid) AS num_distinct_boats
                                    FROM
                                        reserves r
                                    JOIN
                                        sailors s
                                    ON
                                        r.sid = s.sid
                                    GROUP BY
                                        r.sid''')
    question_3_query.show()

    ## PART 2

    # Loading csvs with specified schemas
    print('Reading artist_term csv and specifying schema')
    artist_term = spark.read.csv('artist_term.csv', schema='artistID STRING, term STRING')
    print('Printing artist_term with specified schema')
    artist_term.printSchema()
    artist_term.createOrReplaceTempView('artist_term')

    print('Reading tracks csv and specifying schema')
    tracks = spark.read.csv('tracks.csv', schema='trackID STRING, title STRING, release STRING, year INT, duration DOUBLE, artistID STRING')
    print('Printing tracks with specified schema')
    tracks.printSchema()
    tracks.createOrReplaceTempView('tracks')

    # New queries
    print('Question 4')
    # Join the two DataFrames on artistID
    joined_4 = tracks.join(artist_term, on="artistID", how="inner")
    joined_4 = joined_4.filter((joined_4["year"] > 0) & (joined_4["duration"] > 0))

    # Group and aggregate
    result_4 = joined_4.groupBy("term").agg(
        F.expr("percentile_approx(year, 0.5)").alias("median_year"),
        F.max("duration").alias("max_duration"),
        F.countDistinct("artistID").alias("num_artists"),
        F.avg("duration").alias("avg_duration")
    )

    final_result_4 = result_4.orderBy("avg_duration").select(
        "term", "median_year", "max_duration", "num_artists", "avg_duration"
    ).limit(10)

    final_result_4.show(truncate=False)
    

    # New queries
    print('Question 5')
    # Join the two DataFrames on artistID
    joined_5 = tracks.join(artist_term, on="artistID", how="inner")
    joined_5 = joined_5.filter((joined_4["year"] > 0) & (joined_4["duration"] > 0))

    
    distinct_tracks = joined_5.groupBy("term").agg(F.countDistinct("trackID").alias("distinct_tracks"))

    # Top 10
    top_10 = distinct_tracks.orderBy(F.desc("distinct_tracks")).limit(10)
    top_10.show(truncate=False)

    # Bot 10
    bot_10 = distinct_tracks.orderBy("distinct_tracks").limit(10)
    bot_10.show(truncate=False)

    

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user userID from the command line
    # We need this to access the user's folder in HDFS
    userID = os.environ['USER']

    # Call our main routine
    main(spark, userID)
