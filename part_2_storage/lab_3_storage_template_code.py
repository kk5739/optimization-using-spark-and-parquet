#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Template script to connect to Active Spark Session
Usage:
    $ spark-submit --deploy-mode client lab_3_storage_template_code.py <any arguments you wish to add>
'''


# Import command line arguments and helper functions(if necessary)
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

csv_files = {
    "small": "hdfs:/user/pw44_nyu_edu/peopleSmall.csv",
    "medium": "hdfs:/user/pw44_nyu_edu/peopleModerate.csv",
    "large": "hdfs:/user/pw44_nyu_edu/peopleBig.csv"
}

def main(spark):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''
    #####--------------YOUR CODE STARTS HERE--------------#####

    #Use this template to as much as you want for your parquet saving and optimizations!

    # Convert each file to Parquet
    # for size, csv_path in csv_files.items():
    #     df = spark.read.csv(csv_path, header=True, 
    #                         schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    #     output_path = f"hdfs:/user/np3106_nyu_edu/people_{size}.parquet"
    #     df.write.mode("overwrite").parquet(output_path)
    #     print(f"✅ Converted and saved: {csv_path} → {output_path}")
    print("="*30)
    print("Optimization 1 (just order by zipcode)")
    print("="*30)
    for size, csv_path in csv_files.items():
        df = spark.read.csv(csv_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
        output_path = f"hdfs:/user/np3106_nyu_edu/people_{size}_sorted.parquet"
        df_sorted = df.orderBy("zipcode")
        df_sorted.write.mode("overwrite").parquet(output_path)
        print(f"✅ Converted and saved: {csv_path} → {output_path}")

    print("="*30)
    print("Optimization 2 (repartition zipcode)")
    print("="*30)
    
    for size, csv_path in csv_files.items():
        df = spark.read.csv(csv_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
        output_path = f"hdfs:/user/np3106_nyu_edu/people_{size}_repartitioned.parquet"
        df_repartitioned = df.repartition(16, "zipcode")  
        df_repartitioned.write.mode("overwrite").parquet(output_path)
        print(f"✅ Converted and saved: {csv_path} → {output_path}")
    
    print("="*30)
    print("Optimization 3 (partitionBy loyalty, rewards)")
    print("="*30)
    
    for size, csv_path in csv_files.items():
        df = spark.read.csv(csv_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
        output_path = f"hdfs:/user/np3106_nyu_edu/people_{size}_partitioned.parquet"
        df.write.mode("overwrite").partitionBy("rewards", "loyalty").parquet(output_path)
        print(f"✅ Converted and saved: {csv_path} → {output_path}")

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
