from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
import csv

from time import perf_counter

# Keep the Python executable the same on the driver and on Spark workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sort in descending order the sections of day depending on the percentage of crime records in road 'STREET' using the RDD API.",
    )
    parser.add_argument("--data-path", default='hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/*csv', help="Base path that contains all the dataset.")
    parser.add_argument("--output",  required=True, help="Output path.")
    return parser.parse_args()



def compute_day_part(time):
    try:
        t = int(time)
    except(TypeError, ValueError):
        return 'LateNight'
    
    if 500 <= t and t <= 1159:
        return 'Morning'
    elif 1200 <=t and t <= 1659:
        return 'Afternoon'
    elif 1700 <= t and t <= 2059:
        return 'Night'
    else:
        return 'LateNight'




def main() -> None:
    args = parse_args()

    output_path = args.output
    

    builder = SparkSession.builder.appName("RDD query 1 execution")
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    
    start_time = perf_counter()

    la_crime__df =  sc.textFile(args.data_path)

    counts_df = (
        la_crime__df.map(lambda line: next(csv.reader([line])))        
        .filter(lambda row: row[15] == "STREET")
        .map(lambda row: ( compute_day_part(row[3]) , 1 ) )        
        .reduceByKey(lambda a, b: a + b)
        .cache()    #   Cache to avoid running twice in collect and write
    )

    total_crimes = counts_df.map(lambda x: x[1]).sum()
    
    percentage_items = (
        counts_df.map(lambda row: (row[0], 100 * row[1]/total_crimes))
        .sortBy(lambda item: item[1], ascending=False)
        .collect()
    )
    
    end_time = perf_counter()
    print(f"Execution Time: {end_time-start_time}")

    for day_part, percentage in percentage_items:
        print( f"{day_part}: {percentage:.2f}%" ) 

    if output_path:
        result_df = spark.createDataFrame(percentage_items, ['day_part', 'percentage'])
        result_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
