from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, to_timestamp, year, month, row_number
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from time import perf_counter

# Keep the Python executable the same on the driver and on Spark workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Use an explicit schema
LA_CRIME_data_SCHEMA = StructType(
    [
        StructField("DR_NO", StringType()),
        StructField("Date Rptd", StringType()),
        StructField("DATE OCC", StringType()),
        StructField("TIME OCC", StringType()),        
        StructField("AREA", StringType()),
        StructField("AREA NAME", StringType()),
        StructField("Rpt Dist No", StringType()),
        StructField("Part 1-2", StringType()),        
        StructField("Crm Cd", StringType()),
        StructField("Crm Cd Desc", StringType()),
        StructField("Mocodes", StringType()),
        StructField("Vict Age", StringType()),        
        StructField("Vict Sex", StringType()),
        StructField("Vict Descent", StringType()),
        StructField("Premis Cd", StringType()),
        StructField("Premis Desc", StringType()),        
        StructField("Weapon Used Cd", StringType()),
        StructField("Weapon Desc", StringType()),
        StructField("Status", StringType()),
        StructField("Status Desc", StringType()),        
        StructField("Crm Cd 1", StringType()),
        StructField("Crm Cd 2", StringType()),
        StructField("Crm Cd 3", StringType()),
        StructField("Crm Cd 4", StringType()),        
        StructField("LOCATION", StringType()),
        StructField("Cross Street", StringType()),
        StructField("LAT", StringType()),
        StructField("LON", StringType()),
    ]
)



def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sort in descending order the sections of day depending on the percentage of crime records in road 'STREET' using the DataFrame API.",
    )
    parser.add_argument("--data-path", default='hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data', help="Base path that contains all the dataset.")
    parser.add_argument("--output",  required=True, help="Output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    la_crime_data_2010_19_path = build_path(args.data_path, 'LA_Crime_Data_2010_2019.csv')
    la_crime_data_2020_25_path = build_path(args.data_path, 'LA_Crime_Data_2020_2025.csv')

    output_path = args.output
    

    builder = SparkSession.builder.appName("DF query 1 execution")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    la_crime__df = (
        spark.read 
        .option("header", True) 
        .csv([la_crime_data_2010_19_path, la_crime_data_2020_25_path], 
             schema=LA_CRIME_data_SCHEMA)
    )

    crimes_with_year_month = ( 
        la_crime__df
        .withColumn('timestamp', to_timestamp(col("DATE OCC"), 'yyyy MMM dd hh:mm:ss a'))
        .withColumn('year', year(col('timestamp')))
        .withColumn('month', month(col('timestamp')))
    )
    
    counts_by_year_month_df = (
        crimes_with_year_month
        .groupBy('year', 'month')
        .agg(count("*").alias("crime_total"))
    )

    year_window = Window.partitionBy('year').orderBy(col('crime_total').desc())

    top_3_per_year = (
        counts_by_year_month_df
        .withColumn('ranking', row_number.over(year_window))
        .filter(col('ranking') <= 3)
        .orderBy( col('year').asc(), col('crime_total').desc() )
        .cache()
    )

    start_time = perf_counter()
    collected_rows = top_3_per_year.collect()
    end_time = perf_counter()
    print(f"Execution Time: {end_time-start_time}")

    results = [(row.year, row.month, row.crime_total, row.ranking) for row in collected_rows]
    for (year, month, crime_total, ranking) in results:
        print( f"{year}-{month}: {crime_total} crimes - {ranking} ranking" ) 

    if output_path:
        top_3_per_year.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
