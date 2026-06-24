from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as pyspark_sum
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
    parser.add_argument("--data-path-parquet", required=True, help="Base path that contains all the dataset.")
    parser.add_argument("--output",  required=True, help="Output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    la_crime_data_2010_19_path = args.data_path_parquet

    output_path = args.output
    

    builder = SparkSession.builder.appName("DF query 1 execution")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    la_crime__df = spark.read.parquet(la_crime_data_2010_19_path)
    

    filtered_street_crimes__df = la_crime__df.filter(col("Premis Desc") == "STREET")

    time = col("TIME OCC").cast("int")
    day_part = (
        when(time.between(500, 1159), 'Morning')
        .when(time.between(1200, 1659), 'Afternoon')
        .when(time.between(1700, 2059), 'Night')
        .otherwise('LateNight')
    )
    crimes_with_daypart_df = filtered_street_crimes__df.withColumn('day_part', day_part)

    counts_df = crimes_with_daypart_df.groupBy("day_part").agg(count("*").alias("crime_count"))

    percentage_df = (
        counts_df
        .withColumn(
            'percentage',
            col('crime_count') / pyspark_sum('crime_count').over(Window.partitionBy()) * 100
        )
        .orderBy('percentage', ascending=False)
    )

    percentage_df = percentage_df.cache()   #   Cache to avoid running twice in collect and write

    start_time = perf_counter()
    collected_rows = percentage_df.collect()
    end_time = perf_counter()
    print(f"Execution Time: {end_time-start_time}")

    results = [(row.day_part, row.percentage) for row in collected_rows]
    for day_part, percentage in results:
        print( f"{day_part}: {percentage:.2f}%" ) 

    if output_path:
        percentage_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
