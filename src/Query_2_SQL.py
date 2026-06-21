from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
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
        description="For each year retrieve the 3 months with highest number of crimes using the SQL API.",
    )
    parser.add_argument("--data-path", default='hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data', help="Base path that contains all the dataset.")
    parser.add_argument("--output",  required=True, help="Output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    la_crime_data_2010_19_path = build_path(args.data_path, 'LA_Crime_Data_2010_2019.csv')
    la_crime_data_2020_25_path = build_path(args.data_path, 'LA_Crime_Data_2020_2025.csv')

    output_path = args.output
    

    builder = SparkSession.builder.appName("SQL query 2 execution")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    la_crime_df = (
        spark.read 
        .option("header", True) 
        .csv([la_crime_data_2010_19_path, la_crime_data_2020_25_path], 
             schema=LA_CRIME_data_SCHEMA)        
    ).createOrReplaceTempView("la_crime")


    spark.sql(
        """
        SELECT year, month, COUNT(*) AS crime_total
        FROM (
            SELECT year(to_timestamp(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS year,
                   month(to_timestamp(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS month
            FROM la_crime
        )
        GROUP BY year, month
        """
    ).createOrReplaceTempView("counts_by_month_year")    

    result = spark.sql(
        """
        SELECT year, month, crime_total, ranking
        FROM (
            SELECT year, month, crime_total, 
                    ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
            FROM counts_by_month_year
        )
        WHERE ranking <= 3
        ORDER BY year ASC, crime_total DESC
        """
    ).cache()

    start_time = perf_counter()
    collected_rows = result.collect()
    end_time = perf_counter()
    print(f"Execution Time: {end_time-start_time}")

    results = [(row.year, row.month, row.crime_total, row.ranking) for row in collected_rows]
    for (yr, mn, crime_total, ranking) in results:
        print( f"{yr}-{mn}: {crime_total} crimes - {ranking} ranking" ) 

    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
