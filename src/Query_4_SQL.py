from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from time import perf_counter

# Keep the Python executable the same on the driver and on Spark workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Use an explicit schema
LA_CRIME_DATA_SCHEMA = StructType(
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


LA_POLICE_STATION_SCHEMA = StructType(
    [
        StructField("X", StringType()),
        StructField("Y", StringType()),
        StructField("FID", StringType()),
        StructField("DIVISION", StringType()),     
        StructField("LOCATION", StringType()),
        StructField("PREC", StringType()),      
    ]
)



def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find for each crime the nearest station. For each station show the number of crimes and the average distance using the SQL API.",
    )
    parser.add_argument("--data-path", default='hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/', help="Base path that contains all the dataset.")
    parser.add_argument("--output",  required=True, help="Output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    la_crime_data_2010_19_path = build_path(args.data_path, 'LA_Crime_Data/LA_Crime_Data_2010_2019.csv')
    la_crime_data_2020_25_path = build_path(args.data_path, 'LA_Crime_Data/LA_Crime_Data_2020_2025.csv')
    la_police_stations_path = build_path(args.data_path, 'LA_Police_Stations.csv')

    output_path = args.output
    

    builder = SparkSession.builder.appName("SQL query 4 execution")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    la_crime_df = (
        spark.read 
        .option("header", True) 
        .csv([la_crime_data_2010_19_path, la_crime_data_2020_25_path], 
             schema=LA_CRIME_DATA_SCHEMA)        
    ).createOrReplaceTempView("la_crime")

    spark.sql(
        """
        SELECT DR_NO, CAST(LAT AS FLOAT) as LAT, cast(LON AS FLOAT) as LON
        FROM la_crime
        WHERE LAT != 0 AND LON != 0 AND LAT IS NOT NULL AND LON is not NULL
        """
    ).createOrReplaceTempView("la_crime")    


    la_Police_Stations_df = (
        spark.read 
        .option("header", True) 
        .csv(la_police_stations_path, schema=LA_POLICE_STATION_SCHEMA)        
    ).createOrReplaceTempView("la_Police_Stations")

    spark.sql(
        """
        SELECT DIVISION, CAST(X AS FLOAT) as X, cast(Y AS FLOAT) as Y
        FROM la_Police_Stations
        WHERE X != 0 AND Y != 0 AND X IS NOT NULL AND Y is not NULL
        """
    ).createOrReplaceTempView("la_Police_Stations") 


    

    spark.sql(
        """
        SELECT *
        FROM la_crime CROSS JOIN la_Police_Stations
        """
    ).createOrReplaceTempView("cross_joined_crimes_stations") 

    spark.sql(
        """
        SELECT
            DR_NO, LAT, LON, X, Y, DIVISION,
            2 * 6371 * ASIN(SQRT(
                POWER(SIN((RADIANS(Y) - RADIANS(LAT)) / 2), 2) +
                COS(RADIANS(LAT)) * COS(RADIANS(Y)) *
                POWER(SIN((RADIANS(X) - RADIANS(LON)) / 2), 2)
            )) AS distance
        FROM cross_joined_crimes_stations
        """
    ).createOrReplaceTempView("cross_joined_haversine_distance") 


    spark.sql(
        """
        SELECT DR_NO, distance, DIVISION
        FROM (
            SELECT DR_NO, distance, DIVISION, 
                    ROW_NUMBER() OVER (PARTITION BY DR_NO ORDER BY distance ASC) AS ranking
            FROM cross_joined_haversine_distance
        )
        WHERE ranking = 1
        """
    ).createOrReplaceTempView("closest_station_to_crime_df") 


    result = spark.sql(
        """
        SELECT DIVISION, AVG(distance) as average_distance, count(*) as num_crimes
        FROM closest_station_to_crime_df
        GROUP BY DIVISION
        ORDER BY num_crimes DESC
        """
    ).cache()




    start_time = perf_counter()
    collected_rows = result.collect()
    end_time = perf_counter()
    print(f"Execution Time: {end_time-start_time}")

    results = [(row.DIVISION, row.average_distance, row.num_crimes) for row in collected_rows]
    for DIVISION, average_distance, num_crimes in results:
        print( f"DIVISION:{DIVISION},     Average Distance:{average_distance},     {num_crimes}crimes " ) 

    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
