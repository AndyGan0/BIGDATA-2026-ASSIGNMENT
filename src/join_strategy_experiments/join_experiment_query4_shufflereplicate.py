from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from pyspark.sql.functions import broadcast

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
        description="Find for each crime the nearest station. For each station show the number of crimes and the average distance using the DataFrame API.",
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
    

    builder = SparkSession.builder.appName("DF query 4 execution")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    la_crime__df = (
        spark.read 
        .option("header", True) 
        .csv([la_crime_data_2010_19_path, la_crime_data_2020_25_path], 
             schema=LA_CRIME_DATA_SCHEMA)
        .select('DR_NO', 'LAT', 'LON')
        .withColumn('LAT', F.col('LAT').cast('double'))
        .withColumn('LON', F.col('LON').cast('double'))
        .filter(F.col("LAT").isNotNull() & F.col("LON").isNotNull()
                & (F.col("LAT")!=0) & (F.col("LON")!=0))
    )

    la_Police_Stations_df = (        
        spark.read 
        .option("header", True) 
        .csv(la_police_stations_path, schema=LA_POLICE_STATION_SCHEMA)
        .select('X', 'Y', 'DIVISION')
        .withColumn('X', F.col('X').cast('double'))
        .withColumn('Y', F.col('Y').cast('double'))
        .filter(F.col("X").isNotNull() & F.col("Y").isNotNull())
    )

    cross_joined_crimes_stations_df = la_crime__df.hint("shuffle_replicate_nl").crossJoin(la_Police_Stations_df)
    

    cross_joined_haversine_distance_df = (
        cross_joined_crimes_stations_df
        .withColumn('tempLon1', F.radians("LON"))
        .withColumn('tempLat1', F.radians("LAT"))
        .withColumn('tempLon2', F.radians("X"))
        .withColumn('tempLat2', F.radians("Y"))
        .withColumn(
            'distance',
            2 * 6371 * F.asin(
                F.sqrt(
                    F.sin( (F.col('tempLat2') - F.col('tempLat1')) / 2 ) ** 2 + \
                    F.cos('tempLat1') * F.cos('tempLat2') * \
                    F.sin( (F.col('tempLon2') - F.col('tempLon1')) / 2 ) ** 2
                )
            )
        )
        .select('DR_NO', 'LAT', 'LON', 'X', 'Y', 'DIVISION', 'distance' )
    )

    window = Window.partitionBy("DR_NO").orderBy(F.col("distance").asc())
    closest_station_to_crime_df = (
        cross_joined_haversine_distance_df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col('row_num') == 1)
        .select('DIVISION', 'DR_NO', 'distance')
    )

    result = (
        closest_station_to_crime_df
        .groupBy('DIVISION')
        .agg(
            F.avg(F.col('distance')).alias('average_distance'),
            F.count('*').alias('num_of_crimes'),
        )
        .orderBy(F.col('num_of_crimes').desc())
        .cache()
    )

    result.explain("formatted")


    start_time = perf_counter()
    collected_rows = result.collect()
    end_time = perf_counter()
    print(f"Execution Time: {end_time-start_time}")

    results = [(row.DIVISION, row.average_distance, row.num_of_crimes) for row in collected_rows]
    for DIVISION, average_distance, num_crimes in results:
        print( f"DIVISION:{DIVISION},     Average Distance:{average_distance},     {num_crimes}crimes " ) 

    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
