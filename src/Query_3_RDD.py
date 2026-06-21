from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
import csv, json

from time import perf_counter

# Keep the Python executable the same on the driver and on Spark workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable



def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find Average per person income based on ZIP code using the RDD API.",
    )
    parser.add_argument("--data-path", default='hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/', help="Base path that contains all the dataset.")
    parser.add_argument("--output",  required=True, help="Output path.")
    return parser.parse_args()




def main() -> None:
    args = parse_args()
    la_census_block_path = build_path(args.data_path, 'LA_Census_Blocks_2020.geojson')
    la_income_path = build_path(args.data_path, 'LA_income_2021.csv')

    output_path = args.output
    

    builder = SparkSession.builder.appName("RDD query 3 execution")
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    
    start_time = perf_counter()

    la_census_block =  sc.wholeTextFiles(la_census_block_path)
    la_census_features = la_census_block.flatMap( lambda kv: json.loads(kv[1])['features'] )

    def extract(feature):
        p = feature['properties']
        return (p.get('ZCTA20'), (p.get('POP20') or 0, 
                                  p.get('HOUSING20') or 0))
    
    la_census_block = (
        la_census_features
        .map(extract)
        .filter(lambda row: row[0] is not None and row[1][0] > 0)
        .reduceByKey(lambda row_a, row_b: (row_a[0]+row_a[0] , row_a[1]+row_a[1]))
    )



    la_income =  (
        sc.textFile(la_income_path)
        .map(lambda line: line.split(";"))
        .filter(lambda row: row[0] != 'Zip Code')
        .map(lambda row: (row[0], row[1], row[2].replace('$','').replace(',','')))
    )


    joined_df = la_census_block.join(la_income)

    median_per_person_income = (
        joined_df
        .map(lambda row: (row[0], row[3]*row[1][1] / row[1][0] ))
        .collect()
    )

    
    end_time = perf_counter()
    print(f"Execution Time: {end_time-start_time}")

    for day_part, percentage in median_per_person_income:
        print( f"{day_part}: {percentage:.2f}%" ) 

    if output_path:
        result_df = spark.createDataFrame(median_per_person_income, ['day_part', 'percentage'])
        result_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
