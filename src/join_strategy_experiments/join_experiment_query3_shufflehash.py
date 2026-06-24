from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pyspark.sql.functions import broadcast

from time import perf_counter

# Keep the Python executable the same on the driver and on Spark workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# Use an explicit schema
LA_INCOME_SCHEMA = StructType(
    [
        StructField("Zip_Code", StringType()),
        StructField("Community", StringType()),
        StructField("Estimated_Median_Income", StringType())
    ]
)



def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find Average per person income based on ZIP code using the DataFrame API.",
    )
    parser.add_argument("--data-path", default='hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/', help="Base path that contains all the dataset.")
    parser.add_argument("--output",  required=True, help="Output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    la_census_block_path = build_path(args.data_path, 'LA_Census_Blocks_2020.geojson')
    la_income_path = build_path(args.data_path, 'LA_income_2021.csv')

    output_path = args.output
    

    builder = SparkSession.builder.appName("DF query 3 execution")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")



    la_census_block = (
        spark.read 
        .option("multiLine", "true")
        .json(la_census_block_path)
    ).selectExpr("explode(features) as features") \
    .select("features.*")

    la_census_block = la_census_block.select(
        [
            F.col(f"properties.{col_name}").alias(col_name)
            for col_name in la_census_block.schema["properties"].dataType.fieldNames()
        ]
        + ["geometry"]
    ).drop("properties").drop("type")

    la_census_block = (
        la_census_block
        .groupBy('ZCTA20')
        .agg(
            F.sum("HOUSING20").alias("HOUSING20"),
            F.sum("POP20").alias("POP20")
        )
        .filter(F.col('POP20') > 0)
    )



    la_income = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .csv(la_income_path, schema=LA_INCOME_SCHEMA)
        .withColumn(
            'Estimated_Median_Income',
            F.regexp_replace(F.col('Estimated_Median_Income'), '[$,]', '').cast('double')
        )
        .filter(F.col('Estimated_Median_Income').isNotNull())
    )


    joined_df = la_census_block.hint("shuffle_hash").join(la_income, la_census_block.ZCTA20 == la_income.Zip_Code)


    median_per_person_income = (
        joined_df
        .withColumn(
            'total_income',
            F.col('Estimated_Median_Income') * F.col('HOUSING20')
        )
        .withColumn(
            'per_person_income',
            F.col('total_income') / F.col('POP20')
        )
        .select('Zip_Code', 'per_person_income')
        .cache()
    )
    

    median_per_person_income.explain("formatted")

    start_time = perf_counter()
    collected_rows = median_per_person_income.collect()
    end_time = perf_counter()
    print(f"Execution Time: {end_time-start_time}")

    results = [(row.Zip_Code, row.per_person_income) for row in collected_rows]
    for Zip_Code, per_person_income in results:
        print( f"Zip Code {Zip_Code}: {per_person_income}$ per person Income" ) 

    if output_path:
        median_per_person_income.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
