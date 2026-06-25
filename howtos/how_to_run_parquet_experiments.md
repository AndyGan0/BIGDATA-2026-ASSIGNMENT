#  <b> How to run experiments for parquet </b>

This file explains how to run the experiments for the parquet. 

First, the enviroment has to be set up based on the guides of the lab.
https://github.com/ikons/bigdata-dsml

After that, the script files have to be copied to wsl.

After using VPN to connect to the cluster, the following command is used to run the file which converts the LA crime dataset into parquet.

```bash
spark-submit <path to script Parquet_convert.py> \
  --output hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/<username>/parquet/LA_Crime_Data
```

The parquet file will be saved into the user folder.

After that, query 1 can be run using the parquet file with the following command:


```bash
spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  <path to script Parquet_query1.py> \
  --data-path-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/<username>/parquet/LA_Crime_Data \
  --output hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/<username>/Assignment_output/Parquet_query1
```

