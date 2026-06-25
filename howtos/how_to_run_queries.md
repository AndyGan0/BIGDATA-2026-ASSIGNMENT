#  <b> How to run queries </b>

This file explains how to run the queries. The join experiments are run in the same way.

First, the enviroment has to be set up based on the guides of the lab.
https://github.com/ikons/bigdata-dsml

After that, the script files have to be copied to wsl.

After using VPN to connect to the cluster, the following command is used to run a script.

```bash
spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=8g \
  <path_to_script.py> \
  --output hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/<username>/Assignment_output/<script_name>
```

The file names describe the query as well as the version (DF, RDD or SQL).

The result will be saved into the user folder. Results vary depending on the query. Some queries might contain just the output and execution time while others might contain the physical plan chosen by the optimizer.

