# coding:utf8

import os

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = '/export/server/jdk1.8.0_202'
os.environ["PYSPARK_PYTHON"] = "/export/server/miniconda3/envs/pyspark/bin/python3.8"
os.environ['YARN_CONF_DIR'] = '/export/server/hadoop-3.3.0/etc/hadoop'

if __name__ == '__main__':
    # 构建SparkSession
    spark = SparkSession.builder. \
        appName("main"). \
        config('hive.metastore.uris', 'thrift://node1:9083'). \
        config("hive.metastore.warehouse.dir", '/user/hive/warehouse'). \
        enableHiveSupport(). \
        master("yarn"). \
        getOrCreate()

    sc = spark.sparkContext


    file_rdd = sc.textFile("hdfs://node1:8020/monero.log")

    # QUESTION:如果数据集很大，要预先抽样预测正确性，如何编写代码？

    split_rdd = file_rdd.map(lambda x: x.split('\t'))

    print(split_rdd.takeSample(True, 10))

    # TODO:解析两类数据
    # '[120.235.132.221:44395 INC] calling /get_transaction_pool_hashes.bin'
    # 'HTTP [83.137.158.6] POST /get_transaction_pool_hashes.bin'
