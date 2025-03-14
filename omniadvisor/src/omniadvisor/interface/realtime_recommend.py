from omniadvisor.service.parameter_parser import SparkParameterParser
from omniadvisor.service.spark_executor import SparkExecutor
from django_server.app.models import Load
from omniadvisor.utils.logger import logger
from omniadvisor.utils.exception import ParameterException

from typing import Dict, Any

"""
典型的Spark提交命令如下：

"""

spark_sql_cmd = "spark-sql --master yarn --deploy-mode client \
    --name ${name}_advisor_${i} \
    --driver-cores 2 \
    --driver-memory 8G \
    --conf spark.sql.shuffle.partitions=3200 \
    --conf spark.sql.result.partitions=3200 \
    --conf spark.executor.cores=4 \
    --conf spark.executor.memory=28G \
    --conf spark.executor.memoryOverhead=2G \
    --conf spark.memory.storageFraction=0.5 \
    --conf spark.memory.fraction=0.6 \
    --num-executors 400 \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.shuffle.io.maxRetries=6 \
    --conf spark.shuffle.io.retryWait=60 \
    --conf spark.shuffle.useOldFetchProtocol=false \
    --conf spark.shuffle.io.serverThreads=32 \
    --conf spark.sql.hive.convertMetastoreParquet=true \
    --conf spark.shuffle.file.buffer=1024k \
    --conf spark.reducer.maxSizeInFlight=96M \
    --conf spark.reducer.maxReqsInFlight=10 \
    --conf spark.network.timeout=300s \
    --conf spark.rpc.askTimeout=300s \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.minPartitionNum=100 \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=64M \
    --conf spark.sql.adaptive.fetchShuffleBlocksInBatch=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=5 \
    --conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256mb \
    --conf spark.sql.inMemoryColumnarStorage.compressed=true \
    --conf spark.sql.sources.partitionColumnTypeInference.enabled=false \
    --conf spark.sql.storeAssignmentPolicy=LEGACY \
    --conf spark.sql.legacy.timeParserPolicy=LEGACY \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.speculation=true \
    --conf spark.speculation.multiplier=5 \
    --conf spark.speculation.quantile=0.99 \
    --conf spark.speculation.minTaskRuntime=3000ms \
    --conf spark.sql.autoBroadcastJoinThreshold=64mb \
    --conf spark.sql.adaptive.coalescePartitions.minPartitionSize=32mb \
    --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
    --conf spark.sql.parquet.writeLegacyFormat=true \
    --conf spark.driver.maxResultSize=40g \
    -f $file 2>&1"


def query_best_config(parsed_parameter: Dict[str, Any]) -> Load:
    # TODO 与Load类交互 根据用户传入的负载信息从数据库中查询相关配置
    pass
    is_optimized = True
    config_for_test = ""
    best_config = ""
    task_load = Load()  # TODO 数据库部分回传的三个配置用于构建Load，然后Load本身带有函数去把配置拼接成命令

    return is_optimized, config_for_test, best_config


def realtime_recommend(spark_sql_cmd):
    # 获取用户传入的Spark命令 并解析命令
    spark_parameter_parser = SparkParameterParser(spark_sql_cmd)
    parsed_parameter = spark_parameter_parser.process_parameter()  # TODO 需要关注parsed_parameter的格式

    # 根据解析的参数从数据库中查询最优配置
    is_optimized, config_for_test, best_config = query_best_config(parsed_parameter)

    # 实例化SparkExecutor
    spark_executor = SparkExecutor()

    # 执行判断逻辑
    is_sparktask_successed = True
    if is_optimized:
        if config_for_test:
            spark_executor.submit_spark_task(config_for_test)
        elif best_config:
            spark_executor.submit_spark_task(best_config)
    else:
        spark_executor.submit_spark_task(spark_sql_cmd)  # 执行租户默认配置


def main():
    realtime_recommend(spark_sql_cmd)


if __name__ == "__main__":
    main()