import argparse
import shlex
from typing import Dict
from omniadvisor.utils.logger import logger
from omniadvisor.utils.constant import OA_CONF
from omniadvisor.service.parameter_parser import ParserInterface


class SparkParameterParser(ParserInterface):
    """
    Spark 引擎的解析器
    :param submit_cmd: 提交的命令
    """

    def __init__(self, submit_cmd):
        super().__init__(submit_cmd)
        self._parser.add_argument("--master",
                                  help="spark://host:port, mesos://host:port, yarn, k8s://https://host:port, "
                                       "or local (Default: local[*]).")
        self._parser.add_argument("--deploy-mode",
                                  help="Whether to launch the driver program locally (client) or on one of the worker "
                                       "machines inside the cluster (cluster)(Default: client)")
        self._parser.add_argument('--class', help="Your application's main class(for Java / Scala apps")
        self._parser.add_argument('--name', help="A name of your application")
        self._parser.add_argument('--jar')
        self._parser.add_argument('--packages')
        self._parser.add_argument('--exclude-packages')
        self._parser.add_argument('--repositories')
        self._parser.add_argument('--py-files')
        self._parser.add_argument('--files')
        self._parser.add_argument('--archives')
        self._parser.add_argument('--conf', action='append', help="Arbitrary Spark configuration property")
        self._parser.add_argument('--driver-memory')
        self._parser.add_argument('--driver-java-options')
        self._parser.add_argument('--driver-library-path')
        self._parser.add_argument('--driver-class-path')
        self._parser.add_argument('--executor-memory')
        self._parser.add_argument('--proxy-user')
        self._parser.add_argument('--verbose')
        self._parser.add_argument('--version')
        self._parser.add_argument('--driver-cores')
        self._parser.add_argument('--supervise')
        self._parser.add_argument('--kill')
        self._parser.add_argument('--status')
        self._parser.add_argument('--total-executor-cores')
        self._parser.add_argument('--executor-cores')
        self._parser.add_argument('--num-executors')
        self._parser.add_argument('--principal')
        self._parser.add_argument('--keytab')
        self._parser.add_argument('--queue')
        self._parser.add_argument('-d', "--database", metavar="database")
        self._parser.add_argument('-e', '--execute', type=str, help='SQL statement to execute.')
        self._parser.add_argument('-f', '--file', type=str, help='File containing SQL script.')
        self._parser.add_argument('-i', help='Initialization SQL file')
        self._parser.add_argument('remainder', nargs=argparse.REMAINDER, help="Collect the rest of parameters")

    def process_parameter(self) -> Dict:
        """
        解析提交后的命令，得到解析的参数后，以字典的形式保存
        :return:dict
        返回一个包含所有提取出的基础参数的字典,每个键代表一个参数名,详见add_argument中的配置的参数。
        对应的值为该参数的具体设置或值。
        base_params = {"name":"", "conf_params":{}, "exec_attr":{}}
        """
        base_params = self._get_base_params()
        return base_params

    def _get_base_params(self):
        """
        使用argparse来解析命令行中的输入命令，并返回解析后的非空参数，以字典类型保存
        :return:
        """
        if not self._submit_cmd:
            raise ValueError("Submit command cannot be null")
        param_start_index = self._submit_cmd.strip().index(" ")
        base_params = {"name": "", "conf_params": {}, "exec_attr": {}}
        conf_params = {}
        exec_attr = {}
        args = shlex.split(self._submit_cmd[param_start_index:])
        params, unknown = self._parser.parse_known_args(args=args)
        options_dict = vars(params)
        for key, value in options_dict.items():
            # 过滤空值参数
            if value is None:
                continue
            # 提取执行相关参数保存至exec_attr
            if key not in OA_CONF.SPARK_NON_EXEC_ATTR_LIST:
                exec_attr[key] = value
            # 提取conf配置
            elif key == "conf":
                for confitem in value:
                    parts = confitem.split("=")
                    if len(parts) != 2:
                        raise ValueError(f"conf{parts}的格式不符合key=value的形式")
                    confkey, confvalue = parts
                    # 处理重复键问题，例如合并值为列表
                    if confkey in conf_params:
                        if isinstance(conf_params[confkey], list):
                            conf_params[confkey].append(confvalue)
                        else:
                            conf_params[confkey] = [conf_params[key], value]
                        logger.warn(f"{confkey}被重复配置,多次配置值如下{conf_params[confvalue]}")
                    else:
                        conf_params[confkey] = confvalue
            else:
                base_params[key] = value

            # exec_attr 和 conf_params合并进入base_params
            base_params["exec_attr"] = exec_attr
            base_params["conf_params"] = conf_params

        return base_params


if __name__ == "__main__":
    submit_cmd = "spark-sql --master yarn --deploy-mode client \
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
    -f $file"

    spark_parameter_parser = SparkParameterParser(submit_cmd)
    parsed_parameter = spark_parameter_parser.process_parameter()
    print("")