# BoostKit Spark Native Sql Engine Extension
A Native SQL Engine Extension for Spark SQL with vectorized SIMD optimizations

## Introduction
Spark SQL works very well with structured row-based data. It used WholeStageCodeGen to improve the performance by Java JIT code. However Java JIT is usually not working very well on utilizing latest SIMD instructions, especially under  complicated queries.omni-vec provided CPU-cache friendly columnar in-memory layout, its SIMD-optimized kernels and LLVM-based SQL engine omni-jit are also very efficient.

BoostKit Spark Native Sql Engine Extension reimplements Spark SQL execution layer with SIMD-friendly columnar data processing based on omni-vec, and leverages omni-vec's CPU-cache friendly columnar in-memory layout, SIMD-optimized kernels and LLVM-based codegen engine to bring better performance to Spark SQL.

## 编译环境
1.如果在公司内部，请配置绿区/黄区代理：
```
绿区代理：
export https_proxy="http://ptaishanpublic2:Huawei123@90.90.64.10:8080"
export http_proxy="http://ptaishanpublic2:Huawei123@90.90.64.10:8080"
```

2.挂载本地镜像，安装相关依赖（以OpenEuler 20.3 LTS操作系统为例）
```
yum install lz4-devel.aarch64 -y
yum install zstd-devel.aarch64 -y
yum install snappy-devel.aarch64 -y
yum install protobuf-c-devel.aarch64 protobuf-lite-devel.aarch64 -y
yum install boost-devel.aarch64 -y
yum install cyrus-sasl-devel.aarch64 -y
yum install jsoncpp-devel.aarch64 -y
yum install openssl-devel.aarch64 -y
yum install libatomic.aarch64 -y
```
3.安装编译CMake
```
wget https://github.com/Kitware/CMake/archive/refs/tags/v3.13.4.tar.gz --no-check-certificate
tar -zxvf v3.13.4.tar.gz
cd CMake-3.13.4
./bootstrap
gmake
gmake install
```

4.安装编译LLVM
```
wget https://github.com/llvm/llvm-project/archive/refs/tags/llvmorg-12.0.0.tar.gz --no-check-certificate
tar -zxvf llvmorg-12.0.0.tar.gz
cd llvm-project-llvmorg-12.0.0
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DLLVM_BUILD_LLVM_DYLIB=true -DLLVM_ENABLE_PROJECTS="clang;libcxx;libcxxabi" -G "Unix Makefiles" ../llvm
make -j64
make install
ln -s /usr/local/bin/clang-12 /usr/local/bin/clang++-12
```

5.安装编译GoogleTest
```
wget  https://github.com/google/googletest/archive/refs/tags/release-1.11.0.tar.gz --no-check-certificate
tar -zxvf release-1.11.0.tar.gz
cd googletest-release-1.11.0
cmake CMakeLists.txt
make
cp ./googletest-release-1.11.0/lib/libgtest*.a /usr/lib
cp -r ./googletest-release-1.11.0/googletest/include/gtest /usr/include/
```

6.安装编译Json
```
wget https://github.com/nlohmann/json/archive/refs/tags/v3.9.1.tar.gz --no-check-certificate
tar -zxvf v3.9.1.tar.gz
cd json-3.9.1
mkdir build
cd build
cmake ../
make -j4
make install
```

7.安装编译Jemalloc
```
wget https://github.com/jemalloc/jemalloc/archive/refs/tags/5.2.1.tar.gz --no-check-certificate
tar -zxvf 5.2.1.tar.gz
cd jemalloc-5.2.1
./autogen.sh --disable-initial-exec-tls
make -j2
make install
```

8.安装编译ProtocolBuf
```
wget https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.12.3.zip --no-check-certificate
unzip v3.12.3.zip
cd protobuf-3.12.3
./autogen.sh
./configure
make
make install
ldconfig
```

9.安装编译ORC
```
git clone -b v1.7.0 https://github.com/apache/orc.git
cd orc
mkdir build && cd build
cmake ../ -DBUILD_JAVA=OFF -DANALYZE_JAVA=OFF -DBUILD_LIBHDFSPP=ON -DBUILD_CPP_TESTS=OFF -DBUILD_TOOLS=ON -DBUILD_POSITION_INDEPENDENT_LIB=ON
make && make install //完成orc组件的安装
gcc -shared -fPIC -o liborc.so -Wl,--whole-archive /usr/local/lib/libhdfspp_static.a /usr/local/lib/liborc.a -Wl,--no-whole-archive -lsasl2 -lssl -lcrypto -lpthread -lprotobuf -lsnappy -lzstd -llz4
cp liborc.so /opt/lib/liborc.so
注：如果配置了代理还是有cmake下载失败的问题，请直接使用下面的zip包
orc.zip is avaiable in onebox：
https://onebox.huawei.com/v/2c21ea5bb8604cf946a8b58d69ae2587
注：执行期间报错：core-site.xml is invalid。 执行： export HADOOP_CONF_DIR=core-site所在的目录。默认为 export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
```

10.编译安全函数库
```
git clone -b tag_huawei_secure_c_V100R001C01SPC010B002_00002 ssh://git@gitlab.huawei.com:2222/hwsecurec_group/huawei_secure_c.git secure_c // 安全函数库下载
cd uawei_secure_c/src
make
cd huawei_secure_c/lib/libsecurec.so
cp libsecurec.so /usr/local/lib/
注：安全库下载失败可以到 https://codehub-y.huawei.com/hwsecurec_group/huawei_secure_c/files?ref=tag_Huawei_Secure_C_V100R001C01SPC011B003_00001下获取
```

## 运行环境部署
1.环境准备
```
和编译环境的前三步一致。运行环境确认的需要的五个so包是libLLVM-12.so，libjemalloc.so.2，libprotobuf.so.23，libsecurec.so和liborc.so，将so包放入/opt/lib下。
```
2.安装编译LLVM
```
wget https://github.com/llvm/llvm-project/archive/refs/tags/llvmorg-12.0.0.tar.gz --no-check-certificate
tar -zxvf llvmorg-12.0.0.tar.gz
cd llvm-project-llvmorg-12.0.0
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DLLVM_BUILD_LLVM_DYLIB=true -DLLVM_ENABLE_PROJECTS="clang;libcxx;libcxxabi" -G "Unix Makefiles" ../llvm
make -j64
cd lib
cp libLLVM-12.so /opt/lib
scp libLLVM-12.so agent*/opt/lib  // 此步骤需要将libLLVM-12.so分发到集群所有节点的/opt/lib下
ln -s /usr/local/bin/clang-12 /usr/local/bin/clang++-12
```

3.安装编译Jemalloc
```
wget https://github.com/jemalloc/jemalloc/archive/refs/tags/5.2.1.tar.gz --no-check-certificate
tar -zxvf 5.2.1.tar.gz
cd jemalloc-5.2.1
./autogen.sh --disable-initial-exec-tls
make -j2
cd lib
cp libjemalloc.so.2 /opt/lib
scp libjemalloc.so.2 agent*/opt/lib   // 此步骤需要将libjemalloc.so.2分发到集群所有节点的/opt/lib下
```

4.安装编译ProtocolBuf
```
wget https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.12.3.zip --no-check-certificate
unzip v3.12.3.zip
cd protobuf-3.12.3
./autogen.sh
./configure
make
cd src/.libs
cp libprotobuf.so.23 /opt/lib
scp libprotobuf.so.23 agent*/opt/lib  // 此步骤需要将libprotobuf.so.23分发到集群所有节点的/opt/lib下
ldconfig
```

5.编译安全函数库
```
git clone -b tag_huawei_secure_c_V100R001C01SPC010B002_00002 ssh://git@gitlab.huawei.com:2222/hwsecurec_group/huawei_secure_c.git secure_c // 安全函数库下载
cd uawei_secure_c/src
make
cd huawei_secure_c/lib/libsecurec.so
cp libsecurec.so /opt/lib
scp libsecurec.so agent*/opt/lib  // 此步骤需要将libsecucec.so分发到集群所有节点的/opt/lib下
注：安全库下载失败可以到 https://codehub-y.huawei.com/hwsecurec_group/huawei_secure_c/files?ref=tag_Huawei_Secure_C_V100R001C01SPC011B003_00001下获取
```

6.编译ORC库
```
git clone -b v1.7.0 https://github.com/apache/orc.git
cd orc
mkdir build && cd build
cmake ../ -DBUILD_JAVA=OFF -DANALYZE_JAVA=OFF -DBUILD_LIBHDFSPP=ON -DBUILD_CPP_TESTS=OFF -DBUILD_TOOLS=ON -DBUILD_POSITION_INDEPENDENT_LIB=ON
make && make install //完成orc组件的安装
gcc -shared -fPIC -o liborc.so -Wl,--whole-archive /usr/local/lib/libhdfspp_static.a /usr/local/lib/liborc.a -Wl,--no-whole-archive -lsasl2 -lssl -lcrypto -lpthread -lprotobuf -lsnappy -lzstd -llz4
cp liborc.so /opt/lib/liborc.so
注：如果配置了代理还是有cmake下载失败的问题，请直接使用下面的zip包
orc.zip is avaiable in onebox：
https://onebox.huawei.com/v/2c21ea5bb8604cf946a8b58d69ae2587
注：执行期间报错：core-site.xml is invalid。 执行： export HADOOP_CONF_DIR=core-site所在的目录。默认为 export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
```
## 编译OmniJit代码和Spark插件仓代码
1.拉取OmniRuntime代码，并且编译得到so包和jar包
```
cd /home
git clone https://codehub-dg-y.huawei.com/Kunpeng-Computing-DC/BigData/OmniOperatorJIT.git
cd /home/OmniOperatorJIT/core/build
sh build.sh release  // 此步骤可以将编译得到的so包安装到/opt/lib目录下面

cd /home/OmniOperatorJIT/bindings/java
mvn clean install -Ddep.os.arch=-aarch64 -DskipTests // 此步骤可以编译得到boostkit-omniop-bindings-1.0.0-aarch64.jar
cp /home/OmniOperatorJIT/bindings/java/target/boostkit-omniop-bindings-1.0.0-aarch64.jar /opt/lib
```

2.编译BoostKit Spark Native Sql Engine Extension
```
cd /home
git clone https://codehub-dg-y.huawei.com/Kunpeng-Solution/thestral_plugin.git
cd /home/thestral_plugin
mvn clean package -Ddep.os.arch=-aarch64 -DskipTests // 此步骤可以在thestral_plugin/java/target目录下面得到boostkit-omniop-spark-3.1.1-1.0.0-aarch64.jar
cp /home/thestral_plugin/java/target/boostkit-omniop-spark-3.1.1-1.0.0-aarch64.jar /opt/lib
```

3.从发布包中下载并得到dependencies，放入/opt/lib中
```
解压获取得到的 dependencies 需要将目录和目录下面的文件一起放到/opt/lib下面。
```

4.将/opt/lib整个目录下面所有的so和jar包拷贝到每个节点，包括计算节点和控制节点
```
scp -r /opt/lib server1_IP:/opt
ssh server1_IP "scp -r /opt/lib agent1_IP:/opt"
ssh server1_IP "scp -r /opt/lib agent2_IP:/opt"
ssh server1_IP "scp -r /opt/lib agent3_IP:/opt"
```

5.在执行的终端输入命令：export LD_LIBRARY_PATH=/opt/lib

6.执行如下的命令，注意替换jar包具体的路径，数据库名称，和具体执行SQL的名称
```
/usr/local/spark/bin/spark-sql --deploy-mode client --driver-cores 5 --driver-memory 5g --num-executors 18 --executor-cores 21 --executor-memory 10g --master yarn  --conf spark.executor.memoryOverhead=5g --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=45g --conf spark.task.cpus=1 --conf spark.driver.extraClassPath=/opt/lib/boostkit-omniop-spark-3.1.1-1.0.0-aarch64.jar:/opt/lib/boostkit-omniop-bindings-1.0.0-aarch64.jar:/opt/lib/dependencies/* --conf spark.executor.extraClassPath=/opt/lib/boostkit-omniop-spark-3.1.1-1.0.0-aarch64.jar:/opt/lib/boostkit-omniop-bindings-1.0.0-aarch64.jar:/opt/lib/dependencies/* --conf spark.sql.codegen.wholeStage=false --conf  spark.executorEnv.LD_LIBRARY_PATH="/opt/lib/" --conf spark.driverEnv.LD_LIBRARY_PATH="/opt/lib/" --conf spark.executor.extraLibraryPath=/opt/lib --conf spark.driverEnv.LD_PRELOAD=/opt/lib/libjemalloc.so.2 --conf spark.executorEnv.LD_PRELOAD=/opt/lib/libjemalloc.so.2 --conf spark.sql.extensions="com.huawei.boostkit.spark.ColumnarPlugin" --jars /opt/lib/boostkit-omniop-spark-3.1.1-1.0.0-aarch64.jar --jars /opt/lib/boostkit-omniop-bindings-1.0.0-aarch64.jar  --conf spark.sql.orc.impl=native --conf spark.shuffle.manager="org.apache.spark.shuffle.sort.ColumnarShuffleManager" --conf spark.sql.join.columnar.preferShuffledHashJoin=true --database tpcds_bin_partitioned_orc_2 --name shuffle_sql1 -f /usr/local/spark/bin/sqls/sql1.sql
```

7.其他注意事项
```
1启用列式shuffle添加配置项
--conf spark.shuffle.manager="org.apache.spark.shuffle.sort.ColumnarShuffleManager"

2启用ORC配置项
使用C++ 版本native ORC（该参数是默认参数，如果不指定则为该场景）:
--conf spark.sql.orc.impl=native

使用spark版本native ORC:
--conf spark.sql.orc.impl=native
--conf spark.sql.columnar.nativefilescan.enable=false

使用spark版本Hive ORC:
--conf spark.sql.orc.impl=hive
```

## Coding Style