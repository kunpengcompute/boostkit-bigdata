OmniShield 对接 rats TLS demo

OmniShield提供了Spark on YARN 应用级远程证明的能力。 使用者需自行实现OmniShield提供的RemoteAttestationAgent的接口。下面将以BoostKit virtCCA 的rats tls 为例完成demo演示。


**1.编译demo jar 包**

------------



在omnishield目录执行如下指令，指令执行完成后将生成target目录，target目录下的 omnishield-1.0-SNAPSHOT-rats-tls-demo.jar 即为 demo jar 包
```
mvn clean package -DskipTests=true
```
<br>

**2.配置resourcemanager所在节点**

------------



2.1 放置virtcca-client到/usr/bin目录下

virtcca-client 生成参考virtcca或rats-tls相关文档获取
```
cp virtcca-client /usr/bin
```
2.2 创建socket_server文件夹
```
mkdir socket_server
```
2.3 编辑secret.json 并放到socket_server文件夹中
```
vi secret.json
```
输入以下内容
```
{"key1": "value1", "key2": "value2", "key3": "value3"}

```
2.4 编辑base.json并放到socket_server文件夹中
```
vi base.json
```
输入以下内容，work 表示nodeManager所在节点的hostname, ip 表示nodeManager所在节点的ip, baseValue表示nodeManager所在节点的虚机基线度量值, imaPath表示IMA度量的文件路径
```
{
    "work": {
        "ip": "192.168.122.99",
        "baseValue": "60b79bd1e07f63c4c18e42929923c835e1fc1e32a0692326c086c395af9bab60",
        "imaPath" : "work.ima"
    }
}

```
2.5 编辑IMA度量的文并放到socket_server文件夹中

计算jar包基线度量值，可通过如下指令计算
```
sh imahash.sh file /usr/local/spark/examples/jars/spark-examples_2.12-3.3.1.jar
```
指令输出最后一行如下，2c87... 即为对应jar包的基线度量值
```
2c87c97589a97d4a1306474a694506baea2d6674e61fc3113c1c85411641e8ff  /usr/local/spark/examples/jars/spark-examples_2.12-3.3.1.jar
```
计算spark jars 文件夹基线度量值，可通过如下指令计算
```
sh imahash.sh sparkdir /usr/local/spark/jars
```
指令输出最后一行如下，bbe4... 即为对应spark jars 文件夹的基线度量值
```
bbe4027c24426f496a2007120961733eada3976b8e9ef2e19e998ca9e7bd6764  dir.tar
```
将步骤2.4中的虚机基线度量值，jar包的基线度量值 和 spark jars 文件夹的基线度量值写入步骤2.4中指定的IMA度量的文件路径中
```
echo "60b79bd1e07f63c4c18e42929923c835e1fc1e32a0692326c086c395af9bab60
> 2c87c97589a97d4a1306474a694506baea2d6674e61fc3113c1c85411641e8ff
> bbe4027c24426f496a2007120961733eada3976b8e9ef2e19e998ca9e7bd6764" >work.ima
```
2.6 放置证书和jar包到socket_server文件夹中

```
放置virtcca-client所需证书 aik_cert.der，root_cert.pem，sub_cert.pem 文件到socket_server文件夹中 
放置sorcket server所需证书 server.keystore，server.crt 文件到socket_server文件夹中 
放置omnishield-1.0-SNAPSHOT-rats-tls-demo.jar文件到socket_server文件夹中 

virtcca-client 证书参考virtcca或rats-tls相关文档获取
sorcket server 证书需自行生成，演示demo中使用的是通过Openssl生成的自签名证书
```

2.7 检查socket_server文件夹中文件

socket_server中应该具备以下文件
```
├── aik_cert.der
├── base.json
├── omnishield-1.0-SNAPSHOT-rats-tls-demo.jar
├── work.ima
├── root_cert.pem
├── secret.json
├── server.crt
├── server.keystore
└── sub_cert.pem
```
**3.配置nodemanager所在节点**

------------
3.1 放置virtcca-server到/usr/bin目录下
```
cp virtcca-server到 /usr/bin
```
virtcca-server 生成参考virtcca或rats-tls相关文档获取

3.2 添加用于做ima度量的用户

```
useradd attest
```
3.3 配置ima度量策略

1001 是步骤3.2中新增的attest用户的id，根据实际情况修改。可以通过cat /etc/passwd | grep attest 查寻id。

```
echo "measure func=FILE_CHECK mask=MAY_READ fowner=1001" > policy
```
3.4 生效ima策略
```
cat policy > /sys/kernel/security/ima/policy
```
3.5 配置ima度量环境变量

编辑/etc/profile文件，在文件末尾添加如下内容
```
export IMA_USER=attest
```
3.6 创建haoop rats文件夹

在HADOOP_HOME路径下执行如下指令
```
mkdir rats
```
3.7 放置证书到rats文件夹中
```
放置sorcket client所需证书 client.keystore，client.crt 文件到rats文件夹中
```
3.8 编辑socket.properties并放到rats文件夹中
```
vi socket.properties
```
输入以下内容，serverip表示socket服务端ip地址即resourcemanager所在节点的ip地址, serverport表示socket端口, truststorepath表示步骤3.8中client.keystore的文件路径, truststorepwd表示Truststore密码
```
serverip=x.x.x.x
serverport=8443
truststorepath=/usr/local/hadoop/rats/client.keystore
truststorepwd=123456
```
4.启动YARN和Socket服务

4.1 在resourcemanager节点启动resourcemanager
```
yarn-daemon.sh start resourcemanager
```
4.2 在resourcemanager节点启动Socket服务

在2.2步骤创建的文件夹socket_server中执行如下指令
```
java -cp omnishield-1.0-SNAPSHOT-rats-tls-demo.jar com.huawei.analytics.shield.ra.example.RatsTLSRemoteAttestationServer server.keystore secret.json
```
4.3 在nodemanager节点启动nodemanager
```
yarn-daemon.sh start nodemanager
```
5.提交作业

5.1 在resourcemanager节点提交spark作业
```
spark-submit --master yarn  --class org.apache.spark.examples.SparkPi --num-executors 1  --executor-cores 1 --executor-memory 1G --conf spark.remote.attestation.enable=true  --conf spark.remote.attestation.agent.class=rats.RatsTLSRemoteAttestationAgent --conf spark.network.timeout=1200 --conf spark.yarn.max.executor.failures=1 --conf spark.yarn.maxAppAttempts=1  --jars "omnishield-1.0-SNAPSHOT.jar,rats-tls-demo-1.0.jar"  --conf spark.executor.extraClassPath=omnishield-1.0-SNAPSHOT.jar:rats-tls-demo-1.0.jar --driver-class-path omnishield-1.0-SNAPSHOT.jar:rats-tls-demo-1.0.jar  /usr/local/spark/examples/jars/spark-examples_2.12-3.3.1.jar
```
5.2 观察Socket服务控制台打印日志

spark executor启动时会触发远程证明，远程证明通过时socket控制台有如下日志打印
```
remote attestation request arrived, executorHostName: node executorId: 1, start handle msg...
run virtcca-client -i x.x.x.x -p xx -r 60b79bd1e07f63c4c18e42929923c835e1fc1e32a0692326c086c395af9bab60 -d /opt/work.ima
startRatsTlsClient Exit Code: 0
remote attestation pass
start reply msg
msg sending completed
```
