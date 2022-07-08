# How to build omniop-openlookeng-extension
The project depends on pieces of `hetu-core` and `boostkit-omniop-bindings`, which are recommended to deployed beforehand.  `hetu-core` is available in the central repo, and this project requests the version >= `1.6.1`. Thus, you only need to 
 get `boostkit-omniop-bindings-1.0.0.jar` from [Link](https://www.hikunpeng.com/en/developer/boostkit/big-data?acclerated=3), and install it in your maven repository as follows:

 ```
 mvn install:install-file -DgroupId=com.huawei.boostkit -DartifactId=boostkit-omniop-bindings -Dversion=1.0.0 -Dpackaging=jar -Dfile=boostkit-omniop-bindings-1.0.0.jar
 ```

Then, you can build omniop-openlookeng-extension as follows:
```
git clone https://github.com/kunpengcompute/boostkit-bigdata.git
cd ./boostkit-bigdata/omnioperator/omniop-openlookeng-extension
mvn clean install -DskipTests
```
