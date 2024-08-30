find ./LoadsMetric/LoadsMetricServer/package/src/main/resources -type f -exec dos2unix {} \;
find ./LoadsMetric/LoadsMetricServer/server/src/main/resources -type f -exec dos2unix {} \;
cd LoadsMetric/LoadsMetricServer
mvn clean package -DskipTests=true -Dos.detected.arch=aarch64

cd ../../yarn-schedule-load-evolution
find ./src/main/resources -type f -exec dos2unix {} \;
mvn clean package -DskipTests=true -Dos.detected.arch=aarch64