# /usr/bin/sh

RUN_TAG=$(echo "10000linesGroupByOnly")

# for productions
#mvn clean -Pquickclean

# echo "" > ./run.log

# rm ./outputs/**.json** &

rm runner.

FLINK_LOG=runner.flink.$(date "+%F.%N").$RUN_TAG.log;

touch $FLINK_LOG

./package.sh && clear && mvn compile exec:java -T100C -Dexec.mainClass="org.apache.beam.examples.App" \
 -Dexec.args=" \
 --runner=FlinkRunner \
 --flinkMaster=localhost:6123 \
 --filesToStage=./target/*.jar \
 " -Pflink-runner -X -e > $FLINK_LOG 2>&1