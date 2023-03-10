# for productions
#mvn clean -Pquickclean

RUN_TAG=$(echo "TEST")

FLINK_LOG=./logs/runner.flink.collection.$(date "+%F.%N").$RUN_TAG.log;

echo $(java -version) > $FLINK_LOG

echo "run.flink.collection.sh"

rm ./outputs/**.json**

rm runner.flink.collection*

# clear && mvn compile exec:java -T1000C -Dexec.mainClass="org.apache.beam.examples.App" \
# -Dexec.args="--runner=DirectRunner" -Pdirect-runner -X -e > ./outputs/run.log 2>&1

export MAVEN_OPTS="-Xms1024m -Xmx10240m -XX:MaxDirectMemorySize=8192m"

clear && mvn --offline -am -T800 exec:java -Dexec.mainClass="org.apache.beam.examples.App" \
 -Dexec.args=" \
 --runner=FlinkRunner \
 --flinkConfDir=$PWD/config \
 --flinkMaster=[collection] \
 --resourceHints=min_ram=10GB \
 --objectReuse=true \
 " \
 -Pflink-runner -X -e > $FLINK_LOG 2>&1 > $FLINK_LOG 2>&1 

