# for productions
#mvn clean -Pquickclean

RUN_TAG=$(echo "TEST")

FLINK_LOG=./logs/runner.flink.local.$(date "+%F.%H.%M.%S").$RUN_TAG.log;

# echo $(java -version) > $FLINK_LOG

echo "run.flink.local.sh"

rm ./outputs/**.json**

# rm /tmp/runner.flink.local*

# clear && mvn compile exec:java -T1000C -Dexec.mainClass="org.apache.beam.examples.App" \
# -Dexec.args="--runner=DirectRunner" -Pdirect-runner -X -e > ./outputs/run.log 2>&1
export MAVEN_OPTS="-Xms1024m -Xmx10240m -XX:MaxDirectMemorySize=8192m"

time mvn -am -T800 exec:java -Dexec.mainClass="org.apache.beam.examples.App" \
 -Dexec.args=" \
 --runner=FlinkRunner \
 --flinkConfDir=$PWD/config \
 --flinkMaster=[local] \
 --resourceHints=min_ram=10GB \
 --objectReuse=true \
 " -Pflink-runner -X -e 2>&1 && touch __SUCCESS__;

#   ./scripts/finalize-outputs.sh