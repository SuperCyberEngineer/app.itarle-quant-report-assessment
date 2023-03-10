# /usr/bin/sh

RUN_TAG=$(echo "pgsql")

rm ./outputs/**.json** &

rm ./runner.direct** &

DIRECT_LOG=./logs/runner.direct.$(date "+%F.%N").$RUN_TAG.log

touch $DIRECT_LOG

export MAVEN_OPTS="-Xms1024m -Xmx10240m -XX:MaxDirectMemorySize=8192m"

clear && mvn exec:java -T300 -Dexec.mainClass="org.apache.beam.examples.App" \
-Dexec.args="--runner=DirectRunner" -Pdirect-runner -X -e > $DIRECT_LOG 2>&1
