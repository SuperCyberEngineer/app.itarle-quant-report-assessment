#!/bin/bash

# please install sdk java 

# sdk use java 17.0.4-amzn

LOG=./logs/compile.log

echo "" > $LOG
echo $(java -version) >> $LOG
echo $JAVA_HOME >> $LOG
echo $MAVEN_OPTS >> $LOG

export MAVEN_OPTS="-Xmx10240m -XX:MaxDirectMemorySize=10240m"

time mvn clean -Pquickclean

time mvn compile -am -T800 -X -e 2>&1
