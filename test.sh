echo "" > ./logs/test.log
clear && time mvn -T800 -am -Dmaven.javadoc.skip=true -Dtest=AppTest test -X -e >> ./logs/test.log 2>&1