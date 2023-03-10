#!/bin/sh

docker rm -f data-processing-service.itarle 2>&1 
docker rm -f data-storage-service.itarle 2>&1 
docker rm -f data-cleaning-service.itarle 2>&1
docker rm -f data-importing-service.itarle 2>&1
docker rm -f app-monitoring-service.itarle 2>&1
