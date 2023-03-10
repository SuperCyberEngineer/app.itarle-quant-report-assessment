#!/bin/bash

########
# Vars #
########
HOST="${VARIABLE:-127.0.0.1}"
USER="${VARIABLE:-oem}"
PASSWORD="${VARIABLE:-oem}"
DB="${VARIABLE:-oem}"

PGPASSWORD=$PASSWORD

echo $COPY_FILE_ABS_PATH

while true; do
    echo "looking for cleaned csv..."
    # SIGNAL=./inputs/finished-cleaning
    # (ls ./inputs/finished-cleaning 1> /dev/null 2>&1 && echo finish && break;) || echo "."
    if [ -f "./inputs/finished-cleaning" ]; then
        echo "prepare to import the cleaned data ..."
        break
    fi
    sleep 3
done

CLEANED_DATA_FILE_TAG=cleaned.csv
# search cleaned file
COPY_FILE_ABS_PATH=$(find $(pwd) -name "*$CLEANED_DATA_FILE_TAG" | awk 'NR==1')

export COPY_FILE_ABS_PATH && ./bin/envsubst <./scripts/data-importing/init.sql.template >./scripts/data-importing/init.sql 2>&1 && echo envsubst ok

export PGPASSWORD

while true; do
    echo "pending..."
    if time psql -U $USER -h localhost -p 5432 -f ./scripts/data-importing/init.sql 2>&1; 
    then
    echo psql ok
    break
    fi
    sleep 1
done

