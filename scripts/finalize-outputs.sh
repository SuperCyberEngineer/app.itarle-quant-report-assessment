#!/bin/bash

while true; do
    echo "checking if processed"
    if [ -f "./__SUCCESS__" ]; then
        echo "success to process ..."
        break
    fi
    sleep 3
done

cat ./outputs/QuantStockReports.json* > ./outputs/_QuantStockReports.json
mv ./outputs/_QuantStockReports.json `cat __ORGINAL_FOLDER__`/outputs/QuantStockReports.json
mv ./logs/* `cat __ORGINAL_FOLDER__`/logs/

# orginal folder result
