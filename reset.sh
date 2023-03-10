# rm cleaned.csv, sql && clear db?

sudo ./stop.sh 2>&1

sudo rm -f ./inputs/*cleaned.csv 2>&1
sudo rm -f ./inputs/finished-cleaning 2>&1
sudo rm -f ./scripts/data-loading/init.sql 2>&1
sudo rm -f ./inputs/finished-cleaning 2>&1
sudo rm -f ./inputs/database-ready 2>&1
