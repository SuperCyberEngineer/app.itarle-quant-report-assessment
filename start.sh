#!/usr/bin/bash

export UID=$(id -u)
export GID=$(id -g)

touch __ORGINAL_FOLDER__
echo $@ > __ORGINAL_FOLDER__


./reset.sh && docker-compose -f docker-compose.yml up -d

./scripts/finalize-outputs.sh