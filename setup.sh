#!/bin/bash

################### 
# LIGHTING DRIVER #
###################

CURRENT_DIR=$(basename `pwd`)
CURRENT_PATH=`pwd`
ID=$RANDOM

DEBUG_TAG="[DEBUG]: "
DT=$DEBUG_TAG
#MOUNT_DRIVE_NAME="lighting-disk-$CURRENT_DIR-$(date +%F.%N)-$ID"
MOUNT_DRIVE_NAME="lighting-disk-$CURRENT_DIR"
MOUNT_PT="/tmp/$MOUNT_DRIVE_NAME"

echo "$DT MOUNTED DISK: $MOUNT_DRIVE_NAME"

if [ ! -f $MOUNT_PT ];
then
  mkdir -p $MOUNT_PT 
  echo "$DT MOUNT_PT: $MOUNT_PT"
else 
  # clean the old folder
  sudo rm -rf $MOUNT_PT
fi

#CMD_CP="cp -ruf $MOUNT_PT/** ./"
#CMD_CP_LOG="$CMD_CP 1> /dev/null 2>&1"
#CMD_CP_LOG="$CMD_CP"

echo "$DT starting app ..."

sudo mount -t tmpfs $MOUNT_DRIVE_NAME $MOUNT_PT && \
cp -ruf ./. $MOUNT_PT && \
#cd $MOUNT_PT && ./compile.sh && ./start.sh && $($CMD_CP_LOG) && rm -rf $MOUNT_PT && echo "$DT all ok!"
cd $MOUNT_PT && ./start.sh $CURRENT_PATH;

# wait for all finished signal
  
#cd $CURRENT_PATH && bash -c "$CMD_CP_LOG" && umount $MOUNT_PT && rm -rf $MOUNT_PT && echo "$DT all ok!"

