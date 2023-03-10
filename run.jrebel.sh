#!/bin/bash
export REBEL_HOME=/home/oem/Desktop/jrebel
export ANT_OPTS=-agentpath:$REBEL_HOME/lib/libjrebel64.so
ant devmode