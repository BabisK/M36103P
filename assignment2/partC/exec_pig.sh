#!/bin/bash

START_TIME=$(date +"%s")

pig assignment.pig &> exec_pig.log

END_TIME=$(date +"%s") 
DIFF=$(($END_TIME-$START_TIME))
echo "$(($DIFF / 60)) minutes and $(($DIFF % 60)) seconds."
