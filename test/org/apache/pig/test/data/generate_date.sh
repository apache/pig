#!/bin/bash

if  [ $# -eq 0 ]
then
        echo "20080228";
elif [ $# -eq 1 ]
then
        #echo `date +%Y``date +%m``date +%d`;
        echo $1;
fi
