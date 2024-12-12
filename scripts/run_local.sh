#!/bin/bash

spark-submit \
  --master local[*] \
  D:\pyspark\Project\src\main.py --env default
