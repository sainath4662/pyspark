#!/bin/bash

spark-submit \
  --master yarn \
  src/main.py --env prod
