#!/bin/bash
if [ $# -eq 0 ]; then
  echo "No arguments supplied"
fi
bq query --use_legacy_sql=false < $1