#!/bin/bash
cd ~/ids-bigdata
mkdir -p results
OUT=results/scale_results.jsonl
run() {
  echo "$(date +%H:%M:%S) running files=$1 cores=$2"
  docker compose exec -T spark-master /opt/spark/bin/spark-submit /src/scale_test.py "$1" "$2" \
    2> "results/scale_${1}f_${2}c.log" | grep '^RESULT' | sed 's/^RESULT //' >> "$OUT"
}
RUNS=("$@")
if [ ${#RUNS[@]} -eq 0 ]; then RUNS=("1 4" "5 4" "10 4" "15 4" "5 2" "5 1"); fi
for r in "${RUNS[@]}"; do run $r; done
echo "$(date +%H:%M:%S) done"
