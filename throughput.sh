#!/bin/bash
REPEAT=5
AFFINITY=0
THREADS_SEQ="1 2 4 6 8 12 16 20 24 28 32 40 48 56 64 72 88 104 120 136 144"

PROJECT="src/julia-serial/"
EXEC="src/julia-serial/run_main.jl"

for threads in ${THREADS_SEQ}; do
    for i in $(seq 1 $REPEAT);do
        output_file="timing_${threads}_${i}.csv"
        echo "Starting measurements for ${threads} threads, trial $i"
        CMD=(
          numactl --cpunodebind="${AFFINITY}" --membind="${AFFINITY}" julia -t "${threads}"
          --project="${PROJECT}" "${EXEC}" "${WORKFLOW}" --save-timing="${output_file}"
        )
        CMD=${CMD[*]}
        echo "Running command: ${CMD}"
        $CMD
    done
done
