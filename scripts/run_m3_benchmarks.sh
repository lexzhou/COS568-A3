#!/usr/bin/env bash

echo "Executing M3 benchmarks (all datasets, mixed workloads)..."

BENCHMARK=build/benchmark
if [ ! -f $BENCHMARK ]; then
    echo "benchmark binary does not exist"
    exit 1
fi

function execute_m3() {
    local DATA=$1
    local INDEX=$2
    echo "Executing operations for $DATA and index $INDEX"
    echo "  Mixed workload with insert-ratio 0.1 (90% lookup)"
    $BENCHMARK ./data/$DATA ./data/${DATA}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix --through --csv --only $INDEX -r 3
    echo "  Mixed workload with insert-ratio 0.9 (90% insert)"
    $BENCHMARK ./data/$DATA ./data/${DATA}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix --through --csv --only $INDEX -r 3
}

mkdir -p ./results

DATASETS="fb_100M_public_uint64 books_100M_public_uint64 osmc_100M_public_uint64"

# Clear old M3 results for mix workloads across all datasets
for DATA in $DATASETS; do
    rm -f ./results/${DATA}_ops_2M_*_mix_results_table.csv
done

# Run benchmarks for each dataset and each index
for DATA in $DATASETS; do
    echo ""
    echo "===== Dataset: $DATA ====="
    for INDEX in DynamicPGM LIPP HybridPGMLIPP; do
        execute_m3 ${DATA} $INDEX
    done
done

echo ""
echo "===================M3 Benchmarking complete!===================="

# Add headers for CSV files
for FILE in ./results/*_mix_results_table.csv; do
    if [ ! -f "$FILE" ]; then
        continue
    fi
    if head -n 1 "$FILE" | grep -q "index_name"; then
        echo "Header already present in $FILE"
    else
        sed -i '1s/^/index_name,build_time_ns1,build_time_ns2,build_time_ns3,index_size_bytes,mixed_throughput_mops1,mixed_throughput_mops2,mixed_throughput_mops3,search_method,pgm_error,flush_pct,max_buffer\n/' "$FILE"
        echo "Header added to $FILE"
    fi
done
