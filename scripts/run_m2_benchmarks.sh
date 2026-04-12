#!/usr/bin/env bash

echo "Executing M2 benchmarks (Facebook dataset, mixed workloads)..."

BENCHMARK=build/benchmark
if [ ! -f $BENCHMARK ]; then
    echo "benchmark binary does not exist"
    exit 1
fi

function execute_m2() {
    echo "Executing operations for $1 and index $2"
    echo "  Mixed workload with insert-ratio 0.1 (90% lookup)"
    $BENCHMARK ./data/$1 ./data/$1_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix --through --csv --only $2 -r 3
    echo "  Mixed workload with insert-ratio 0.9 (90% insert)"
    $BENCHMARK ./data/$1 ./data/$1_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix --through --csv --only $2 -r 3
}

mkdir -p ./results

# Clear old results for these specific workloads to avoid stale data
rm -f ./results/fb_100M_public_uint64_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix_results_table.csv
rm -f ./results/fb_100M_public_uint64_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix_results_table.csv

DATA="fb_100M_public_uint64"

for INDEX in DynamicPGM LIPP HybridPGMLIPP
do
    execute_m2 ${DATA} $INDEX
done

echo "===================M2 Benchmarking complete!===================="

# Add headers for CSV files
for FILE in ./results/fb_100M_public_uint64*mix*results_table.csv
do
    if head -n 1 "$FILE" | grep -q "index_name"; then
        echo "Header already present in $FILE"
    else
        sed -i '1s/^/index_name,build_time_ns1,build_time_ns2,build_time_ns3,index_size_bytes,mixed_throughput_mops1,mixed_throughput_mops2,mixed_throughput_mops3,search_method,value\n/' "$FILE"
        echo "Header added to $FILE"
    fi
done
