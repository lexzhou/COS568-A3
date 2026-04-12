#!/bin/bash
#SBATCH --job-name=cos568_m3
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=03:00:00
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

echo "Starting Milestone 3 benchmarks at $(date)"
echo "Node: $(hostname)"

# Setup environment
export LD_LIBRARY_PATH=/usr/local/boost/1.85.0/lib64:$LD_LIBRARY_PATH

# Build (skip on compute node — binary built on login node)
cd /scratch/gpfs/HENDERSON/lz5066/COS568-A3
if [ ! -f build/benchmark ]; then
    echo "Building benchmark..."
    bash scripts/build_benchmark.sh
else
    echo "Using pre-built benchmark binary."
fi

# Run M3 benchmarks (all 3 datasets, both workloads)
echo "Running M3 benchmarks..."
bash scripts/run_m3_benchmarks.sh

# Run analysis
echo "Running analysis..."
module load anaconda3/2023.3
python scripts/analysis_m3.py

echo "Milestone 3 complete at $(date)"
