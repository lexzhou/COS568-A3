#!/bin/bash
#SBATCH --job-name=cos568_m2
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=02:00:00
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

set -e

module purge
module load anaconda3/2023.3

cd /scratch/gpfs/HENDERSON/lz5066/COS568-A3

export LD_LIBRARY_PATH=/usr/local/boost/1.85.0/lib64:$LD_LIBRARY_PATH

echo "=== Running M2 benchmarks ==="
bash scripts/run_m2_benchmarks.sh

echo "=== Running M2 analysis ==="
python scripts/analysis_m2.py

echo "=== Milestone 2 complete ==="
