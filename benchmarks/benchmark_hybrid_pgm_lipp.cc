#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                   bool pareto, const std::vector<int>& params) {
  if (!pareto) {
    util::fail("HybridPGMLIPP's hyperparameter cannot be set");
  } else {
    // Sweep pgm_error with default flush_pct=5
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 32>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 64>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 128>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 256>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 512>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 1024>>();
  }
}

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, const std::string& filename) {
  // For each dataset, sweep configurations for mixed workloads.
  //
  // Insert-heavy (90% insert): max_buffer=0, sweep flush_pct.
  //   Double-buffered incremental drain handles the high insert rate.
  //
  // Lookup-heavy (10% insert): micro-threshold via max_buffer (1-256).
  //   Drain-during-lookup empties the buffer on the first lookup after each
  //   insert, so the skip-when-empty fast path fires ~89% of the time.
  //   Also test percentage-based flush for comparison.

  if (filename.find("fb_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      // --- Percentage-based flush (insert-heavy sweet spot) ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 2>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 20>>();
      // Other pgm_error values
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 512, 5>>();
      // --- Drain-during-insert (lookup-heavy, compliant) ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 1>>();
      // --- Micro-threshold (lookup-heavy with drain-during-lookup) ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 8>>();
    }
  }
  if (filename.find("books_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      // --- Percentage-based flush ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 2>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 20>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 512, 5>>();
      // --- Drain-during-insert (lookup-heavy, compliant) ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 1>>();
      // --- Micro-threshold (lookup-heavy with drain-during-lookup) ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 8>>();
    }
  }
  if (filename.find("osmc_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      // --- Percentage-based flush ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 2>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 20>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 512, 5>>();
      // --- Drain-during-insert (lookup-heavy, compliant) ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 1>>();
      // --- Micro-threshold (lookup-heavy with drain-during-lookup) ---
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 8>>();
    }
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
