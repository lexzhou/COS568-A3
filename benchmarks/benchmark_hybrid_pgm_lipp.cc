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
  // For each dataset, test pgm_error=64 with various flush_pct values,
  // plus a couple of other pgm_error values at the default flush_pct.
  if (filename.find("fb_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      // Sweep flush_pct with best pgm_error=64
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 1>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 2>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 20>>();
      // Also test other pgm_error values at flush_pct=5
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 512, 5>>();
      // Small absolute buffer cap (lookup-heavy optimization)
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 256>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 1024>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 4096>>();
      // Never-flush: all inserts stay in DPGM, LIPP stays perfectly bulk-loaded
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 250000>>();
    }
  }
  if (filename.find("books_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 1>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 2>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 20>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 512, 5>>();
      // Small absolute buffer cap (lookup-heavy optimization)
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 256>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 1024>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 4096>>();
      // Never-flush: all inserts stay in DPGM, LIPP stays perfectly bulk-loaded
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 250000>>();
    }
  }
  if (filename.find("osmc_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 1>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 2>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 20>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 512, 5>>();
      // Small absolute buffer cap (lookup-heavy optimization)
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 256>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 1024>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 4096>>();
      // Never-flush: all inserts stay in DPGM, LIPP stays perfectly bulk-loaded
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 100, 250000>>();
    }
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
