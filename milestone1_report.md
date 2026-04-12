# COS568 Assignment 3 — Milestone 1 Report

## Task: Evaluate Lookup and Insertion Performance of B+Tree, Dynamic PGM, and LIPP

We benchmark three index structures across three datasets (Facebook, Books, OSMC) and four workloads. Each experiment is repeated 3 times; we report the average throughput (Mops/s) using the best hyperparameter configuration per index. For BTree and DynamicPGM, which have configurable search methods and node/error parameters, we select the single configuration with the highest average throughput for each workload. LIPP has no tunable hyperparameters.

## Results

### 1. Lookup-only Throughput (Mops/s)

| Index | Facebook | OSMC | Books |
|-------|----------|------|-------|
| BTree | 0.83 | 1.26 | 0.98 |
| DynamicPGM | 1.94 | 2.30 | 2.14 |
| LIPP | **609.95** | **609.24** | **548.77** |

LIPP achieves ~300x higher lookup throughput than both BTree and DynamicPGM.

### 2. Insert+Lookup Throughput (50% insert ratio)

**Insert throughput (Mops/s):**

| Index | Facebook | OSMC | Books |
|-------|----------|------|-------|
| BTree | 0.67 | 0.67 | 0.74 |
| DynamicPGM | **7.92** | **7.14** | **7.81** |
| LIPP | 1.47 | 1.19 | 2.13 |

**Lookup throughput after insertion (Mops/s):**

| Index | Facebook | OSMC | Books |
|-------|----------|------|-------|
| BTree | 0.76 | 1.16 | 0.89 |
| DynamicPGM | 0.47 | 0.16 | 0.40 |
| LIPP | **618.10** | **634.24** | **603.27** |

DynamicPGM has the best insert throughput (~4-5x faster than LIPP, ~10x faster than BTree), while LIPP maintains its dominant lookup performance even after insertions. Note that DynamicPGM's lookup throughput after insertions is dramatically lower than its lookup-only performance (e.g., 0.16 vs 2.30 Mops/s on OSMC), because insertions create multiple auxiliary buffers that must all be searched during lookups.

### 3. Mixed Workload — 10% Insert Ratio (Mops/s)

| Index | Facebook | OSMC | Books |
|-------|----------|------|-------|
| BTree | 0.79 | 1.14 | 0.73 |
| DynamicPGM | 0.89 | 1.02 | 0.99 |
| LIPP | **14.14** | **9.89** | **18.02** |

### 4. Mixed Workload — 90% Insert Ratio (Mops/s)

| Index | Facebook | OSMC | Books |
|-------|----------|------|-------|
| BTree | 0.83 | 0.80 | 0.62 |
| DynamicPGM | **3.60** | **3.05** | **3.09** |
| LIPP | 1.82 | 1.20 | 1.99 |

The corresponding bar plots are shown below:

![Benchmark Results](benchmark_results.png)

## Analysis: Why Do We Observe These Differences?

### LIPP dominates lookups

LIPP uses learned linear models at each node to predict the **exact** slot position of a key. When a lookup arrives, the model computes the slot directly — no binary search or scanning is needed within a node. The lookup cost is proportional only to the tree height. This yields throughput orders of magnitude higher than BTree and DynamicPGM, which both require in-node searches at every level.

### DynamicPGM dominates insertions

DynamicPGM uses a "logarithmic method" with exponentially-sized buffers: new keys go into a small buffer, and when it fills, it merges with the next level. This amortizes the cost of rebuilding the piecewise linear model. The amortized insert cost is O(log^2 n / epsilon), which is significantly cheaper than LIPP's insert path (which must traverse the tree to find the right node, and may need to create new child nodes to resolve prediction conflicts) or BTree's insert path (which may trigger expensive node splits propagating up the tree).

### BTree is consistently slowest

BTree relies on pointer-chasing traversal through multiple tree levels. Each internal node comparison requires following a pointer to a potentially non-contiguous memory location, causing frequent cache misses. Neither lookups nor insertions benefit from learning the data distribution, so BTree cannot exploit the structure present in real-world datasets the way learned indexes can.

### DynamicPGM lookup degrades severely after insertions

DynamicPGM's lookup throughput drops 76-93% after insertions compared to lookup-only performance (e.g., OSMC: 2.30 → 0.16 Mops/s). This is because the "logarithmic method" insertion strategy creates multiple auxiliary PGM sub-indexes of exponentially increasing size. During a lookup, all active buffers must be searched, multiplying the lookup cost. In contrast, LIPP's lookup performance is essentially unaffected by prior insertions because its per-node models still provide precise position predictions regardless of how many keys have been inserted.

### Index memory footprint explains part of the speed gap

LIPP uses significantly more memory than the other indexes: ~12.7 GB on Facebook vs ~1.7 GB for DynamicPGM and ~1.9-2.1 GB for BTree. This ~6x memory overhead is the cost of storing per-node learned models with precise position predictions and gap-filled arrays. The larger footprint enables LIPP's O(1)-like per-node lookups but also means the working set may not fit in CPU cache for very large datasets.

### Mixed workload behavior reflects the insert/lookup tradeoff

- **10% insert (lookup-heavy):** LIPP wins because the workload is dominated by lookups, where LIPP's precise position prediction gives it an enormous advantage. The occasional inserts are not frequent enough to become a bottleneck.
- **90% insert (insert-heavy):** DynamicPGM wins because its buffer-based insertion strategy handles high insertion rates efficiently. LIPP's insert path is more expensive per operation (conflict resolution, potential node creation), and the infrequent lookups cannot compensate.

### Dataset differences

Performance varies slightly across datasets due to different key distributions:
- **Facebook**: Relatively uniform distribution, favorable for all indexes.
- **Books**: More clustered/skewed distribution, slightly harder for LIPP's models (lower lookup throughput at 549 vs ~610 Mops/s).
- **OSMC**: Geographic cell IDs with some clustering, similar to Facebook in performance characteristics.

### A note on run-to-run variance

While most measurements are stable across the 3 repetitions, some show notable variance. For example, LIPP on Books lookup-only recorded 433, 604, and 609 Mops/s — the first run is 28% lower, likely due to cold-cache effects during initial execution. Similarly, BTree on Books insert+lookup shows insert throughput ranging from 0.49 to 1.00 Mops/s. These outliers affect the reported averages but do not change the overall ranking or conclusions.

## Conclusion

Learned indexes (LIPP and DynamicPGM) significantly outperform the traditional BTree. However, each learned index has a distinct strength: LIPP for lookups and DynamicPGM for insertions. This motivates the hybrid approach explored in Milestones 2 and 3.
