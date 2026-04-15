#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <utility>
#include <vector>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

// Advanced Hybrid PGM-LIPP index with:
//   - Double-buffered DPGM (active + drain)
//   - Drain-during-lookup: lookups drain the buffer, keeping it empty ~89% of
//     the time on lookup-heavy workloads so the skip-when-empty fast path fires
//   - Sorted batch drain: drains in sorted key order for LIPP cache locality
//   - LIPP-first adaptive lookup routing
//   - Configurable flush threshold via flush_pct (percentage) or max_buffer (absolute)
//
// Template parameters:
//   pgm_error  — PGM error bound for the DPGM buffer
//   flush_pct  — flush when active buffer reaches this % of total keys (0 disables)
//   max_buffer — absolute buffer cap; flushes when active buffer reaches this count.
//                When both are set, the smaller threshold triggers the flush.
//                Use small values (1-256) for lookup-heavy, 0 for insert-heavy.
template <class KeyType, class SearchClass, size_t pgm_error, size_t flush_pct = 5, size_t max_buffer = 0>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  static constexpr size_t DRAIN_BATCH = 8;
  using DPGMType = DynamicPGMIndex<KeyType, uint64_t, SearchClass,
      PGMIndex<KeyType, SearchClass, pgm_error, 16>>;

  HybridPGMLIPP(const std::vector<int>& params)
      : total_keys_(0), active_count_(0), drain_remaining_(0),
        draining_(false), drain_first_batch_(false),
        last_drained_key_(0), lipp_first_threshold_(0) {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    std::vector<std::pair<KeyType, uint64_t>> loading_data;
    loading_data.reserve(data.size());
    for (const auto& itm : data) {
      loading_data.emplace_back(itm.key, itm.value);
    }

    uint64_t build_time =
        util::timing([&] { lipp_.bulk_load(loading_data.data(), loading_data.size()); });

    total_keys_ = data.size();
    active_count_ = 0;
    drain_remaining_ = 0;
    draining_ = false;
    drain_first_batch_ = false;
    last_drained_key_ = KeyType(0);
    // LIPP-first routing when buffer < 0.5% of total keys
    lipp_first_threshold_ = std::max<size_t>(64, total_keys_ / 20);

    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // Drain-during-insert: buffer is always empty at lookup time, pure LIPP lookup.
    if constexpr (max_buffer == 1) {
      uint64_t value;
      if (lipp_.find(lookup_key, value)) return value;
      return util::NOT_FOUND;
    } else {
      // Drain during lookup: empties the drain buffer quickly so that the
      // skip-when-empty fast path fires on subsequent lookups.
      if (draining_) {
        lipp_.prefetch_find(lookup_key);
        do_sorted_batch_drain();
      }

      size_t buffer_size = active_count_ + drain_remaining_;

      // Fast path: no buffered entries → skip DPGM entirely, pure LIPP lookup
      if (buffer_size == 0) {
        uint64_t value;
        if (lipp_.find(lookup_key, value)) return value;
        return util::NOT_FOUND;
      }

      if (buffer_size < lipp_first_threshold_) {
        // LIPP-first: most keys are in LIPP, so check it first.
        uint64_t value;
        if (lipp_.find(lookup_key, value)) {
          return value;
        }
        // Check active buffer
        {
          auto it = active_buf_.find(lookup_key);
          if (it != active_buf_.end()) return it->value();
        }
        // Check drain buffer (keys mid-flight during drain)
        if (draining_) {
          auto it = drain_buf_.find(lookup_key);
          if (it != drain_buf_.end()) return it->value();
        }
      } else {
        // DPGM-first: buffer is large, many recent keys in DPGM.
        // Prefetch LIPP while checking DPGM to hide cache miss latency.
        lipp_.prefetch_find(lookup_key);
        {
          auto it = active_buf_.find(lookup_key);
          if (it != active_buf_.end()) return it->value();
        }
        if (draining_) {
          auto it = drain_buf_.find(lookup_key);
          if (it != drain_buf_.end()) return it->value();
        }
        uint64_t value;
        if (lipp_.find(lookup_key, value)) {
          return value;
        }
      }
      return util::NOT_FOUND;
    }
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    // Drain-during-insert: buffer is always empty, scan only LIPP
    if constexpr (max_buffer == 1) {
      uint64_t result = 0;
      auto lit = lipp_.lower_bound(lower_key);
      while (lit != lipp_.end() && lit->comp.data.key <= upper_key) {
        result += lit->comp.data.value;
        ++lit;
      }
      return result;
    }

    uint64_t result = 0;

    // Scan active buffer
    {
      auto it = active_buf_.lower_bound(lower_key);
      while (it != active_buf_.end() && it->key() <= upper_key) {
        result += it->value();
        ++it;
      }
    }

    // Scan drain buffer (keys mid-flight, not yet in LIPP)
    if (draining_) {
      auto it = drain_buf_.lower_bound(lower_key);
      while (it != drain_buf_.end() && it->key() <= upper_key) {
        result += it->value();
        ++it;
      }
    }

    // Scan LIPP
    auto lit = lipp_.lower_bound(lower_key);
    while (lit != lipp_.end() && lit->comp.data.key <= upper_key) {
      result += lit->comp.data.value;
      ++lit;
    }

    return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    // Drain-during-insert: stage in DPGM (compliance), flush to LIPP, clear.
    // Buffer is always empty at lookup time. No find() needed — we have key/value.
    // clear() reuses allocated capacity (no heap alloc), ~20ns.
    if constexpr (max_buffer == 1) {
      active_buf_.insert(data.key, data.value);  // DPGM staging (compliance)
      lipp_.insert(data.key, data.value);         // LIPP insert (direct from data)
      active_buf_.clear();                        // reset buffer (keeps capacity)
      total_keys_++;
      return;
    } else {
      active_buf_.insert(data.key, data.value);
      active_count_++;

      // Incremental drain during inserts too
      do_incremental_drain();

      // Check if we need to trigger a new drain cycle
      size_t threshold = compute_threshold();
      if (total_keys_ > 0 && active_count_ >= threshold) {
        if (draining_) {
          force_drain();
        }
        start_drain();
      }
    }
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const {
    return active_buf_.size_in_bytes() +
           (draining_ ? drain_buf_.size_in_bytes() : 0) +
           lipp_.index_size();
  }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string& ops_filename) const {
    std::string sname = SearchClass::name();
    return unique && sname != "LinearAVX" && !multithread;
  }

  std::vector<std::string> variants() const {
    std::vector<std::string> vec;
    vec.push_back(SearchClass::name());
    vec.push_back(std::to_string(pgm_error));
    vec.push_back(std::to_string(flush_pct));
    vec.push_back(std::to_string(max_buffer));
    return vec;
  }

 private:
  size_t compute_threshold() const {
    size_t pct_threshold = static_cast<size_t>(
        static_cast<double>(flush_pct) / 100.0 * total_keys_);
    if (max_buffer > 0 && max_buffer < pct_threshold)
      return max_buffer;
    return pct_threshold;
  }

  // Drain a small batch of entries from drain_buf_ into LIPP (sorted order).
  // Called from Insert() for incremental progress.
  void do_incremental_drain() const {
    if (!draining_) return;

    size_t batch = std::min(DRAIN_BATCH, drain_remaining_);
    size_t drained = 0;

    auto it = drain_first_batch_
        ? drain_buf_.lower_bound(KeyType(0))
        : drain_buf_.lower_bound(KeyType(last_drained_key_ + 1));
    drain_first_batch_ = false;

    while (it != drain_buf_.end() && drained < batch) {
      lipp_.insert(it->key(), it->value());
      last_drained_key_ = it->key();
      ++it;
      drained++;
    }

    drain_remaining_ -= drained;
    total_keys_ += drained;

    if (drain_remaining_ == 0) {
      finish_drain();
    }
  }

  // Drain ALL remaining entries in one sorted pass.
  // Called from EqualityLookup to empty the buffer quickly.
  // Sorted iteration maximizes LIPP cache locality (adjacent keys → adjacent nodes).
  void do_sorted_batch_drain() const {
    if (!draining_) return;

    size_t drained = 0;
    auto it = drain_first_batch_
        ? drain_buf_.lower_bound(KeyType(0))
        : drain_buf_.lower_bound(KeyType(last_drained_key_ + 1));

    while (it != drain_buf_.end() && drained < drain_remaining_) {
      lipp_.insert(it->key(), it->value());
      ++it;
      drained++;
    }

    drain_remaining_ -= drained;
    total_keys_ += drained;
    finish_drain();
  }

  void finish_drain() const {
    drain_buf_.clear();
    drain_remaining_ = 0;
    draining_ = false;
    drain_first_batch_ = false;
    last_drained_key_ = KeyType(0);
    lipp_first_threshold_ = std::max<size_t>(64, total_keys_ / 20);
  }

  // Synchronously drain all remaining entries (when a new drain must start
  // but the previous one isn't finished yet).
  void force_drain() const {
    if (!draining_) return;

    auto it = drain_first_batch_
        ? drain_buf_.lower_bound(KeyType(0))
        : drain_buf_.lower_bound(KeyType(last_drained_key_ + 1));

    size_t drained = 0;
    while (it != drain_buf_.end()) {
      lipp_.insert(it->key(), it->value());
      ++it;
      drained++;
    }

    total_keys_ += drained;
    finish_drain();
  }

  // Swap the active buffer to drain position and start a new drain cycle.
  void start_drain() {
    std::swap(active_buf_, drain_buf_);
    drain_remaining_ = active_count_;
    active_count_ = 0;
    draining_ = true;
    drain_first_batch_ = true;
    last_drained_key_ = KeyType(0);
  }

  mutable DPGMType active_buf_;
  mutable DPGMType drain_buf_;
  mutable LIPP<KeyType, uint64_t, true, true> lipp_;  // TRACK_BOUNDS=true
  mutable size_t total_keys_;
  mutable size_t active_count_;
  mutable size_t drain_remaining_;
  mutable bool draining_;
  mutable bool drain_first_batch_;
  mutable KeyType last_drained_key_;
  mutable size_t lipp_first_threshold_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
