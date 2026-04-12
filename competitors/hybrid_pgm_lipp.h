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
//   - LIPP-first adaptive lookup routing
//   - Incremental drain during inserts only
//   - Configurable flush threshold via template parameter flush_pct (percentage)
template <class KeyType, class SearchClass, size_t pgm_error, size_t flush_pct = 5, size_t max_buffer = 0>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  static constexpr size_t DRAIN_BATCH = 8;

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
    lipp_first_threshold_ = std::max<size_t>(64, total_keys_ / 200);

    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    size_t buffer_size = active_count_ + drain_remaining_;

    if (buffer_size < lipp_first_threshold_) {
      // LIPP-first: most keys are in LIPP, so check it first.
      // Wins big on positive lookups for bulk-loaded keys (one check instead of three).
      // Neutral on negative lookups (all stores checked regardless of order).
      uint64_t value;
      if (lipp_.find(lookup_key, value)) {
        return value;
      }
      // Check active DPGM
      auto it = active_dpgm_.find(lookup_key);
      if (it != active_dpgm_.end()) {
        return it->value();
      }
      // Check drain DPGM (keys mid-flight during incremental drain)
      if (draining_) {
        auto dit = drain_dpgm_.find(lookup_key);
        if (dit != drain_dpgm_.end()) {
          return dit->value();
        }
      }
    } else {
      // DPGM-first: buffer is large (insert-heavy), many recent keys in DPGM
      auto it = active_dpgm_.find(lookup_key);
      if (it != active_dpgm_.end()) {
        return it->value();
      }
      if (draining_) {
        auto dit = drain_dpgm_.find(lookup_key);
        if (dit != drain_dpgm_.end()) {
          return dit->value();
        }
      }
      uint64_t value;
      if (lipp_.find(lookup_key, value)) {
        return value;
      }
    }
    return util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    uint64_t result = 0;

    // Scan active DPGM
    auto it = active_dpgm_.lower_bound(lower_key);
    while (it != active_dpgm_.end() && it->key() <= upper_key) {
      result += it->value();
      ++it;
    }

    // Scan drain DPGM (keys mid-flight, not yet in LIPP)
    if (draining_) {
      auto dit = drain_dpgm_.lower_bound(lower_key);
      while (dit != drain_dpgm_.end() && dit->key() <= upper_key) {
        result += dit->value();
        ++dit;
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
    active_dpgm_.insert(data.key, data.value);
    active_count_++;

    // Do incremental drain work (only during inserts to keep lookup path clean)
    do_incremental_drain();

    // Check if we need to trigger a new drain cycle
    size_t pct_threshold = static_cast<size_t>(
        static_cast<double>(flush_pct) / 100.0 * total_keys_);
    size_t threshold = (max_buffer > 0 && max_buffer < pct_threshold)
        ? max_buffer : pct_threshold;
    if (total_keys_ > 0 && active_count_ >= threshold) {
      if (draining_) {
        // Previous drain not complete — force-drain remaining entries synchronously
        force_drain();
      }
      start_drain();
    }
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const {
    return active_dpgm_.size_in_bytes() +
           (draining_ ? drain_dpgm_.size_in_bytes() : 0) +
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
  // Drain a batch of entries from drain_dpgm_ into LIPP.
  // Called only from Insert() to avoid adding latency to lookups.
  void do_incremental_drain() {
    if (!draining_) return;

    // Resume iteration: first batch starts from beginning, subsequent batches
    // resume after last_drained_key_.
    auto it = drain_first_batch_
        ? drain_dpgm_.lower_bound(KeyType(0))
        : drain_dpgm_.lower_bound(KeyType(last_drained_key_ + 1));
    drain_first_batch_ = false;

    size_t batch = std::min(DRAIN_BATCH, drain_remaining_);
    size_t drained = 0;

    while (it != drain_dpgm_.end() && drained < batch) {
      // Insert into LIPP first. The key remains in drain_dpgm_ (no deletion
      // mid-drain) so lookups can find it in either place during the transition.
      lipp_.insert(it->key(), it->value());
      last_drained_key_ = it->key();
      ++it;
      drained++;
    }

    drain_remaining_ -= drained;
    total_keys_ += drained;

    // Check if drain is complete
    if (drain_remaining_ == 0 || it == drain_dpgm_.end()) {
      // Reset drain DPGM entirely (no per-entry deletion)
      drain_dpgm_ = DPGMType();
      drain_remaining_ = 0;
      draining_ = false;
      // Update LIPP-first threshold based on new total
      lipp_first_threshold_ = std::max<size_t>(64, total_keys_ / 200);
    }
  }

  // Synchronously drain all remaining entries. Used as fallback when a new
  // swap is needed before the previous drain completed.
  void force_drain() {
    if (!draining_) return;

    auto it = drain_first_batch_
        ? drain_dpgm_.lower_bound(KeyType(0))
        : drain_dpgm_.lower_bound(KeyType(last_drained_key_ + 1));

    size_t drained = 0;
    while (it != drain_dpgm_.end()) {
      lipp_.insert(it->key(), it->value());
      ++it;
      drained++;
    }

    total_keys_ += drained;
    drain_dpgm_ = DPGMType();
    drain_remaining_ = 0;
    draining_ = false;
    drain_first_batch_ = false;
    lipp_first_threshold_ = std::max<size_t>(64, total_keys_ / 200);
  }

  // Swap the active DPGM to drain position and start a new drain cycle.
  void start_drain() {
    std::swap(active_dpgm_, drain_dpgm_);
    drain_remaining_ = active_count_;
    active_count_ = 0;
    draining_ = true;
    drain_first_batch_ = true;
    last_drained_key_ = KeyType(0);
  }

  using DPGMType = DynamicPGMIndex<KeyType, uint64_t, SearchClass,
      PGMIndex<KeyType, SearchClass, pgm_error, 16>>;

  DPGMType active_dpgm_;
  DPGMType drain_dpgm_;
  LIPP<KeyType, uint64_t> lipp_;
  size_t total_keys_;
  size_t active_count_;
  size_t drain_remaining_;
  bool draining_;
  bool drain_first_batch_;
  KeyType last_drained_key_;
  size_t lipp_first_threshold_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
