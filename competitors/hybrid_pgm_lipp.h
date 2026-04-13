#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <utility>
#include <vector>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

// Lightweight Bloom filter for skipping DPGM probes on negative lookups.
// Enabled only when max_buffer > 0 (small-buffer configs for lookup-heavy workloads).
template <size_t NumBits>
struct MiniBloom {
  static constexpr size_t NUM_WORDS = (NumBits + 63) / 64;
  uint64_t bits_[NUM_WORDS];

  MiniBloom() { clear(); }
  void clear() { std::memset(bits_, 0, sizeof(bits_)); }

  void insert(uint64_t key) {
    set_bit(key * 0x9E3779B97F4A7C15ULL);
    set_bit(key * 0x517CC1B727220A95ULL);
    set_bit(key * 0x6C62272E07BB0142ULL);
  }

  bool may_contain(uint64_t key) const {
    return check_bit(key * 0x9E3779B97F4A7C15ULL)
        && check_bit(key * 0x517CC1B727220A95ULL)
        && check_bit(key * 0x6C62272E07BB0142ULL);
  }

 private:
  void set_bit(uint64_t h) {
    size_t idx = (h >> 32) % NumBits;
    bits_[idx / 64] |= (1ULL << (idx % 64));
  }
  bool check_bit(uint64_t h) const {
    size_t idx = (h >> 32) % NumBits;
    return bits_[idx / 64] & (1ULL << (idx % 64));
  }
};

// Dummy no-op Bloom filter when disabled (max_buffer == 0).
struct NoBloom {
  void clear() {}
  void insert(uint64_t) {}
  bool may_contain(uint64_t) const { return true; }
};

// Cache-friendly sorted vector buffer for small max_buffer values.
// Binary search on contiguous L1-resident data (~4KB for 256 entries)
// is much faster than DPGM's multi-level PGM structure.
template <typename KeyType>
struct SortedVecBuffer {
  using Entry = std::pair<KeyType, uint64_t>;
  std::vector<Entry> data_;

  void insert(const KeyType& key, uint64_t value) {
    auto it = std::lower_bound(data_.begin(), data_.end(), key,
        [](const Entry& e, const KeyType& k) { return e.first < k; });
    if (it != data_.end() && it->first == key) {
      it->second = value;  // update existing
    } else {
      data_.insert(it, {key, value});
    }
  }

  // Returns pointer to value if found, nullptr otherwise.
  const uint64_t* find(const KeyType& key) const {
    auto it = std::lower_bound(data_.begin(), data_.end(), key,
        [](const Entry& e, const KeyType& k) { return e.first < k; });
    if (it != data_.end() && it->first == key) return &it->second;
    return nullptr;
  }

  // Iterate all entries (for drain into LIPP).
  const std::vector<Entry>& entries() const { return data_; }

  size_t size() const { return data_.size(); }
  size_t size_in_bytes() const { return data_.capacity() * sizeof(Entry); }
  void clear() { data_.clear(); }
};

// Advanced Hybrid PGM-LIPP index with:
//   - Double-buffered DPGM (active + drain)
//   - Bloom filter to skip DPGM probes on negative lookups
//   - LIPP-first adaptive lookup routing
//   - Incremental drain during inserts only
//   - Configurable flush threshold via template parameter flush_pct (percentage)
template <class KeyType, class SearchClass, size_t pgm_error, size_t flush_pct = 5, size_t max_buffer = 0>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  static constexpr size_t DRAIN_BATCH = 8;
  static constexpr bool use_vec_buffer = (max_buffer > 0 && max_buffer <= 4096);
  // Bloom filter: 16 bits per entry when enabled, no-op when max_buffer==0
  using BloomType = typename std::conditional<
      (max_buffer > 0), MiniBloom<max_buffer * 16>, NoBloom>::type;
  using DPGMType = DynamicPGMIndex<KeyType, uint64_t, SearchClass,
      PGMIndex<KeyType, SearchClass, pgm_error, 16>>;
  using BufferType = typename std::conditional<
      use_vec_buffer, SortedVecBuffer<KeyType>, DPGMType>::type;

  HybridPGMLIPP(const std::vector<int>& params)
      : total_keys_(0), active_count_(0), drain_remaining_(0),
        draining_(false), drain_first_batch_(false), drain_pos_(0),
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
    drain_pos_ = 0;
    last_drained_key_ = KeyType(0);
    active_bloom_.clear();
    drain_bloom_.clear();
    // LIPP-first routing when buffer < 0.5% of total keys
    lipp_first_threshold_ = std::max<size_t>(64, total_keys_ / 200);

    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    size_t buffer_size = active_count_ + drain_remaining_;

    // Fast path: no buffered entries → skip DPGM entirely, pure LIPP lookup
    if (buffer_size == 0) {
      uint64_t value;
      if (lipp_.find(lookup_key, value)) return value;
      return util::NOT_FOUND;
    }

    if (buffer_size < lipp_first_threshold_) {
      // LIPP-first: most keys are in LIPP, so check it first.
      // Wins big on positive lookups for bulk-loaded keys (one check instead of three).
      // Neutral on negative lookups (all stores checked regardless of order).
      uint64_t value;
      if (lipp_.find(lookup_key, value)) {
        return value;
      }
      // Check active buffer (skip if bloom says not present)
      if (active_bloom_.may_contain(lookup_key)) {
        if (auto v = buf_find(active_buf_, lookup_key)) return *v;
      }
      // Check drain buffer (keys mid-flight during incremental drain)
      if (draining_ && drain_bloom_.may_contain(lookup_key)) {
        if (auto v = buf_find(drain_buf_, lookup_key)) return *v;
      }
    } else {
      // DPGM-first: buffer is large (insert-heavy), many recent keys in DPGM
      if (active_bloom_.may_contain(lookup_key)) {
        if (auto v = buf_find(active_buf_, lookup_key)) return *v;
      }
      if (draining_ && drain_bloom_.may_contain(lookup_key)) {
        if (auto v = buf_find(drain_buf_, lookup_key)) return *v;
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

    // Scan active buffer
    buf_range_scan(active_buf_, lower_key, upper_key, result);

    // Scan drain buffer (keys mid-flight, not yet in LIPP)
    if (draining_) {
      buf_range_scan(drain_buf_, lower_key, upper_key, result);
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
    active_buf_.insert(data.key, data.value);
    active_bloom_.insert(data.key);
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
  // --- Buffer abstraction helpers (DPGM vs SortedVecBuffer) ---
  // These use if constexpr so only the matching branch is compiled.

  // Find a key in a buffer; returns pointer to value or nullptr.
  static const uint64_t* buf_find(const BufferType& buf, const KeyType& key) {
    if constexpr (use_vec_buffer) {
      return buf.find(key);
    } else {
      auto it = buf.find(key);
      if (it != buf.end()) {
        static thread_local uint64_t tmp;
        tmp = it->value();
        return &tmp;
      }
      return nullptr;
    }
  }

  // Range scan a buffer, accumulating values into result.
  static void buf_range_scan(const BufferType& buf, const KeyType& lo,
                             const KeyType& hi, uint64_t& result) {
    if constexpr (use_vec_buffer) {
      auto& data = buf.entries();
      auto it = std::lower_bound(data.begin(), data.end(), lo,
          [](const std::pair<KeyType, uint64_t>& e, const KeyType& k) { return e.first < k; });
      while (it != data.end() && it->first <= hi) {
        result += it->second;
        ++it;
      }
    } else {
      auto it = buf.lower_bound(lo);
      while (it != buf.end() && it->key() <= hi) {
        result += it->value();
        ++it;
      }
    }
  }

  // Reset a buffer to empty state.
  static void buf_reset(BufferType& buf) {
    if constexpr (use_vec_buffer) {
      buf.clear();
    } else {
      buf = BufferType();
    }
  }

  // Drain a batch of entries from drain_buf_ into LIPP.
  // Called only from Insert() to avoid adding latency to lookups.
  void do_incremental_drain() {
    if (!draining_) return;

    size_t batch = std::min(DRAIN_BATCH, drain_remaining_);
    size_t drained = 0;

    if constexpr (use_vec_buffer) {
      // SortedVecBuffer: iterate by drain_pos_ index
      auto& data = drain_buf_.entries();
      while (drain_pos_ < data.size() && drained < batch) {
        lipp_.insert(data[drain_pos_].first, data[drain_pos_].second);
        ++drain_pos_;
        drained++;
      }
    } else {
      // DPGM: iterate via lower_bound cursor
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
    }

    drain_remaining_ -= drained;
    total_keys_ += drained;

    // Check if drain is complete
    if (drain_remaining_ == 0) {
      buf_reset(drain_buf_);
      drain_bloom_.clear();
      drain_remaining_ = 0;
      draining_ = false;
      lipp_first_threshold_ = std::max<size_t>(64, total_keys_ / 200);
    }
  }

  // Synchronously drain all remaining entries.
  void force_drain() {
    if (!draining_) return;

    size_t drained = 0;
    if constexpr (use_vec_buffer) {
      auto& data = drain_buf_.entries();
      while (drain_pos_ < data.size()) {
        lipp_.insert(data[drain_pos_].first, data[drain_pos_].second);
        ++drain_pos_;
        drained++;
      }
    } else {
      auto it = drain_first_batch_
          ? drain_buf_.lower_bound(KeyType(0))
          : drain_buf_.lower_bound(KeyType(last_drained_key_ + 1));
      while (it != drain_buf_.end()) {
        lipp_.insert(it->key(), it->value());
        ++it;
        drained++;
      }
    }

    total_keys_ += drained;
    buf_reset(drain_buf_);
    drain_bloom_.clear();
    drain_remaining_ = 0;
    draining_ = false;
    drain_first_batch_ = false;
    drain_pos_ = 0;
    lipp_first_threshold_ = std::max<size_t>(64, total_keys_ / 200);
  }

  // Swap the active buffer to drain position and start a new drain cycle.
  void start_drain() {
    std::swap(active_buf_, drain_buf_);
    std::swap(active_bloom_, drain_bloom_);
    active_bloom_.clear();
    drain_remaining_ = active_count_;
    active_count_ = 0;
    draining_ = true;
    drain_first_batch_ = true;
    drain_pos_ = 0;
    last_drained_key_ = KeyType(0);
  }

  BufferType active_buf_;
  BufferType drain_buf_;
  BloomType active_bloom_;
  BloomType drain_bloom_;
  LIPP<KeyType, uint64_t> lipp_;
  size_t total_keys_;
  size_t active_count_;
  size_t drain_remaining_;
  bool draining_;
  bool drain_first_batch_;
  size_t drain_pos_;          // for SortedVecBuffer drain iteration
  KeyType last_drained_key_;  // for DPGM drain iteration
  size_t lipp_first_threshold_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
