//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <sys/time.h>
#include <unistd.h>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <map>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  this->k_ = k;
  std::cout << "Init of k: " << k_ << std::endl;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  std::cout << "Size of k: " << k_ << std::endl;
  time_t current_time = time(nullptr);
  auto time = static_cast<int64_t>(current_time);
  // struct timeval time_now{};
  // gettimeofday(&time_now, nullptr);
  // time_t msecs_time = (time_now.tv_sec * 1000) + (time_now.tv_usec / 1000);
  // int64_t time = static_cast<int64_t>(msecs_time);

  int64_t maximum_kdist = -1;
  frame_id_t max_frame = 0;
  frame_id_t max_frame_lru = 0;
  int inf_k_dist_count = 0;

  int64_t min_time = 9223372036854775807;

  // cout << "Attempting to evict entry" << endl;
  for (auto &entry : node_store_) {
    // cout << "looking at entry: " << entry.first << " with evist status: " << entry.second.is_evictable_<< endl;
    // cout << "Elemnts in the history: ";
    // for(auto  &num: node_store_[entry.first].history_){
    //   cout<<num<<", ";
    // }
    // cout<<endl;
    if (entry.second.is_evictable_) {
      int64_t kdist = node_store_[entry.first].BackwardKDist(time, k_);
      // cout << "kdist for frame " << entry.first << " has kdist " << kdist << endl;
      if (kdist > maximum_kdist) {
        // cout<<"setting new Kdist "<< entry.first<<endl;ç
        maximum_kdist = kdist;
        max_frame = entry.first;
      }

      // check if K distis not
      if (kdist == 9223372036854775807) {
        inf_k_dist_count++;
      }
      // get the frame with least recent acsess time
      auto frame_most_recent_acsess_time = node_store_[entry.first].GetMostRecentTime();
      if (frame_most_recent_acsess_time < min_time) {
        min_time = frame_most_recent_acsess_time;
        max_frame_lru = entry.first;
      }
    }
  }
  if (max_frame_lru == 0 && max_frame == 0) {
    return false;
  }
  // multiple frames with inf K dist
  if (inf_k_dist_count >= 2) {
    *frame_id = max_frame_lru;
    node_store_.erase(*frame_id);
    curr_size_--;
    return true;
  }

  *frame_id = max_frame;
  node_store_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  time_t current_time = time(nullptr);
  auto time = static_cast<int64_t>(current_time);
  sleep(1);

  // frame doesn't exist yet
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode new_node;
    new_node.k_ = k_;
    new_node.history_.push_back(time);
    node_store_[frame_id] = new_node;
    curr_size_++;
  } else {
    node_store_[frame_id].history_.push_back(time);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // Decrement size if not evictable? According to test file
  if (!set_evictable && node_store_[frame_id].is_evictable_) {
    curr_size_--;
  }
  if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    curr_size_++;
  }
  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (node_store_[frame_id].is_evictable_) {
    curr_size_--;
  }
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
