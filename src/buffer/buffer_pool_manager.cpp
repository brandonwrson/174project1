//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  // If there is a free frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // check if all frames are currently in use
    bool all_frames_in_use = true;
    for (auto &node : replacer_->node_store_) {
      if (node.second.is_evictable_) {
        all_frames_in_use = false;
      }
    }
    if (all_frames_in_use) {
      return nullptr;
    }
    frame_id = page_table_[*page_id];
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    page_table_.erase(pages_[frame_id].page_id_);
  }
  // Allocate a new page
  page_id_t new_page_id = AllocatePage();
  // Update the page table with the new page id
  page_table_[new_page_id] = frame_id;

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = new_page_id;
  // Pin the frame
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  // Record Access
  replacer_->RecordAccess(frame_id);
  // Return the pointer to the new page
  *page_id = new_page_id;
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  // std::unique_lock<std::mutex> lock(latch_);

  // Check if the page is in the Buffer Pool
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // Get Frame ID
    frame_id_t frame_id = it->second;

    // Pin the Frame
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }

  // Check if free frame
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

    // Update the page table
    page_table_[page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }

  // Use replacer to find replacement frame
  frame_id_t frame_id;
  if (!replacer_->Evict(&frame_id)) {
    // No evicted frame = return nullptr
    return nullptr;
  }
  frame_id = page_table_[page_id];
  // Get page id of old page
  page_id_t old_id = pages_[frame_id].GetPageId();

  // Check if the old page is dirty
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(old_id, pages_[frame_id].data_);
  }

  // Remove the entry for the old page from the page table
  page_table_.erase(old_id);
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  page_table_[page_id] = frame_id;

  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    std::cout << "not in buffer pool" << std::endl;
    return false;
  }
  int frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    std::cout << "pin count less than 0 " << std::endl;
    return false;
  }
  pages_[frame_id].pin_count_--;
  pages_[frame_id].is_dirty_ = is_dirty;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // std::unique_lock<std::mutex> lock(latch_);
  //  Find the page in page table
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frame_id = it->second;

  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (const auto &page : page_table_) {
    FlushPage(page.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // Find page in page table
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }

  // Check if the page is pinned
  auto frame_id = it->second;
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  page_table_.erase(it);
  replacer_->Remove(frame_id);
  pages_[frame_id].ResetMemory();

  // Add the frame back to the free list
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  BasicPageGuard guard;
  return guard;
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  ReadPageGuard guard(this, page);
  return guard;
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  WritePageGuard guard(this, page);
  return guard;
}

auto BufferPoolManager::NewPageGuarded(const page_id_t *page_id) -> BasicPageGuard {
  BasicPageGuard guard;
  return guard;
}

}  // namespace bustub
