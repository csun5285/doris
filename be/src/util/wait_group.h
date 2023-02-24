// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
namespace doris {

class WaitGroup {
public:

WaitGroup() = default;

~WaitGroup() = default;

WaitGroup(const WaitGroup& ) = delete;
WaitGroup(WaitGroup&&) = delete;
void operator=(const WaitGroup&) = delete;
void operator=(WaitGroup&&) = delete;

// add one counter indicating one more concurrent worker
void add(int count = 1) {
    _count += count;
}

// decrese count if one concurrent worker finished it's work
void done() {
    _count--;
    if (_count.load() <= 0) {
        _cv.notify_all();
    }
}

// wait for all concurrent workers finish their work
// would return if timeout, default timeout would be 5min
void wait(int64_t timeout_seconds = 300) {
    if (_count.load() <= 0) {
        return;
    }
    std::unique_lock<std::mutex> lck{_lock};
    _cv.wait_for(lck, std::chrono::seconds(timeout_seconds), [this](){
        return _count.load() <= 0;
    });
}


private:
    std::mutex _lock;
    std::condition_variable _cv;
    std::atomic_int64_t _count{0};
};

}
