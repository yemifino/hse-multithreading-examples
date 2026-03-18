#pragma once

#include <algorithm>
#include <cstddef>
#include <exception>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

template <class T>
void ApplyFunction(
    std::vector<T>& data,
    const std::function<void(T&)>& transform,
    const int threadCount = 1) {
    if (data.empty()) {
        return;
    }

    int workers = std::max(1, threadCount);
    workers = std::min<int>(workers, static_cast<int>(data.size()));

    if (workers == 1) {
        for (auto& item : data) {
            transform(item);
        }
        return;
    }

    std::exception_ptr first_exception;
    std::mutex exception_mutex;

    auto worker = [&data, &transform, &first_exception, &exception_mutex](std::size_t begin, std::size_t end) {
        try {
            for (std::size_t i = begin; i < end; ++i) {
                transform(data[i]);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(exception_mutex);
            if (first_exception == nullptr) {
                first_exception = std::current_exception();
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(workers);

    const std::size_t n = data.size();
    const std::size_t base_chunk = n / workers;
    const std::size_t remainder = n % workers;

    std::size_t begin = 0;
    for (int i = 0; i < workers; ++i) {
        const std::size_t chunk_size = base_chunk + (static_cast<std::size_t>(i) < remainder ? 1 : 0);
        const std::size_t end = begin + chunk_size;

        threads.emplace_back(worker, begin, end);

        begin = end;
    }

    for (auto& thread : threads) {
        thread.join();
    }

    if (first_exception != nullptr) {
        std::rethrow_exception(first_exception);
    }
}
