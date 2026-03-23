#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

template <class T>
class UnbufferedChannel {
public:
    void Send(const T& value) {
        std::unique_lock lock(mutex_);

        send_ready_cv_.wait(lock, [this]() { return closed_ || !slot_.has_value(); });
        if (closed_) {
            throw std::runtime_error("Channel is closed");
        }

        const std::size_t my_seq = ++send_seq_;
        slot_.emplace(value);
        recv_ready_cv_.notify_one();

        send_done_cv_.wait(lock, [this, my_seq]() { return closed_ || recv_seq_ >= my_seq; });
        if (closed_ && recv_seq_ < my_seq) {
            throw std::runtime_error("Channel is closed");
        }
    }

    std::optional<T> Recv() {
        std::unique_lock lock(mutex_);
        recv_ready_cv_.wait(lock, [this]() { return closed_ || slot_.has_value(); });

        if (!slot_.has_value()) {
            return std::nullopt;
        }

        T value = std::move(*slot_);
        slot_.reset();
        recv_seq_ = send_seq_;
        send_done_cv_.notify_one();
        send_ready_cv_.notify_one();
        return value;
    }

    void Close() {
        {
            std::lock_guard lock(mutex_);
            if (closed_) {
                return;
            }
            closed_ = true;
            slot_.reset();
        }
        send_ready_cv_.notify_all();
        send_done_cv_.notify_all();
        recv_ready_cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable send_ready_cv_;
    std::condition_variable send_done_cv_;
    std::condition_variable recv_ready_cv_;
    std::optional<T> slot_;
    bool closed_ = false;
    std::size_t send_seq_ = 0;
    std::size_t recv_seq_ = 0;
};
