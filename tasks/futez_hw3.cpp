#include <atomic>
#include <climits>
#include <cstdint>
#include <deque>
#include <iostream>
#include <mutex>
#include <thread>

#include <cerrno>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

class FutexConditionVariable {
public:
    void NotifyOne() {
        seq_.fetch_add(1, std::memory_order_release);
        syscall(SYS_futex, &seq_, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
    }

    void NotifyAll() {
        seq_.fetch_add(1, std::memory_order_release);
        syscall(SYS_futex, &seq_, FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0);
    }

    void Wait(std::unique_lock<std::mutex>& lock) {
        const std::uint32_t expected = seq_.load(std::memory_order_relaxed);
        lock.unlock();
        while (seq_.load(std::memory_order_acquire) == expected) {
            const int rc = syscall(SYS_futex, &seq_, FUTEX_WAIT_PRIVATE, expected, nullptr, nullptr, 0);
            if (rc == -1 && errno == EINTR) {
                continue;
            }
            if (rc == -1 && errno == EAGAIN) {
                break;
            }
            if (rc == 0) {
                break;
            }
        }
        lock.lock();
    }

    template <class Predicate>
    void Wait(std::unique_lock<std::mutex>& lock, Predicate pred) {
        while (!pred()) {
            Wait(lock);
        }
    }

private:
    std::atomic<std::uint32_t> seq_{0};
};

int main() {
    std::mutex mutex;
    std::deque<int> queue;
    FutexConditionVariable cv;
    bool done = false;
    long long sum = 0;

    std::thread producer([&]() {
        for (int i = 1; i <= 1000; ++i) {
            std::lock_guard<std::mutex> g(mutex);
            queue.push_back(i);
            cv.NotifyOne();
        }
        std::lock_guard<std::mutex> g(mutex);
        done = true;
        cv.NotifyAll();
    });

    std::thread consumer([&]() {
        std::unique_lock<std::mutex> lock(mutex);
        while (!done || !queue.empty()) {
            cv.Wait(lock, [&]() { return done || !queue.empty(); });
            while (!queue.empty()) {
                sum += queue.front();
                queue.pop_front();
            }
        }
    });

    producer.join();
    consumer.join();
    std::cout << sum << '\n';
    return 0;
}
