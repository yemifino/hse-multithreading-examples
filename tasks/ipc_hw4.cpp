#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

constexpr std::uint32_t kMagic = 0x49504331;
constexpr std::uint32_t kVersion = 1;
constexpr std::size_t kDefaultShmSize = 1 << 20;

struct MessageHeader {
    std::uint32_t type{};
    std::uint32_t size{};
};

struct SharedQueue {
    std::atomic<std::uint32_t> ready{0};
    std::uint32_t magic{};
    std::uint32_t version{};
    std::uint64_t capacity_bytes{};

    alignas(64) std::atomic<std::uint64_t> head{0};
    alignas(64) std::atomic<std::uint64_t> reserve_tail{0};
    alignas(64) std::atomic<std::uint64_t> publish_tail{0};
};

struct Message {
    MessageHeader header{};
    std::vector<char> payload;

    std::string AsString() const {
        return std::string(payload.data(), payload.size());
    }
};

[[noreturn]] void ThrowErrno(const char* where) {
    throw std::runtime_error(std::string(where) + ": " + std::strerror(errno));
}

std::size_t ParseSize(const char* value, const char* name) {
    try {
        return static_cast<std::size_t>(std::stoull(value));
    } catch (...) {
        throw std::runtime_error(std::string("invalid numeric argument: ") + name);
    }
}

void WriteRing(char* ring, std::size_t capacity, std::uint64_t abs_pos, const void* src, std::size_t size) {
    const std::size_t offset = static_cast<std::size_t>(abs_pos % capacity);
    const std::size_t first = std::min(size, capacity - offset);
    std::memcpy(ring + offset, src, first);
    if (size > first) {
        std::memcpy(ring, static_cast<const char*>(src) + first, size - first);
    }
}

void ReadRing(const char* ring, std::size_t capacity, std::uint64_t abs_pos, void* dst, std::size_t size) {
    const std::size_t offset = static_cast<std::size_t>(abs_pos % capacity);
    const std::size_t first = std::min(size, capacity - offset);
    std::memcpy(dst, ring + offset, first);
    if (size > first) {
        std::memcpy(static_cast<char*>(dst) + first, ring, size - first);
    }
}

void InitQueue(SharedQueue& q, std::size_t mapped_bytes) {
    const std::size_t payload_bytes = mapped_bytes - sizeof(SharedQueue);
    if (payload_bytes < sizeof(MessageHeader)) {
        throw std::runtime_error("shared memory size is too small for protocol");
    }

    q.ready.store(0, std::memory_order_relaxed);
    q.magic = kMagic;
    q.version = kVersion;
    q.capacity_bytes = static_cast<std::uint64_t>(payload_bytes);
    q.head.store(0, std::memory_order_relaxed);
    q.reserve_tail.store(0, std::memory_order_relaxed);
    q.publish_tail.store(0, std::memory_order_relaxed);

    q.ready.store(1, std::memory_order_release);
}

void CheckQueue(const SharedQueue& q, std::size_t mapped_bytes) {
    const std::size_t payload_bytes = mapped_bytes - sizeof(SharedQueue);
    if (q.magic != kMagic || q.version != kVersion || q.capacity_bytes != payload_bytes) {
        throw std::runtime_error("protocol mismatch");
    }
}

class SharedMemory {
public:
    SharedMemory(const std::string& name, bool create, std::size_t bytes)
        : bytes_(bytes) {
        if (bytes_ < sizeof(SharedQueue) + sizeof(MessageHeader)) {
            throw std::runtime_error("shared memory size is too small");
        }

        bool created = false;
        if (create) {
            fd_ = shm_open(name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0666);
            if (fd_ >= 0) {
                created = true;
                if (ftruncate(fd_, static_cast<off_t>(bytes_)) == -1) {
                    const int saved = errno;
                    close(fd_);
                    fd_ = -1;
                    shm_unlink(name.c_str());
                    errno = saved;
                    ThrowErrno("ftruncate failed");
                }
            } else if (errno == EEXIST) {
                fd_ = shm_open(name.c_str(), O_RDWR, 0666);
                if (fd_ == -1) {
                    ThrowErrno("shm_open existing failed");
                }
            } else {
                ThrowErrno("shm_open create failed");
            }
        } else {
            fd_ = shm_open(name.c_str(), O_RDWR, 0666);
            if (fd_ == -1) {
                ThrowErrno("shm_open failed");
            }
        }

        struct stat st {};
        if (fstat(fd_, &st) == -1) {
            ThrowErrno("fstat failed");
        }
        bytes_ = static_cast<std::size_t>(st.st_size);
        if (bytes_ < sizeof(SharedQueue) + sizeof(MessageHeader)) {
            throw std::runtime_error("shared memory object is too small");
        }

        void* ptr = mmap(nullptr, bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (ptr == MAP_FAILED) {
            ThrowErrno("mmap failed");
        }
        queue_ = static_cast<SharedQueue*>(ptr);

        if (created) {
            InitQueue(*queue_, bytes_);
        } else {
            while (queue_->ready.load(std::memory_order_acquire) != 1) {
                std::this_thread::yield();
            }
            CheckQueue(*queue_, bytes_);
        }
    }

    ~SharedMemory() {
        if (queue_ != nullptr) {
            munmap(queue_, bytes_);
        }
        if (fd_ != -1) {
            close(fd_);
        }
    }

    SharedMemory(const SharedMemory&) = delete;
    SharedMemory& operator=(const SharedMemory&) = delete;

    SharedQueue* Get() const {
        return queue_;
    }

private:
    int fd_{-1};
    std::size_t bytes_{};
    SharedQueue* queue_{nullptr};
};

class ProducerNode {
public:
    explicit ProducerNode(const std::string& shm_path, std::size_t shm_size)
        : shm_(shm_path, true, shm_size), q_(shm_.Get()), ring_(reinterpret_cast<char*>(q_) + sizeof(SharedQueue)) {
    }

    bool Send(std::uint32_t type, const void* data, std::uint32_t size) {
        const std::uint64_t message_bytes = sizeof(MessageHeader) + static_cast<std::uint64_t>(size);
        if (message_bytes > q_->capacity_bytes) {
            return false;
        }

        while (true) {
            const std::uint64_t tail = q_->reserve_tail.load(std::memory_order_relaxed);
            const std::uint64_t head = q_->head.load(std::memory_order_acquire);
            const std::uint64_t used = tail - head;
            const std::uint64_t free = q_->capacity_bytes - used;
            if (message_bytes > free) {
                return false;
            }

            std::uint64_t expected = tail;
            if (q_->reserve_tail.compare_exchange_weak(expected, tail + message_bytes, std::memory_order_acq_rel)) {
                const MessageHeader header{type, size};
                WriteRing(ring_, static_cast<std::size_t>(q_->capacity_bytes), tail, &header, sizeof(header));
                if (size > 0) {
                    WriteRing(ring_, static_cast<std::size_t>(q_->capacity_bytes), tail + sizeof(header), data, size);
                }

                while (q_->publish_tail.load(std::memory_order_acquire) != tail) {
                    std::this_thread::yield();
                }
                q_->publish_tail.store(tail + message_bytes, std::memory_order_release);
                return true;
            }
        }
    }

    bool SendString(std::uint32_t type, std::string_view text) {
        return Send(type, text.data(), static_cast<std::uint32_t>(text.size()));
    }

private:
    SharedMemory shm_;
    SharedQueue* q_;
    char* ring_;
};

class ConsumerNode {
public:
    explicit ConsumerNode(const std::string& shm_path, std::size_t shm_size)
        : shm_(shm_path, false, shm_size), q_(shm_.Get()), ring_(reinterpret_cast<char*>(q_) + sizeof(SharedQueue)) {
    }

    std::optional<Message> Receive(std::optional<std::uint32_t> filter = std::nullopt) {
        while (true) {
            auto message = TryPop();
            if (!message) {
                return std::nullopt;
            }
            if (!filter || message->header.type == *filter) {
                return message;
            }
        }
    }

private:
    std::optional<Message> TryPop() {
        const std::uint64_t head = q_->head.load(std::memory_order_relaxed);
        const std::uint64_t published = q_->publish_tail.load(std::memory_order_acquire);
        if (head == published) {
            return std::nullopt;
        }

        const std::uint64_t available = published - head;
        if (available < sizeof(MessageHeader)) {
            return std::nullopt;
        }

        MessageHeader header{};
        ReadRing(ring_, static_cast<std::size_t>(q_->capacity_bytes), head, &header, sizeof(header));
        const std::uint64_t message_bytes = sizeof(MessageHeader) + static_cast<std::uint64_t>(header.size);
        if (message_bytes > q_->capacity_bytes) {
            throw std::runtime_error("corrupted message size");
        }
        if (message_bytes > available) {
            return std::nullopt;
        }

        Message out;
        out.header = header;
        out.payload.resize(header.size);
        if (!out.payload.empty()) {
            ReadRing(ring_,
                     static_cast<std::size_t>(q_->capacity_bytes),
                     head + sizeof(MessageHeader),
                     out.payload.data(),
                     out.payload.size());
        }

        q_->head.store(head + message_bytes, std::memory_order_release);
        return out;
    }

private:
    SharedMemory shm_;
    SharedQueue* q_;
    const char* ring_;
};

void PrintUsage(const char* exe) {
    std::cout
        << "Usage:\n"
        << "  " << exe << " producer <shm_path> <message_type> <count> [prefix] [shm_size]\n"
        << "  " << exe << " consumer <shm_path> <expected_count> [filter_type] [shm_size]\n"
        << "  " << exe << " cleanup <shm_path>\n";
}

int RunProducer(int argc, char** argv) {
    if (argc < 5) {
        PrintUsage(argv[0]);
        return 1;
    }

    const std::string shm_path = argv[2];
    const std::uint32_t type = static_cast<std::uint32_t>(ParseSize(argv[3], "message_type"));
    const std::size_t count = ParseSize(argv[4], "count");
    const std::string prefix = (argc >= 6) ? argv[5] : "msg";
    const std::size_t shm_size = (argc >= 7) ? ParseSize(argv[6], "shm_size") : kDefaultShmSize;

    ProducerNode producer(shm_path, shm_size);
    for (std::size_t i = 0; i < count; ++i) {
        const std::string payload = prefix + "_" + std::to_string(i);
        while (!producer.SendString(type, payload)) {
            std::this_thread::yield();
        }
    }

    std::cout << "[producer] sent " << count << " messages of type " << type << '\n';
    return 0;
}

int RunConsumer(int argc, char** argv) {
    if (argc < 4) {
        PrintUsage(argv[0]);
        return 1;
    }

    const std::string shm_path = argv[2];
    const std::size_t expected_count = ParseSize(argv[3], "expected_count");
    const std::optional<std::uint32_t> filter =
        (argc >= 5) ? std::optional<std::uint32_t>(static_cast<std::uint32_t>(ParseSize(argv[4], "filter_type")))
                    : std::nullopt;
    const std::size_t shm_size = (argc >= 6) ? ParseSize(argv[5], "shm_size") : kDefaultShmSize;

    ConsumerNode consumer(shm_path, shm_size);

    std::size_t received = 0;
    while (received < expected_count) {
        auto message = consumer.Receive(filter);
        if (!message) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        ++received;
        if (received <= 5 || received == expected_count) {
            std::cout << "[consumer] #" << received
                      << " type=" << message->header.type
                      << " size=" << message->header.size
                      << " payload=" << message->AsString() << '\n';
        }
    }

    std::cout << "[consumer] done, received " << received << " message(s)\n";
    return 0;
}

}

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            PrintUsage(argv[0]);
            return 1;
        }

        const std::string mode = argv[1];
        if (mode == "producer") {
            return RunProducer(argc, argv);
        }
        if (mode == "consumer") {
            return RunConsumer(argc, argv);
        }
        if (mode == "cleanup") {
            if (argc < 3) {
                PrintUsage(argv[0]);
                return 1;
            }
            if (shm_unlink(argv[2]) == -1 && errno != ENOENT) {
                ThrowErrno("shm_unlink failed");
            }
            std::cout << "[cleanup] shared memory removed\n";
            return 0;
        }

        PrintUsage(argv[0]);
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << '\n';
        return 2;
    }
}
