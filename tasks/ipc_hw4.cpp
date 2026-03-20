#include <array>
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

constexpr std::uint32_t kMagic = 0x49504331;  // "IPC1"
constexpr std::uint32_t kVersion = 1;
constexpr std::size_t kCapacity = 1024;
constexpr std::size_t kMaxPayload = 256;

static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(std::atomic<std::uint32_t>::is_always_lock_free);

struct MessageHeader {
    std::uint32_t type{};
    std::uint32_t size{};
};

struct Slot {
    std::atomic<std::uint64_t> seq{};
    MessageHeader header{};
    std::array<std::byte, kMaxPayload> payload{};
};

struct SharedQueue {
    std::atomic<std::uint32_t> ready{0};
    std::uint32_t magic{};
    std::uint32_t version{};
    std::uint32_t capacity{};
    std::uint32_t max_payload{};

    alignas(64) std::atomic<std::uint64_t> head{0};
    alignas(64) std::atomic<std::uint64_t> tail{0};
    std::array<Slot, kCapacity> slots{};
};

struct Message {
    MessageHeader header{};
    std::vector<std::byte> payload;

    std::string AsString() const {
        return std::string(reinterpret_cast<const char*>(payload.data()), payload.size());
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

void InitQueue(SharedQueue& q) {
    q.ready.store(0, std::memory_order_relaxed);
    q.magic = kMagic;
    q.version = kVersion;
    q.capacity = static_cast<std::uint32_t>(kCapacity);
    q.max_payload = static_cast<std::uint32_t>(kMaxPayload);
    q.head.store(0, std::memory_order_relaxed);
    q.tail.store(0, std::memory_order_relaxed);

    for (std::size_t i = 0; i < kCapacity; ++i) {
        q.slots[i].seq.store(i, std::memory_order_relaxed);
        q.slots[i].header = {};
    }

    q.ready.store(1, std::memory_order_release);
}

void CheckQueue(const SharedQueue& q) {
    if (q.magic != kMagic ||
        q.version != kVersion ||
        q.capacity != kCapacity ||
        q.max_payload != kMaxPayload) {
        throw std::runtime_error("protocol mismatch");
    }
}

class SharedMemory {
public:
    SharedMemory(const std::string& name, bool create, std::size_t bytes)
        : bytes_(bytes) {
        if (bytes_ < sizeof(SharedQueue)) {
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
        if (bytes_ < sizeof(SharedQueue)) {
            throw std::runtime_error("shared memory object is too small");
        }

        void* ptr = mmap(nullptr, bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (ptr == MAP_FAILED) {
            ThrowErrno("mmap failed");
        }
        queue_ = static_cast<SharedQueue*>(ptr);

        if (created) {
            InitQueue(*queue_);
        } else {
            while (queue_->ready.load(std::memory_order_acquire) != 1) {
                std::this_thread::yield();
            }
            CheckQueue(*queue_);
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
        : shm_(shm_path, true, shm_size), q_(shm_.Get()) {
    }

    bool Send(std::uint32_t type, const void* data, std::uint32_t size) {
        if (size > kMaxPayload) {
            return false;
        }

        while (true) {
            const std::uint64_t pos = q_->tail.load(std::memory_order_relaxed);
            Slot& slot = q_->slots[pos % kCapacity];
            const std::uint64_t seq = slot.seq.load(std::memory_order_acquire);
            const std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);

            if (diff == 0) {
                std::uint64_t expected = pos;
                if (q_->tail.compare_exchange_weak(expected, pos + 1, std::memory_order_acq_rel)) {
                    slot.header = {type, size};
                    if (size > 0) {
                        std::memcpy(slot.payload.data(), data, size);
                    }
                    slot.seq.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                return false;  // queue full
            }
        }
    }

    bool SendString(std::uint32_t type, std::string_view text) {
        return Send(type, text.data(), static_cast<std::uint32_t>(text.size()));
    }

private:
    SharedMemory shm_;
    SharedQueue* q_;
};

class ConsumerNode {
public:
    explicit ConsumerNode(const std::string& shm_path, std::size_t shm_size)
        : shm_(shm_path, false, shm_size), q_(shm_.Get()) {
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
            // drop message with another type
        }
    }

private:
    std::optional<Message> TryPop() {
        while (true) {
            const std::uint64_t pos = q_->head.load(std::memory_order_relaxed);
            Slot& slot = q_->slots[pos % kCapacity];
            const std::uint64_t seq = slot.seq.load(std::memory_order_acquire);
            const std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);

            if (diff == 0) {
                q_->head.store(pos + 1, std::memory_order_relaxed);

                if (slot.header.size > kMaxPayload) {
                    throw std::runtime_error("corrupted message size");
                }

                Message out;
                out.header = slot.header;
                out.payload.resize(out.header.size);
                if (!out.payload.empty()) {
                    std::memcpy(out.payload.data(), slot.payload.data(), out.payload.size());
                }

                slot.seq.store(pos + kCapacity, std::memory_order_release);
                return out;
            }

            if (diff < 0) {
                return std::nullopt;  // queue empty
            }
        }
    }

private:
    SharedMemory shm_;
    SharedQueue* q_;
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
    const std::size_t shm_size = (argc >= 7) ? ParseSize(argv[6], "shm_size") : sizeof(SharedQueue);

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
    const std::size_t shm_size = (argc >= 6) ? ParseSize(argv[5], "shm_size") : sizeof(SharedQueue);

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

}  // namespace

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
