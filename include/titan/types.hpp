#ifndef TITAN_TYPES_HPP
#define TITAN_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <chrono>
#include <type_traits>
#include <new>
#include <system_error>

namespace titan {

    using TickType = uint64_t;
    using SocketFd = int;

    enum class EventType : uint32_t {
        Read = 1 << 0,
        Write = 1 << 1,
        Error = 1 << 2,
        HangUp = 1 << 3
    };

    inline constexpr EventType operator|(EventType a, EventType b) noexcept {
        return static_cast<EventType>(static_cast<uint32_t>(a) | static_cast<uint32_t>(b));
    }

    inline constexpr EventType operator&(EventType a, EventType b) noexcept {
        return static_cast<EventType>(static_cast<uint32_t>(a) & static_cast<uint32_t>(b));
    }

    struct alignas(64) TimerNode {
        TimerNode* next;
        TimerNode* prev;
        TickType expire_tick;
        uint64_t interval_ms;
        void* callback_context;
        void (*callback)(void*);
        bool is_recurring;
        std::atomic<bool> cancelled;

        constexpr TimerNode() noexcept
            : next(nullptr), prev(nullptr), expire_tick(0), interval_ms(0),
              callback_context(nullptr), callback(nullptr), is_recurring(false),
              cancelled(false) {}
    };

    struct alignas(64) TimerWheelConfig {
        static constexpr size_t kWheelLevels = 4;
        static constexpr size_t kBitsPerLevel = 8;
        static constexpr size_t kSlotsPerLevel = 1 << kBitsPerLevel;
        static constexpr size_t kSlotMask = kSlotsPerLevel - 1;
        static constexpr uint64_t kTickResolutionMs = 10;
    };

    struct alignas(64) IoEvent {
        IoEvent* next;
        IoEvent* prev;
        SocketFd fd;
        EventType events;
        EventType revents;
        void* user_data;
        std::atomic<uint32_t> ref_count;

        constexpr IoEvent() noexcept
            : next(nullptr), prev(nullptr), fd(-1), events(EventType::Read),
              revents(EventType::StaticCast ? static_cast<EventType>(0) : static_cast<EventType>(0)),
              user_data(nullptr), ref_count(1) {}
    };

    struct alignas(64) ConnectionSlot {
        std::atomic<uint64_t> sequence;
        SocketFd fd;
        uint32_t worker_id;
        std::atomic<bool> active;

        constexpr ConnectionSlot() noexcept
            : sequence(0), fd(-1), worker_id(0), active(false) {}
    };

    template <typename T, size_t Capacity>
    class MpscRingBuffer {
        static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
    private:
        struct Node {
            std::atomic<size_t> sequence;
            T data;
        };

        Node buffer_[Capacity];
        char pad1_[64];
        std::atomic<size_t> enqueue_pos_;
        char pad2_[64];
        size_t dequeue_pos_;
        char pad3_[64];

    public:
        MpscRingBuffer() noexcept : enqueue_pos_(0), dequeue_pos_(0) {
            for (size_t i = 0; i < Capacity; ++i) {
                buffer_[i].sequence.store(i, std::memory_order_relaxed);
            }
        }

        ~MpscRingBuffer() noexcept = default;

        MpscRingBuffer(const MpscRingBuffer&) = delete;
        MpscRingBuffer& operator=(const MpscRingBuffer&) = delete;
        MpscRingBuffer(MpscRingBuffer&&) = delete;
        MpscRingBuffer& operator=(MpscRingBuffer&&) = delete;

        bool push(const T& item) noexcept {
            Node* node;
            size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
            for (;;) {
                node = &buffer_[pos & (Capacity - 1)];
                size_t seq = node->sequence.load(std::memory_order_acquire);
                intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
                if (dif == 0) {
                    if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    }
                } else if (dif < 0) {
                    return false;
                } else {
                    pos = enqueue_pos_.load(std::memory_order_relaxed);
                }
            }
            node->data = item;
            node->sequence.store(pos + 1, std::memory_order_release);
            return true;
        }

        bool pop(T& item) noexcept {
            Node* node = &buffer_[dequeue_pos_ & (Capacity - 1)];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(dequeue_pos_ + 1);
            if (dif == 0) {
                item = node->data;
                node->sequence.store(dequeue_pos_ + Capacity, std::memory_order_release);
                ++dequeue_pos_;
                return true;
            }
            return false;
        }
    };

    class SystemErrorCategory : public std::error_category {
    public:
        const char* name() const noexcept override {
            return "titan_runtime";
        }

        std::string message(int ev) const override {
            switch (ev) {
                case 1: return "Emfile connection limit circuit breaker triggered";
                case 2: return "Timer wheel allocation failure";
                case 3: return "Connection pool exhausted";
                case 4: return "Poller registration fault";
                default: return "Unknown titan runtime error";
            }
        }
    };

    inline const std::error_category& titan_category() noexcept {
        static SystemErrorCategory instance;
        return instance;
    }

    inline std::error_code make_error_code(int err) noexcept {
        return std::error_code(err, titan_category());
    }

} // namespace titan

#endif // TITAN_TYPES_HPP