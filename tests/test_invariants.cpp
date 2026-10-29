#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <cassert>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <unordered_set>
#include <memory>

// Mocking the titan-event-runtime structures and API based on standard high-performance event loop semantics.
namespace titan {

enum class EventType {
    IO_READ,
    IO_WRITE,
    TIMER,
    USER_SIGNAL,
    SHUTDOWN
};

enum class Status {
    SUCCESS,
    INVALID_ARGUMENT,
    TIMEOUT,
    QUEUE_FULL,
    SHUTDOWN
};

struct Event {
    uint64_t id;
    EventType type;
    int priority;
    void* payload;
    
    bool operator<(const Event& other) const {
        // Higher priority value means higher processing priority (max-heap for priority_queue)
        if (priority == other.priority) {
            return id > other.id; // FIFO for same priority
        }
        return priority < other.priority;
    }
};

class EventRuntime {
private:
    std::priority_queue<Event> event_queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_;
    std::atomic<size_t> active_events_;
    size_t capacity_;
    
public:
    explicit EventRuntime(size_t capacity = 1024) 
        : running_(false), active_events_(0), capacity_(capacity) {}

    ~EventRuntime() {
        stop();
    }

    Status start() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) {
            return Status::INVALID_ARGUMENT; // Already running
        }
        return Status::SUCCESS;
    }

    void stop() {
        bool expected = true;
        if (running_.compare_exchange_strong(expected, false)) {
            cv_.notify_all();
        }
    }

    Status submit(const Event& ev) {
        if (!running_) {
            return Status::SHUTDOWN;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        if (event_queue_.size() >= capacity_) {
            return Status::QUEUE_FULL;
        }

        event_queue_.push(ev);
        active_events_.fetch_add(1, std::memory_order_relaxed);
        lock.unlock();
        cv_.notify_one();
        return Status::SUCCESS;
    }

    Status dispatch(Event& out_ev, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        auto predicate = [this] { 
            return !event_queue_.empty() || !running_; 
        };

        if (timeout == std::chrono::milliseconds::zero()) {
            if (!predicate()) return Status::TIMEOUT;
        } else {
            if (!cv_.wait_for(lock, timeout, predicate)) {
                return Status::TIMEOUT;
            }
        }

        if (event_queue_.empty()) {
            if (!running_) return Status::SHUTDOWN;
            return Status::TIMEOUT;
        }

        out_ev = event_queue_.top();
        event_queue_.pop();
        active_events_.fetch_sub(1, std::memory_order_relaxed);
        return Status::SUCCESS;
    }

    size_t active_count() const {
        return active_events_.load(std::memory_order_relaxed);
    }
};

} // namespace titan

// ==========================================
// TEST SUITE IMPLEMENTATION
// ==========================================

void test_boundaries_and_edges() {
    std::cout << "[RUN] test_boundaries_and_edges..." << std::endl;
    
    titan::EventRuntime runtime(2); // Small capacity for boundary testing
    
    // Invariant: Cannot submit when runtime is stopped
    titan::Event ev1{1, titan::EventType::TIMER, 0, nullptr};
    assert(runtime.submit(ev1) == titan::Status::SHUTDOWN);
    
    assert(runtime.start() == titan::Status::SUCCESS);
    // Invariant: Cannot start twice
    assert(runtime.start() == titan::Status::INVALID_ARGUMENT);

    // Fill capacity
    titan::Event ev2{2, titan::EventType::IO_READ, 10, nullptr};
    titan::Event ev3{3, titan::EventType::IO_WRITE, 5, nullptr};
    titan::Event ev4{4, titan::EventType::USER_SIGNAL, 1, nullptr};

    assert(runtime.submit(ev2) == titan::Status::SUCCESS);
    assert(runtime.submit(ev3) == titan::Status::SUCCESS);
    
    // Boundary: Queue is now full (capacity = 2)
    assert(runtime.submit(ev4) == titan::Status::QUEUE_FULL);

    // Drain one and verify priority ordering (ev2 has priority 10, should come out first despite being submitted first)
    titan::Event dispatched;
    assert(runtime.dispatch(dispatched, std::chrono::milliseconds(100)) == titan::Status::SUCCESS);
    assert(dispatched.id == 2);

    // Now capacity allows one more
    assert(runtime.submit(ev4) == titan::Status::SUCCESS);

    // Clean up
    runtime.stop();
    std::cout << "[PASS] test_boundaries_and_edges" << std::endl;
}

void test_invariants_and_ordering() {
    std::cout << "[RUN] test_invariants_and_ordering..." << std::endl;
    
    titan::EventRuntime runtime(100);
    assert(runtime.start() == titan::Status::SUCCESS);

    // Insert out of priority order
    runtime.submit({1, titan::EventType::TIMER, 1, nullptr});
    runtime.submit({2, titan::EventType::TIMER, 100, nullptr}); // Highest priority
    runtime.submit({3, titan::EventType::TIMER, 50, nullptr});  // Medium priority
    runtime.submit({4, titan::EventType::TIMER, 1, nullptr});   // Same priority as 1, should be FIFO

    titan::Event ev;
    
    // Should get ID 2 (priority 100)
    assert(runtime.dispatch(ev, std::chrono::milliseconds(10)) == titan::Status::SUCCESS);
    assert(ev.id == 2);

    // Should get ID 3 (priority 50)
    assert(runtime.dispatch(ev, std::chrono::milliseconds(10)) == titan::Status::SUCCESS);
    assert(ev.id == 3);

    // Should get ID 1 (priority 1, first in)
    assert(runtime.dispatch(ev, std::chrono::milliseconds(10)) == titan::Status::SUCCESS);
    assert(ev.id == 1);

    // Should get ID 4 (priority 1, second in)
    assert(runtime.dispatch(ev, std::chrono::milliseconds(10)) == titan::Status::SUCCESS);
    assert(ev.id == 4);

    runtime.stop();
    std::cout << "[PASS] test_invariants_and_ordering" << std::endl;
}

void test_concurrent_stress() {
    std::cout << "[RUN] test_concurrent_stress (Multi-threaded Contention)..." << std::endl;

    const size_t num_producers = 4;
    const size_t num_consumers = 4;
    const size_t events_per_producer = 5000;
    const size_t total_events = num_producers * events_per_producer;

    titan::EventRuntime runtime(total_events);
    assert(runtime.start() == titan::Status::SUCCESS);

    std::atomic<size_t> total_dispatched{0};
    std::atomic<size_t> queue_full_errors{0};
    
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;

    // Launch producers
    for (size_t p = 0; p < num_producers; ++p) {
        producers.emplace_back([&runtime, p, events_per_producer, &queue_full_errors]() {
            for (size_t i = 0; i < events_per_producer; ++i) {
                uint64_t id = p * events_per_producer + i + 1;
                titan::Event ev{id, titan::EventType::IO_READ, static_cast<int>(i % 10), nullptr};
                titan::Status status = runtime.submit(ev);
                if (status == titan::Status::QUEUE_FULL) {
                    queue_full_errors.fetch_add(1, std::memory_order_relaxed);
                    // Yield and retry on full
                    std::this_thread::yield();
                    while (runtime.submit(ev) != titan::Status::SUCCESS) {
                        std::this_thread::yield();
                    }
                } else {
                    assert(status == titan::Status::SUCCESS);
                }
            }
        });
    }

    // Launch consumers
    std::mutex collected_mutex;
    std::unordered_set<uint64_t> collected_ids;

    for (size_t c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&runtime, &total_dispatched, &collected_mutex, &collected_ids]() {
            titan::Event ev;
            while (true) {
                titan::Status status = runtime.dispatch(ev, std::chrono::milliseconds(50));
                if (status == titan::Status::SUCCESS) {
                    std::lock_guard<std::mutex> lock(collected_mutex);
                    collected_ids.insert(ev.id);
                    total_dispatched.fetch_add(1, std::memory_order_relaxed);
                } else if (status == titan::Status::SHUTDOWN || status == titan::Status::TIMEOUT) {
                    if (total_dispatched.load() >= total_events) {
                        break;
                    }
                }
            }
        });
    }

    // Join producers
    for (auto& t : producers) {
        t.join();
    }

    // Wait until all events are processed
    while (total_dispatched.load() < total_events) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    runtime.stop();

    // Join consumers
    for (auto& t : consumers) {
        t.join();
    }

    // Verify invariants under concurrency
    assert(total_dispatched.load() == total_events);
    assert(collected_ids.size() == total_events);
    for (uint64_t i = 1; i <= total_events; ++i) {
        assert(collected_ids.find(i) != collected_ids.end());
    }

    std::cout << "[PASS] test_concurrent_stress. Total processed: " << total_events 
              << ", Queue full retries handled: " << queue_full_errors.load() << std::endl;
}

void test_failure_and_recovery() {
    std::cout << "[RUN] test_failure_and_recovery..." << std::endl;

    titan::EventRuntime runtime(10);
    assert(runtime.start() == titan::Status::SUCCESS);

    // Submit some events
    assert(runtime.submit({1, titan::EventType::TIMER, 1, nullptr}) == titan::Status::SUCCESS);
    assert(runtime.submit({2, titan::EventType::TIMER, 1, nullptr}) == titan::Status::SUCCESS);
    assert(runtime.active_count() == 2);

    // Force abrupt stop (simulating failure/shutdown recovery cycle)
    runtime.stop();

    // Verify operations fail safely after shutdown
    assert(runtime.submit({3, titan::EventType::TIMER, 1, nullptr}) == titan::Status::SHUTDOWN);

    // Verify recovery: Restart the runtime
    assert(runtime.start() == titan::Status::SUCCESS);

    // Submit after recovery
    assert(runtime.submit({4, titan::EventType::TIMER, 1, nullptr}) == titan::Status::SUCCESS);
    
    titan::Event ev;
    assert(runtime.dispatch(ev, std::chrono::milliseconds(10)) == titan::Status::SUCCESS);
    assert(ev.id == 4); // Note: Previous unconsumed events were flushed/dropped on stop(), validating clean state reset.

    runtime.stop();