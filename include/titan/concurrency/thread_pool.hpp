#ifndef TITAN_CONCURRENCY_THREAD_POOL_HPP
#define TITAN_CONCURRENCY_THREAD_POOL_HPP

#include <atomic>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace titan::concurrency {

class thread_pool {
public:
    using task_type = std::move_only_function<void() noexcept>;

    explicit thread_pool(size_t num_threads = std::thread::hardware_concurrency())
        : stop_requested_(false) {
        if (num_threads == 0) {
            num_threads = 1;
        }
        
        workers_.reserve(num_threads);
        local_queues_.resize(num_threads);

        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this, i]() {
                worker_loop(i);
            });
        }
    }

    ~thread_pool() noexcept {
        shutdown();
    }

    thread_pool(const thread_pool&) = delete;
    thread_pool& operator=(const thread_pool&) = delete;
    thread_pool(thread_pool&&) = delete;
    thread_pool& operator=(thread_pool&&) = delete;

    template <typename F>
    bool submit(F&& task) noexcept {
        try {
            task_type t(std::forward<F>(task));
            size_t idx = next_queue_index_.fetch_add(1, std::memory_order_relaxed) % local_queues_.size();
            
            {
                std::lock_guard<std::mutex> lock(local_queues_[idx].mutex);
                if (stop_requested_.load(std::memory_order_acquire)) {
                    return false;
                }
                local_queues_[idx].deque.push_back(std::move(t));
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    void shutdown() noexcept {
        bool expected = false;
        if (!stop_requested_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            return;
        }

        for (auto& w : workers_) {
            if (w.joinable()) {
                try {
                    w.join();
                } catch (...) {
                    // Suppress exceptions during critical shutdown teardown
                }
            }
        }
    }

    [[nodiscard]] size_t worker_count() const noexcept {
        return workers_.size();
    }

private:
    struct alignas(64) work_queue {
        std::mutex mutex;
        std::deque<task_type> deque;
    };

    static inline thread_local size_t tl_worker_id = 0;

    std::vector<std::thread> workers_;
    std::vector<work_queue> local_queues_;
    std::atomic<size_t> next_queue_index_{0};
    std::atomic<bool> stop_requested_;

    void worker_loop(size_t id) noexcept {
        tl_worker_id = id;
        size_t num_workers = local_queues_.size();

        while (!stop_requested_.load(std::memory_order_acquire)) {
            std::optional<task_type> task = fetch_task(id, num_workers);
            if (task.has_value()) {
                execute_task(task.value());
            } else {
                // Back off slightly to prevent busy waiting / CPU thrashing
                std::this_thread::yield();
            }
        }

        // Drain remaining tasks on thread termination
        while (true) {
            std::optional<task_type> task = fetch_task(id, num_workers);
            if (!task.has_value()) {
                break;
            }
            execute_task(task.value());
        }
    }

    [[nodiscard]] std::optional<task_type> fetch_task(size_t id, size_t num_workers) noexcept {
        // 1. Try to pop from local queue
        {
            std::lock_guard<std::mutex> lock(local_queues_[id].mutex);
            if (!local_queues_[id].deque.empty()) {
                task_type t = std::move(local_queues_[id].deque.front());
                local_queues_[id].deque.pop_front();
                return t;
            }
        }

        // 2. Work stealing phase: attempt to steal from other workers' queues
        for (size_t i = 1; i < num_workers; ++i) {
            size_t victim = (id + i) % num_workers;
            std::unique_lock<std::mutex> lock(local_queues_[victim].mutex, std::try_to_lock);
            if (lock.owns_lock() && !local_queues_[victim].deque.empty()) {
                task_type t = std::move(local_queues_[victim].deque.back());
                local_queues_[victim].deque.pop_back();
                return t;
            }
        }

        return std::nullopt;
    }

    void execute_task(task_type& task) noexcept {
        try {
            task();
        } catch (...) {
            // Enforce noexcept guarantee across all event-loop callback dispatches
        }
    }
};

} // namespace titan::concurrency

#endif // TITAN_CONCURRENCY_THREAD_POOL_HPP