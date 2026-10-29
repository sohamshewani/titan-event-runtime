#include <iostream>
#include <vector>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include <thread>
#include <atomic>

// Mocking the 'titan-event-runtime' API surface for self-contained compilation and benchmarking.
// In a real environment, this would include <titan/event_runtime.hpp>
namespace titan {
    namespace runtime {
        struct Event {
            uint64_t id;
            const char* payload;
            size_t size;
        };

        class EventContext {
        public:
            void dispatch(const Event& ev) {
                // Simulate ultra-low latency event processing overhead (e.g., ring-buffer push)
                #if defined(__x86_64__) || defined(_M_X64)
                _mm_pause();
                #elif defined(__aarch64__)
                asm volatile("yield" ::: "memory");
                #endif
            }
        };
    }
}

class LatencyTracker {
public:
    explicit LatencyTracker(size_t capacity) {
        latencies_.reserve(capacity);
    }

    inline void record(uint64_t nanos) {
        latencies_.push_back(nanos);
    }

    void compute_metrics(size_t total_iterations, double elapsed_seconds) {
        if (latencies_.empty()) return;

        std::sort(latencies_.begin(), latencies_.end());

        uint64_t min_lat = latencies_.front();
        uint64_t max_lat = latencies_.back();
        
        double avg_lat = static_cast<double>(
            std::accumulate(latencies_.begin(), latencies_.end(), UINT64_C(0))
        ) / latencies_.size();

        uint64_t p50 = latencies_[static_cast<size_t>(latencies_.size() * 0.50)];
        uint64_t p90 = latencies_[static_cast<size_t>(latencies_.size() * 0.90)];
        uint64_t p99 = latencies_[static_cast<size_t>(latencies_.size() * 0.99)];
        uint64_t p999 = latencies_[static_cast<size_t>(latencies_.size() * 0.999)];

        double ops_sec = static_cast<double>(total_iterations) / elapsed_seconds;

        // Formatted Tabular Output
        std::cout << "\n";
        std::cout << "====================================================================\n";
        std::cout << "          TITAN-EVENT-RUNTIME MICROBENCHMARK RESULTS                \n";
        std::cout << "====================================================================\n";
        std::cout << std::left << std::setw(30) << "Metric" << std::right << std::setw(20) << "Value" << " | " << std::setw(12) << "Unit" << "\n";
        std::cout << "--------------------------------------------------------------------\n";
        std::cout << std::left << std::setw(30) << "Total Operations"     << std::right << std::setw(20) << total_iterations   << " | " << std::setw(12) << "ops"      << "\n";
        std::cout << std::left << std::setw(30) << "Execution Duration"   << std::right << std::setw(20) << std::fixed << std::setprecision(4) << elapsed_seconds << " | " << std::setw(12) << "seconds"  << "\n";
        std::cout << std::left << std::setw(30) << "Throughput (Ops/Sec)" << std::right << std::setw(20) << static_cast<uint64_t>(ops_sec) << " | " << std::setw(12) << "ops/sec"  << "\n";
        std::cout << "--------------------------------------------------------------------\n";
        std::cout << std::left << std::setw(30) << "Latency - Min"      << std::right << std::setw(20) << min_lat          << " | " << std::setw(12) << "nanos"    << "\n";
        std::cout << std::left << std::setw(30) << "Latency - Avg"      << std::right << std::setw(20) << static_cast<uint64_t>(avg_lat) << " | " << std::setw(12) << "nanos" << "\n";
        std::cout << std::left << std::setw(30) << "Latency - p50"      << std::right << std::setw(20) << p50              << " | " << std::setw(12) << "nanos"    << "\n";
        std::cout << std::left << std::setw(30) << "Latency - p90"      << std::right << std::setw(20) << p90              << " | " << std::setw(12) << "nanos"    << "\n";
        std::cout << std::left << std::setw(30) << "Latency - p99"      << std::right << std::setw(20) << p99              << " | " << std::setw(12) << "nanos"    << "\n";
        std::cout << std::left << std::setw(30) << "Latency - p99.9"    << std::right << std::setw(20) << p999             << " | " << std::setw(12) << "nanos"    << "\n";
        std::cout << std::left << std::setw(30) << "Latency - Max"      << std::right << std::setw(20) << max_lat          << " | " << std::setw(12) << "nanos"    << "\n";
        std::cout << "====================================================================\n";
    }

private:
    std::vector<uint64_t> latencies_;
};

int main() {
    constexpr size_t kWarmupIterations = 1'000'000;
    constexpr size_t kBenchmarkIterations = 10'000'000;

    titan::runtime::EventContext context;
    titan::runtime::Event event{1, "benchmark-payload", 17};

    // Warmup phase to stabilize instruction caches, branch predictors, and frequency scaling
    for (size_t i = 0; i < kWarmupIterations; ++i) {
        context.dispatch(event);
    }

    LatencyTracker tracker(kBenchmarkIterations);

    // Measurement phase
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < kBenchmarkIterations; ++i) {
        auto op_start = std::chrono::high_resolution_clock::now();
        
        context.dispatch(event);

        auto op_end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
        
        tracker.record(static_cast<uint64_t>(duration));
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    tracker.compute_metrics(kBenchmarkIterations, elapsed.count());

    return 0;
}