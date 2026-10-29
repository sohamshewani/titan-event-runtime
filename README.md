# titan-event-runtime
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/titan-event-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/titan-event-runtime/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/titan-event-runtime) [![Stars](https://img.shields.io/github/stars/sohamshewani/titan-event-runtime?style=flat)](https://github.com/sohamshewani/titan-event-runtime)

## Executive Summary & Problem Space

`titan-event-runtime` is an enterprise-grade, high-throughput, low-latency C++ event-driven runtime engine engineered for deterministic I/O multiplexing and microsecond-scale task scheduling. Modern high-concurrency systems frequently suffer from performance degradation caused by dynamic heap fragmentation, lock contention on shared resource pools, and system call overhead under heavy multi-core load. 

`titan-event-runtime` resolves these fundamental bottlenecks by integrating:
- **Arena-Based Bump Allocation**: Eliminates runtime heap allocation overhead and prevents memory fragmentation during high-frequency event cycles.
- **Lock-Free Work-Stealing Thread Pools**: Minimizes thread synchronization overhead and optimizes core utilization across NUMA boundaries.
- **Hierarchical Timing Wheels**: O(1) amortized temporal task scheduling for millions of concurrent timers without red-black tree traversal overhead.
- **Zero-Copy Edge-Triggered Polling**: Direct integration with high-performance kernel event multiplexers (`epoll`/`kqueue`) to maximize throughput and minimize context switching.

---

## Structural ASCII Architecture / Dataflow Diagram

The runtime decouples network ingestion, event polling, temporal scheduling, and worker execution into distinct asynchronous layers:

```
 +-----------------------------------------------------------------------+
 |                        Client / Network Layer                         |
 +-----------------------------------------------------------------------+
                                     | (TCP / Unix Sockets)
                                     v
 +-----------------------------------------------------------------------+
 |                     Connection Pool (`connection_pool.hpp`)           |
 |  - Pre-allocated FD Slots  - O(1) Index Lookup  - State Tracking      |
 +-----------------------------------------------------------------------+
                                     |
                                     v
 +-----------------------------------------------------------------------+
 |                        Poller (`poller.hpp`)                          |
 |  - Edge-Triggered epoll/kqueue multiplexing                           |
 |  - Zero-copy event extraction                                         |
 +-----------------------------------------------------------------------+
            | (Read/Write Ready)                   | (Timeout Expiry)
            v                                      v
 +------------------------------------+  +-------------------------------+
 | Thread Pool (`thread_pool.hpp`)    |  | Timer Wheel (`timer_wheel.hpp`)|
 |  - Lock-free work-stealing queues  |  |  - O(1) Hierarchical buckets  |
 |  - Worker thread affinity          |  |  - Deterministic expiry ticks |
 +------------------------------------+  +-------------------------------+
            |                                      |
            +------------------+-------------------+
                               |
                               v
 +-----------------------------------------------------------------------+
 |                  Memory Arena (`arena.hpp`)                           |
 |  - Contiguous bump-pointer allocations                                |
 |  - Block-based chunking with zero fragmentation                       |
 +-----------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
- **Work-Stealing Architecture**: Worker threads within `ThreadPool` maintain local task deques. Threads consume tasks lock-free from their local head and selectively steal from the tail of peer deques using acquire-release atomic semantics, eliminating global mutex contention.
- **Connection Isolation**: Each network descriptor maps directly to isolated buffers managed via the `ConnectionPool`, ensuring that concurrent I/O operations on distinct file descriptors execute without synchronization barriers.

### Memory Ownership & Invariants
- **Arena Allocations (`Arena`)**: Memory handed out by `include/titan/memory/arena.hpp` adheres to strict monotonic growth invariants. Individual objects allocated within an arena cannot be individually freed; instead, the entire arena scope is reclaimed simultaneously, guaranteeing zero allocation latency ($O(1)$) and preventing memory leaks or use-after-free vectors.
- **Lifetime Safety**: Immutable type definitions in `include/titan/types.hpp` enforce strict const-correctness and pass-by-reference-to-const patterns across cross-subsystem boundaries.

---

## Performance Characteristics & Algorithmic Complexity Table

| Component / Operation | Time Complexity | Space Complexity | Latency Estimate (P99) |
| :--- | :--- | :--- | :--- |
| **Arena Allocation** | $O(1)$ | $O(N)$ total block size | $< 5\text{ ns}$ |
| **Poller Event Dispatch** | $O(E)$ ($E$ = ready events) | $O(F)$ ($F$ = tracked file descriptors) | $< 2\mu\text{s}$ |
| **Timer Wheel Insertion** | $O(1)$ | $O(T)$ ($T$ = active timers) | $< 15\text{ ns}$ |
| **Timer Wheel Tick** | $O(K)$ ($K$ = expired timers in tick) | $O(1)$ auxiliary | $< 50\text{ ns}$ |
| **Thread Pool Task Push/Pop** | $O(1)$ amortized | $O(M)$ queued tasks | $< 25\text{ ns}$ |
| **Connection Pool Lookup** | $O(1)$ | $O(C)$ max connections | $< 10\text{ ns}$ |

---

## Security Architecture & Threat Model

`titan-event-runtime` is engineered to operate in hostile, untrusted network perimeters under the following rigorous security invariants:

1. **Memory Corruption Protections**: 
   - Strict ban on raw pointer arithmetic outside of low-level memory arena primitives.
   - Comprehensive bounds checking and type-safe casting within core data structures (`include/titan/types.hpp`).
2. **Denial of Service (DoS) Mitigation**:
   - **Resource Bounding**: The `ConnectionPool` enforces hard upper limits on concurrent connections, preventing exhaustion of kernel file descriptor tables and memory blocks.
   - **Predictable Latency Bounds**: $O(1)$ timer wheels and bounded task queues prevent algorithmic complexity attacks (e.g., hash-collision or timer-flooding vectors).
3. **Input Sanitization & Boundary Enforcement**:
   - All external socket buffers parsed by the runtime are explicitly length-checked against pre-allocated arena chunk boundaries to prevent stack and heap buffer overflows.
4. **Static & Dynamic Analysis**:
   - The codebase is continuously validated under AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and ThreadSanitizer (TSan) in the CI pipeline.

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
- A modern C++20 compliant compiler (GCC 11+ or Clang 13+)
- GNU Make 4.0+
- POSIX-compliant operating system (Linux kernel 5.4+ recommended for optimal `epoll` performance)

### 1. Build the Runtime & Test Suite
Compile all production artifacts, test suites, and benchmarks using the root `Makefile`:
```bash
make all
```

### 2. Run Invariant & Concurrency Tests
Execute the comprehensive test harness to verify memory invariants, thread synchronization correctness, and edge-case resilience:
```bash
make test
```
*Alternatively, run the compiled binary directly:*
```bash
./tests/test_invariants
```

### 3. Execute Performance Benchmarks
Run the high-throughput engine microbenchmarks to measure arena allocation velocity, timer wheel tick overhead, and thread pool dispatch latencies:
```bash
make bench
```
*Direct execution:*
```bash
./benchmarks/bench_engine
```

### 4. Clean Build Artifacts
Remove all intermediate object files, test binaries, and benchmark executables:
```bash
make clean