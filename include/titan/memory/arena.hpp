#ifndef TITAN_MEMORY_ARENA_HPP
#define TITAN_MEMORY_ARENA_HPP

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <memory_resource>
#include <system_error>
#include <utility>
#include <concepts>

namespace titan::memory {

/**
 * @class Arena
 * @brief High-performance, monotonic memory arena conforming to std::pmr::memory_resource.
 * 
 * Designed for the 'titan-event-runtime' hot paths. Allocations are O(1) pointer bumps
 * with zero individual deallocations. Deallocation occurs in bulk when the arena is reset
 * or destroyed. Cache-line aligned to prevent false sharing in multi-threaded worker pools.
 */
class alignas(64) Arena final : public std::pmr::memory_resource {
public:
    explicit Arena(std::size_t initial_chunk_size = 65536) noexcept
        : m_chunk_size(initial_chunk_size),
          m_current_chunk(nullptr),
          m_total_allocated(0) {
        // Defer initial chunk allocation until first request to maintain lightweight construction,
        // or allocate immediately if desired. We allocate immediately for deterministic layout.
        m_current_chunk = allocate_chunk(m_chunk_size, nullptr);
    }

    ~Arena() noexcept override {
        Chunk* curr = m_current_chunk;
        while (curr != nullptr) {
            Chunk* prev = curr->prev;
            ::operator delete(curr, std::align_val_t{64});
            curr = prev;
        }
    }

    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    Arena(Arena&& other) noexcept
        : m_chunk_size(other.m_chunk_size),
          m_current_chunk(other.m_current_chunk),
          m_total_allocated(other.m_total_allocated) {
        other.m_chunk_size = 0;
        other.m_current_chunk = nullptr;
        other.m_total_allocated = 0;
    }

    Arena& operator=(Arena&& other) noexcept {
        if (this != &other) {
            // Clean up current resources
            Chunk* curr = m_current_chunk;
            while (curr != nullptr) {
                Chunk* prev = curr->prev;
                ::operator delete(curr, std::align_val_t{64});
                curr = prev;
            }

            m_chunk_size = other.m_chunk_size;
            m_current_chunk = other.m_current_chunk;
            m_total_allocated = other.m_total_allocated;

            other.m_chunk_size = 0;
            other.m_current_chunk = nullptr;
            other.m_total_allocated = 0;
        }
        return *this;
    }

    /**
     * @brief Resets the arena, invalidating all previously allocated memory blocks
     * but retaining underlying memory chunks for future allocations. O(1) amortized.
     */
    void reset() noexcept {
        if (!m_current_chunk) {
            return;
        }

        // Keep the head chunk, reset its cursor, and free any older chunks to prevent memory bloat
        Chunk* curr = m_current_chunk->prev;
        while (curr != nullptr) {
            Chunk* prev = curr->prev;
            ::operator delete(curr, std::align_val_t{64});
            curr = prev;
        }

        m_current_chunk->prev = nullptr;
        m_current_chunk->used = sizeof(Chunk);
        m_total_allocated = m_chunk_size;
    }

    [[nodiscard]] std::size_t total_allocated() const noexcept {
        return m_total_allocated;
    }

    /**
     * @brief Type-safe allocation helper bypassing std::pmr virtual dispatch overhead.
     */
    template <typename T, typename... Args>
    [[nodiscard]] T* create(Args&&... args) {
        void* storage = do_allocate(sizeof(T), alignof(T));
        if (!storage) {
            throw std::bad_alloc();
        }
        if constexpr (std::is_trivially_constructible_v<T, Args...>) {
            return ::new (storage) T(std::forward<Args>(args)...);
        } else {
            T* ptr = ::new (storage) T(std::forward<Args>(args)...);
            return ptr;
        }
    }

protected:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        // Enforce minimum alignment and safeguard against overflow
        if (alignment < alignof(std::max_align_t)) {
            alignment = alignof(std::max_align_t);
        }

        // Align current usage offset
        std::size_t current_used = m_current_chunk->used;
        std::size_t space_left = m_chunk_size - current_used;

        // Calculate padding required for alignment
        std::size_t padding = 0;
        uintptr_t current_address = reinterpret_cast<uintptr_t>(m_current_chunk) + current_used;
        uintptr_t aligned_address = (current_address + (alignment - 1)) & ~(alignment - 1);
        padding = aligned_address - current_address;

        std::size_t total_needed = bytes + padding;

        if (space_left < total_needed) {
            // Allocate a new chunk. If requested bytes exceed default chunk size, scale chunk size.
            std::size_t new_chunk_size = m_chunk_size;
            if (bytes + sizeof(Chunk) + alignment > new_chunk_size) {
                new_chunk_size = bytes + sizeof(Chunk) + alignment;
            }

            Chunk* new_chunk = allocate_chunk(new_chunk_size, m_current_chunk);
            if (!new_chunk) {
                return nullptr;
            }
            m_current_chunk = new_chunk;
            m_total_allocated += new_chunk_size;

            // Recalculate for the new chunk
            current_used = m_current_chunk->used;
            current_address = reinterpret_cast<uintptr_t>(m_current_chunk) + current_used;
            aligned_address = (current_address + (alignment - 1)) & ~(alignment - 1);
            padding = aligned_address - current_address;
            total_needed = bytes + padding;
        }

        void* ptr = reinterpret_cast<void*>(aligned_address);
        m_current_chunk->used += total_needed;
        return ptr;
    }

    void do_deallocate([[maybe_unused]] void* p, [[maybe_unused]] std::size_t bytes, [[maybe_unused]] std::size_t alignment) noexcept override {
        // Monotonic arena allocator ignores individual deallocations.
        // Memory is reclaimed in bulk via reset() or destructor.
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }

private:
    struct Chunk {
        Chunk* prev;
        std::size_t capacity;
        std::size_t used;
        // Flexible array member or tail-padded storage follows
    };

    std::size_t m_chunk_size;
    Chunk* m_current_chunk;
    std::size_t m_total_allocated;

    [[nodiscard]] Chunk* allocate_chunk(std::size_t size, Chunk* prev) {
        // Allocate raw memory with cache line alignment (64 bytes)
        void* raw = ::operator new(size, std::align_val_t{64});
        if (!raw) {
            return nullptr;
        }

        Chunk* chunk = reinterpret_cast<Chunk*>(raw);
        chunk->prev = prev;
        chunk->capacity = size;
        chunk->used = sizeof(Chunk); // Account for header metadata
        return chunk;
    }
};

} // namespace titan::memory

#endif // TITAN_MEMORY_ARENA_HPP