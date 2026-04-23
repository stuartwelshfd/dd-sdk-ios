/*
 * Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019-Present Datadog, Inc.
 */

#ifndef DD_PROFILER_AGGREGATION_WORKER_H_
#define DD_PROFILER_AGGREGATION_WORKER_H_

#include "dd_profiler.h"

#if defined(__APPLE__) && !TARGET_OS_WATCH

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <pthread.h>
#include <vector>

namespace dd::profiler {

/**
 * @brief Serialized worker that drains sampled stack-trace batches in-order.
 *
 * The aggregation worker owns the aggregation thread, flush barriers, and the
 * bounded queue used to decouple sample capture from heavy callback work.
 */
class aggregation_worker {
public:
    using flush_action_t = void (*)(void* ctx);

    aggregation_worker(
        size_t buffer_capacity,
        stack_trace_callback_t callback,
        void* ctx,
        qos_class_t worker_qos = QOS_CLASS_UTILITY);

    ~aggregation_worker();

    aggregation_worker(const aggregation_worker&) = delete;
    aggregation_worker& operator=(const aggregation_worker&) = delete;

    bool start();
    void stop();

    /**
     * @brief Blocks until all queued work before this request has been processed.
     *
     * If provided, `action` runs on the aggregation worker after all earlier
     * work has completed and before later batches are processed.
     */
    void request_flush(flush_action_t action = nullptr, void* action_ctx = nullptr);

    /**
     * @brief Enqueues the active sampling buffer, reusing a spare buffer when possible.
     */
    void enqueue_active_buffer(std::vector<stack_trace_t>& active_buffer);

    /**
     * @brief Completes a pending flush request from a producer safe point.
     */
    void service_pending_flush_request(std::vector<stack_trace_t>& active_buffer);

    /**
     * @brief Flushes the final producer buffer and marks that no more batches will arrive.
     */
    void finish_producer(std::vector<stack_trace_t>& active_buffer);

    /**
     * @brief Returns true when called from the worker thread itself.
     */
    bool is_worker_thread();

    /**
     * @brief Returns true when the given Mach thread belongs to this processor.
     */
    bool is_worker_thread(thread_t thread);

private:
    struct work_item {
        enum class kind {
            batch,
            flush_barrier
        };

        kind item_kind;
        std::vector<stack_trace_t> traces;
        uint64_t flush_id = 0;
        flush_action_t action = nullptr;
        void* action_ctx = nullptr;
    };

    size_t buffer_capacity;
    qos_class_t worker_qos;
    stack_trace_callback_t callback;
    void* ctx;

    pthread_t worker_thread{};
    bool worker_thread_started = false;
    /// Cached Mach thread id for hot-path internal-thread filtering.
    std::atomic<thread_t> worker_mach_thread{MACH_PORT_NULL};

    std::deque<work_item> pending_work;
    std::deque<work_item> requested_flushes;
    std::vector<std::vector<stack_trace_t>> reusable_buffers;

    std::mutex work_mutex;
    /// Wakes the aggregation thread when new batches or flush barriers are queued.
    std::condition_variable work_cv;
    /// Wakes flush callers when their requested flush barrier has been completed.
    std::condition_variable flush_cv;
    uint64_t next_flush_id = 0;
    uint64_t completed_flush_id = 0;
    size_t pending_batch_count = 0;
    size_t dropped_batch_count = 0;
    bool producer_finished = true;
    bool worker_finished = true;

    // Keep the queue shallow to protect memory while allowing short aggregation bursts
    // without immediately dropping a full batch.
    static constexpr size_t max_pending_batches = 2;
    static constexpr size_t max_reusable_buffers = max_pending_batches + 2;

    static void* worker_thread_entry(void* arg);
    void worker_main();
    void recycle_batch(std::vector<stack_trace_t>&& batch);
    static void destroy_batch(std::vector<stack_trace_t>& batch);
    void clear_pending_work_locked();
};

} // namespace dd::profiler

#endif // __APPLE__ && !TARGET_OS_WATCH
#endif // DD_PROFILER_AGGREGATION_WORKER_H_
