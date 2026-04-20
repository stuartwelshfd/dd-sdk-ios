/*
 * Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019-Present Datadog, Inc.
 */

#ifndef DD_PROFILER_MACH_SAMPLING_PROFILER_H_
#define DD_PROFILER_MACH_SAMPLING_PROFILER_H_

#include "dd_profiler.h"

#ifdef __APPLE__
#include <TargetConditionals.h>
#if !TARGET_OS_WATCH

#include <atomic>
#include <mach/mach.h>
#include <mach/thread_act.h>
#include <mach/thread_info.h>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Sets the main thread pthread identifier.
 *
 * This function should be called from the main thread early in the process lifecycle.
 *
 * @param thread The pthread identifier for the main thread
 */
void set_main_thread(pthread_t thread);

#ifdef __cplusplus
}
#endif

namespace dd::profiler {

class aggregation_worker;

/**
 * @brief Mach-based sampling profiler
 * 
 * A pure sampling engine. Collects raw stack frames at a configured interval
 * and delivers them to the callback. Binary image resolution is the
 * responsibility of the callback consumer.
 */
class mach_sampling_profiler {
public:
    /**
     * @brief Constructs a new profiler instance
     * 
     * @param config Configuration for the profiler
     * @param callback Function to call with collected stack traces
     * @param ctx Context to pass to the callback
     */
    mach_sampling_profiler(
        const sampling_config_t* config,
        stack_trace_callback_t callback,
        void* ctx);

    /**
     * @brief Destructor that ensures profiling is stopped
     */
    ~mach_sampling_profiler();

    /**
     * @brief Starts the sampling process
     * 
     * @return true if sampling was started successfully
     */
    bool start_sampling();

    /**
     * @brief Stops the sampling process.
     *
     * When called from the sampling thread or the aggregation worker thread,
     * this only requests an asynchronous stop and returns immediately. Full
     * join/drain/reset must be completed later from a non-profiler thread.
     *
     * Timeout/callback paths should use `request_stop()` directly instead of
     * calling `stop_sampling()` from within profiler-owned threads.
     */
    void stop_sampling();

    /**
     * @brief Requests a flush of the sample buffer and blocks until complete.
     * The sampling thread swaps the active buffer at its next safe point and the
     * aggregation worker drains all queued batches before unblocking the caller.
     */
    void request_flush();

    /**
     * @brief Requests that sampling stop at the next safe point.
     *
     * Unlike `stop_sampling()`, this does not join threads. It is intended for
     * asynchronous stop requests issued from the aggregation callback path or
     * other profiler-owned threads.
     */
    void request_stop();

    /**
     * @brief Atomic flag indicating if profiling is currently running
     */
    std::atomic<bool> running;

protected:
    /**
     * @brief Configuration for the profiler
     */
    sampling_config_t config;

    /**
     * @brief Callback function to receive collected stack traces
     */
    stack_trace_callback_t callback;

    /**
     * @brief Context passed to the callback function
     */
    void* ctx;

    /**
     * @brief Thread handle for the sampling thread
     */
    pthread_t sampling_thread{};
    /// Cached Mach thread id for hot-path internal-thread filtering.
    std::atomic<thread_t> sampling_thread_mach{MACH_PORT_NULL};

    /**
     * @brief Thread to profile when in single-thread mode
     */
    pthread_t target_thread{};  

    /**
     * @brief Buffer for collecting stack traces
     */
    std::vector<stack_trace_t> sample_buffer;

    /**
     * @brief Serialized aggregation worker used to drain sampled traces off-thread.
     */
    std::unique_ptr<aggregation_worker> worker;

    /**
     * @brief Main sampling loop that collects stack traces from threads
     */
    void main();

    /**
     * @brief Samples a single thread's stack (common implementation)
     * 
     * @param thread The thread to sample
     * @param interval_nanos The actual sampling interval in nanoseconds for this sample
     */
    void sample_thread(thread_t thread, uint64_t interval_nanos);

    /**
     * @brief Returns true when the thread is owned by the profiler itself.
     */
    bool is_profiler_internal_thread(thread_t thread) const;

private:
    /**
     * @brief Static entry point for the sampling thread
     */
    static void* sampling_thread_entry(void* arg);

    /**
     * @brief Mutex to protect start/stop operations from concurrent access
     */
    std::mutex state_mutex;
    /// Publishes whether `sampling_thread` contains a live thread handle.
    std::atomic<bool> sampling_thread_started{false};
};

} // namespace dd::profiler

#endif // !TARGET_OS_WATCH
#endif // __APPLE__
#endif // DD_PROFILER_MACH_SAMPLING_PROFILER_H_ 
