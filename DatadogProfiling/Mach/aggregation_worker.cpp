/*
 * Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019-Present Datadog, Inc.
 */

#include "aggregation_worker.h"

#if defined(__APPLE__) && !TARGET_OS_WATCH

#include <algorithm>
#include <cstdio>
#include <cstdlib>

namespace dd::profiler {

static void destroy_stack_trace_payload(stack_trace_t* trace) {
    if (!trace) return;

    if (trace->thread_name) {
        std::free((void*)trace->thread_name);
        trace->thread_name = nullptr;
    }

    if (trace->frames) {
        std::free(trace->frames);
        trace->frames = nullptr;
    }
}

aggregation_worker::aggregation_worker(
    size_t buffer_capacity,
    stack_trace_callback_t callback,
    void* ctx,
    qos_class_t worker_qos)
    : buffer_capacity(buffer_capacity)
    , worker_qos(worker_qos)
    , callback(callback)
    , ctx(ctx) {
}

aggregation_worker::~aggregation_worker() {
    stop();
}

void* aggregation_worker::worker_thread_entry(void* arg) {
    pthread_setname_np("com.datadoghq.profiler.aggregate");
    static_cast<aggregation_worker*>(arg)->worker_main();
    return nullptr;
}

bool aggregation_worker::start() {
    std::lock_guard<std::mutex> lock(work_mutex);

    if (worker_thread_started) {
        return false;
    }

    clear_pending_work_locked();
    reusable_buffers.clear();
    next_flush_id = 0;
    completed_flush_id = 0;
    pending_batch_count = 0;
    dropped_batch_count = 0;
    producer_finished = false;
    worker_finished = false;
    worker_mach_thread.store(MACH_PORT_NULL, std::memory_order_relaxed);

    pthread_attr_t worker_attr;
    pthread_attr_init(&worker_attr);
    pthread_attr_set_qos_class_np(&worker_attr, worker_qos, 0);

    int result = pthread_create(&worker_thread, &worker_attr, worker_thread_entry, this);
    pthread_attr_destroy(&worker_attr);
    if (result != 0) {
        producer_finished = true;
        worker_finished = true;
        flush_cv.notify_all();
        return false;
    }

    worker_mach_thread.store(pthread_mach_thread_np(worker_thread), std::memory_order_relaxed);
    worker_thread_started = true;
    return true;
}

void aggregation_worker::stop() {
    if (is_worker_thread()) {
        std::lock_guard<std::mutex> lock(work_mutex);
        producer_finished = true;
        work_cv.notify_all();
        flush_cv.notify_all();
        return;
    }

    {
        std::lock_guard<std::mutex> lock(work_mutex);
        if (!worker_thread_started && worker_finished) {
            clear_pending_work_locked();
            reusable_buffers.clear();
            worker_mach_thread.store(MACH_PORT_NULL, std::memory_order_relaxed);
            return;
        }

        producer_finished = true;
        work_cv.notify_all();
        flush_cv.notify_all();
    }

    if (worker_thread_started) {
        pthread_join(worker_thread, nullptr);
    }

    {
        std::lock_guard<std::mutex> lock(work_mutex);
        worker_thread_started = false;
        worker_mach_thread.store(MACH_PORT_NULL, std::memory_order_relaxed);
        clear_pending_work_locked();
        reusable_buffers.clear();
        worker_finished = true;
        producer_finished = true;
    }
}

void aggregation_worker::request_flush(flush_action_t action, void* action_ctx) {
    std::unique_lock<std::mutex> lock(work_mutex);
    bool execute_inline = false;

    const uint64_t flush_id = ++next_flush_id;
    if (worker_finished) {
        completed_flush_id = flush_id;
        execute_inline = true;
    } else {
        work_item barrier{
            work_item::kind::flush_barrier,
            {},
            flush_id,
            action,
            action_ctx
        };

        if (producer_finished) {
            pending_work.push_back(std::move(barrier));
            work_cv.notify_one();
        } else {
            requested_flushes.push_back(std::move(barrier));
        }

        flush_cv.wait(lock, [this, flush_id] {
            return completed_flush_id >= flush_id || worker_finished;
        });

        if (completed_flush_id < flush_id && worker_finished) {
            completed_flush_id = flush_id;
            execute_inline = true;
        }
    }

    lock.unlock();
    if (execute_inline && action) {
        action(action_ctx);
    }
}

void aggregation_worker::enqueue_active_buffer(std::vector<stack_trace_t>& active_buffer) {
    if (active_buffer.empty()) {
        return;
    }

    std::vector<stack_trace_t> batch;
    bool needs_reserve = false;
    bool should_drop = false;
    size_t dropped_count = 0;

    {
        std::lock_guard<std::mutex> lock(work_mutex);

        batch.swap(active_buffer);

        if (!reusable_buffers.empty()) {
            active_buffer = std::move(reusable_buffers.back());
            reusable_buffers.pop_back();
        } else {
            needs_reserve = true;
        }

        if (pending_batch_count >= max_pending_batches) {
            should_drop = true;
            dropped_count = ++dropped_batch_count;
        } else {
            pending_work.push_back({
                work_item::kind::batch,
                std::move(batch),
                0
            });
            pending_batch_count++;
        }
    }

    if (needs_reserve) {
        active_buffer.reserve(buffer_capacity);
    }

    if (should_drop) {
        destroy_batch(batch);
        return;
    }

    work_cv.notify_one();
}

void aggregation_worker::service_pending_flush_request(std::vector<stack_trace_t>& active_buffer) {
    {
        std::lock_guard<std::mutex> lock(work_mutex);
        if (requested_flushes.empty()) {
            return;
        }
    }

    enqueue_active_buffer(active_buffer);

    {
        std::lock_guard<std::mutex> lock(work_mutex);
        while (!requested_flushes.empty()) {
            pending_work.push_back(std::move(requested_flushes.front()));
            requested_flushes.pop_front();
        }
    }

    work_cv.notify_one();
}

void aggregation_worker::finish_producer(std::vector<stack_trace_t>& active_buffer) {
    service_pending_flush_request(active_buffer);
    enqueue_active_buffer(active_buffer);

    {
        std::lock_guard<std::mutex> lock(work_mutex);
        while (!requested_flushes.empty()) {
            pending_work.push_back(std::move(requested_flushes.front()));
            requested_flushes.pop_front();
        }

        producer_finished = true;
    }

    work_cv.notify_all();
    flush_cv.notify_all();
}

bool aggregation_worker::is_worker_thread() {
    std::lock_guard<std::mutex> lock(work_mutex);
    return worker_thread_started && pthread_equal(pthread_self(), worker_thread);
}

bool aggregation_worker::is_worker_thread(thread_t thread) {
    const thread_t worker_thread_id = worker_mach_thread.load(std::memory_order_relaxed);
    return worker_thread_id != MACH_PORT_NULL && thread == worker_thread_id;
}

void aggregation_worker::worker_main() {
    while (true) {
        work_item item{work_item::kind::flush_barrier, {}, 0};

        {
            std::unique_lock<std::mutex> lock(work_mutex);
            work_cv.wait(lock, [this] {
                return !pending_work.empty() || producer_finished;
            });

            if (pending_work.empty()) {
                if (producer_finished) {
                    worker_finished = true;
                    flush_cv.notify_all();
                    return;
                }

                continue;
            }

            item = std::move(pending_work.front());
            pending_work.pop_front();
            if (item.item_kind == work_item::kind::batch) {
                pending_batch_count--;
            }
        }

        if (item.item_kind == work_item::kind::batch) {
            if (callback && !item.traces.empty()) {
                callback(item.traces.data(), item.traces.size(), ctx);
            }

            destroy_batch(item.traces);
            recycle_batch(std::move(item.traces));
            continue;
        }

        if (item.action) {
            item.action(item.action_ctx);
        }

        {
            std::lock_guard<std::mutex> lock(work_mutex);
            completed_flush_id = std::max(completed_flush_id, item.flush_id);
        }

        flush_cv.notify_all();
    }
}

void aggregation_worker::recycle_batch(std::vector<stack_trace_t>&& batch) {
    batch.clear();

    std::lock_guard<std::mutex> lock(work_mutex);
    if (reusable_buffers.size() >= max_reusable_buffers) {
        return;
    }

    reusable_buffers.push_back(std::move(batch));
}

void aggregation_worker::destroy_batch(std::vector<stack_trace_t>& batch) {
    for (auto& trace : batch) {
        destroy_stack_trace_payload(&trace);
    }
}

void aggregation_worker::clear_pending_work_locked() {
    for (auto& item : pending_work) {
        if (item.item_kind == work_item::kind::batch) {
            destroy_batch(item.traces);
        }
    }

    pending_work.clear();
    requested_flushes.clear();
    pending_batch_count = 0;
}

} // namespace dd::profiler

#endif // __APPLE__ && !TARGET_OS_WATCH
