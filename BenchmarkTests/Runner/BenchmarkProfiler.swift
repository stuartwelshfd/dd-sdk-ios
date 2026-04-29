/*
 * Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019-Present Datadog, Inc.
 */

import Foundation
import DatadogInternalLegacy
import DatadogBenchmarks
import OpenTelemetryApi

internal final class Profiler: DatadogInternalLegacy.BenchmarkProfiler {
    let provider: TracerProvider

    init(provider: TracerProvider) {
        self.provider = provider
    }

    func tracer(operation: @autoclosure () -> String) -> any DatadogInternalLegacy.BenchmarkTracer {
        TracerWrapper(
            tracer: provider.get(
                instrumentationName: operation(),
                instrumentationVersion: nil
            )
        )
    }
}

private final class TracerWrapper: DatadogInternalLegacy.BenchmarkTracer {
    let tracer: OpenTelemetryApi.Tracer

    init(tracer: OpenTelemetryApi.Tracer) {
        self.tracer = tracer
    }

    func startSpan(named: @autoclosure () -> String) -> any DatadogInternalLegacy.BenchmarkSpan {
        SpanWrapper(
            span: tracer
                .spanBuilder(spanName: named())
                .setActive(true)
                .startSpan()
        )
    }
}

private final class SpanWrapper: DatadogInternalLegacy.BenchmarkSpan {
    let span: OpenTelemetryApi.Span

    init(span: OpenTelemetryApi.Span) {
        self.span = span
    }

    func stop() {
        span.end()
    }
}
