// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/noop.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/tracer_provider.h>

#include <string>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "opentelemetry/trace/provider.h"

/// A trace represents the execution process of a single request in the system, span represents a
/// logical operation unit with start time and execution duration in the system, and multiple spans
/// form a trace.
namespace doris {

using OpentelemetryTracer = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>;
using OpentelemetrySpan = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;
using OpentelemetryScope = opentelemetry::trace::Scope;

/// Start a span with the specified tracer, name, and variable name, and create a Scope for this
/// span.
///
/// span represents the execution time of an operation unit, the time of its construction is the
/// start time, the time of its destructuring is the end time, you can also call the End method to
/// let span terminate, like "span->End();"
///
/// We can add Event and Attribute to span. Event will record the timestamp and event content,
/// and Attribute can record key-value pairs.
/// For example:
/// span->AddEvent("event content");
/// span->SetAttribute("key", "value");
///
/// Scope will add the span to a thread-local stack during construction, and remove the span from
/// the stack during destructuring. When starting a span, the top-of-stack span will be the parent
/// span by default, and the top-of-stack span can be obtained via the
/// opentelemetry::trace::Tracer::GetCurrentSpan() method.
#define START_AND_SCOPE_SPAN(tracer, span, name) \
    auto span = tracer->StartSpan(name);         \
    OpentelemetryScope scope {span};

namespace telemetry {

void init_tracer();

/// Return NoopTracer, the span generated by NoopTracer is NoopSpan, and the method bodies of
/// NoopTracer and NoopSpan are empty.
inline OpentelemetryTracer& get_noop_tracer() {
    static OpentelemetryTracer noop_tracer =
            opentelemetry::nostd::shared_ptr<opentelemetry::trace::NoopTracer>(
                    new opentelemetry::trace::NoopTracer);
    return noop_tracer;
}

inline OpentelemetryTracer get_tracer(const std::string& tracer_name) {
    return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(tracer_name);
}

/// Returns true if the active pan stack is not empty.
inline bool is_current_span_valid() {
    return opentelemetry::trace::Tracer::GetCurrentSpan()->GetContext().IsValid();
}

} // namespace telemetry
} // namespace doris
