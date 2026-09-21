#pragma once

#include <string_view>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core::validation {

// These limits mirror the physical TDengine NCHAR columns. They are byte
// limits, which is conservative for UTF-8 and prevents an invalid value from
// reaching the asynchronous storage lane.
inline constexpr std::size_t kMaxSequenceIdBytes = 512;
inline constexpr std::size_t kMaxStringValueBytes = 1024;

// Maps registry metadata to the physical value kind expected at ingest time.
// Unknown metadata remains compatible with existing deployments and accepts
// any concrete TimeseriesValue kind.
TimeseriesValueKind expectedValueKind(std::string_view data_type) noexcept;
TimeseriesValueKind valueKind(const TimeseriesValue& value) noexcept;

// Returns nullptr on success and a stable diagnostic string on failure.
// This is intentionally allocation-free on the hot path.
const char* validateValue(
    const TimeseriesValue& value,
    TimeseriesValueKind expected = TimeseriesValueKind::Unknown) noexcept;

// Checks only whether a raw point can be consumed safely. It does not inspect
// constraints, value ranges, derived series, or registry membership.
const char* validateRawPoint(const RawTimeseriesPoint& point) noexcept;

// Validates project isolation without requiring redundant project_id copies
// on every point. Empty nested IDs inherit the enclosing project.
const char* validateBatchContext(
    const ProjectId& project_id,
    const TimeseriesBatch& batch) noexcept;
const char* validateRawPoint(
    const ProjectId& project_id,
    const RawTimeseriesPoint& point) noexcept;

// Validates the structural invariants shared by services that consume a
// manually assembled WindowData. Empty sequence point vectors are allowed;
// individual services decide whether an empty sequence is a partial result or
// an invalid request.
const char* validateWindowData(
    const ProjectId& project_id,
    const WindowData& data) noexcept;

// Validates the structural invariants shared by services that consume a
// manually assembled AlignedWindowData. Values are checked for safe storage,
// while numeric-only requirements remain the responsibility of the consumer.
const char* validateAlignedWindowData(
    const ProjectId& project_id,
    const AlignedWindowData& data) noexcept;

}  // namespace sfkg::timeseries::core::validation
