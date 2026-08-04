#pragma once

#include <cstddef>
#include <string>
#include <utility>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core::internal {

inline OperationResult makeOperationResult(
    OperationCode code,
    std::size_t success_count,
    std::size_t failed_count,
    std::string message) {
    return OperationResult{
        code, success_count, failed_count, std::move(message)};
}

inline OperationResult notImplemented(
    const char* operation,
    std::size_t failed_count = 0) {
    return makeOperationResult(
        OperationCode::NotImplemented,
        0,
        failed_count,
        std::string(operation) + " is not implemented in the shell stage");
}

inline OperationResult invalidArgument(std::string message) {
    return makeOperationResult(
        OperationCode::InvalidArgument, 0, 0, std::move(message));
}

inline OperationResult ok(std::size_t success_count, std::string message) {
    return makeOperationResult(
        OperationCode::Ok, success_count, 0, std::move(message));
}

}  // namespace sfkg::timeseries::core::internal
