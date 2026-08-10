#pragma once

#include <string>

#include "timeseries_core.pb.h"
#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core::grpc::conversion {

namespace pb = ::sfkg::timeseries::core::v1;

void toProto(const OperationResult& source, pb::OperationResult* target);

bool fromProto(
    const pb::TimeseriesValue& source,
    TimeseriesValue* target,
    std::string* error);
void toProto(const TimeseriesValue& source, pb::TimeseriesValue* target);

bool fromProto(
    const pb::TimeseriesIngestData& source,
    TimeseriesIngestData* target,
    std::string* error);
bool fromProto(
    const pb::TimeseriesBatch& source,
    TimeseriesBatch* target,
    std::string* error);
void toProto(const TimeseriesBatch& source, pb::TimeseriesBatch* target);

bool fromProto(
    const pb::WindowData& source,
    WindowData* target,
    std::string* error);
void toProto(const WindowData& source, pb::WindowData* target);
bool fromProto(
    const pb::AlignedWindowData& source,
    AlignedWindowData* target,
    std::string* error);
void toProto(
    const AlignedWindowData& source,
    pb::AlignedWindowData* target);

bool fromProto(
    const pb::AlignmentConfig& source,
    AlignmentConfig* target,
    std::string* error);

bool fromProto(
    const pb::RuntimeInstanceConfig& source,
    RuntimeInstanceConfig* target,
    std::string* error);
bool fromProto(
    const pb::RuntimeConstraintConfig& source,
    RuntimeConstraintConfig* target,
    std::string* error);
bool fromProto(
    const pb::RelationLagRange& source,
    RelationLagRange* target,
    std::string* error);
bool fromProto(
    const pb::RuntimeRelationSource& source,
    RuntimeRelationSource* target,
    std::string* error);
bool fromProto(
    const pb::RuntimeRelationConfig& source,
    RuntimeRelationConfig* target,
    std::string* error);
bool fromProto(
    const pb::RuntimeWindowConfig& source,
    RuntimeWindowConfig* target,
    std::string* error);
bool fromProto(
    const pb::LinearTerm& source,
    DerivedLinearTerm* target,
    std::string* error);
bool fromProto(
    const pb::LinearCombinationConfig& source,
    DerivedLinearCombination* target,
    std::string* error);
bool fromProto(
    const pb::DerivedExpression& source,
    DerivedExpression* target,
    std::string* error);
bool fromProto(
    const pb::DerivedSeriesConfig& source,
    RuntimeDerivedSeriesConfig* target,
    std::string* error);

WindowQuery fromProto(const pb::QueryWindowDataRequest& source);
HistoryQuery fromProto(const pb::QueryHistoryDataRequest& source);
HistoryOverviewQuery fromProto(
    const pb::QueryHistoryOverviewRequest& source);

void toProto(const StatisticsResult& source,
             pb::ComputeStatisticsResponse* target);
void toProto(const ConstraintCheckResult& source,
             pb::CheckConstraintsResponse* target);
void toProto(
    const HistoryOverviewResult& source,
    pb::QueryHistoryOverviewResponse* target);

}  // namespace sfkg::timeseries::core::grpc::conversion
