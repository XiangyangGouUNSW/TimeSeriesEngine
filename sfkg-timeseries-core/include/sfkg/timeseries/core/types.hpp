#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace sfkg::timeseries::core {

using Timestamp = std::int64_t;
// ProjectId is the tenant/task isolation key. Every stateful object and
// service call that can be persisted or cached is scoped by this value.
using ProjectId = std::string;
using SequenceId = std::string;
inline constexpr Timestamp kDefaultWindowSizeMs =
    3LL * 24 * 60 * 60 * 1000;
using TimeseriesValue =
    std::variant<double, std::int64_t, bool, std::string>;

// Physical value layout used by the raw TDengine schema. This is distinct
// from SeriesKind: it describes which one of the four value columns stores a
// sequence's values.
enum class TimeseriesValueKind {
    Unknown,
    Double,
    Int64,
    Bool,
    String
};

enum class OperationCode {
    Ok,
    PartialSuccess,
    InvalidArgument,
    NotFound,
    FailedPrecondition,
    Unavailable,
    InternalError,
    NotImplemented
};

struct OperationResult {
    OperationCode code{OperationCode::Ok};
    std::size_t success_count{0};
    std::size_t failed_count{0};
    std::string message;
};

struct TimeseriesIngestData {
    std::optional<SequenceId> sequence_id;
    std::string data_source_id;
    std::string external_sequence_id;
    Timestamp time{};
    TimeseriesValue value;
    ProjectId project_id;
};

struct RawTimeseriesPoint {
    Timestamp time{};
    SequenceId sequence_id;
    TimeseriesValue value;
    ProjectId project_id;
};

struct TimeseriesBatch {
    std::vector<RawTimeseriesPoint> points;
    ProjectId project_id;
};

struct WindowData {
    Timestamp window_start_time{};
    Timestamp window_end_time{};
    std::unordered_map<SequenceId, std::vector<RawTimeseriesPoint>>
        sequence_values;
    ProjectId project_id;
};

struct AlignedSample {
    Timestamp time{};
    std::unordered_map<SequenceId, TimeseriesValue> values;
};

struct AlignedWindowData {
    Timestamp window_start_time{};
    Timestamp window_end_time{};
    std::vector<AlignedSample> samples;
    ProjectId project_id;
};

enum class VariableRole {
    Independent,
    Dependent
};

enum class SeriesKind {
    Unspecified,
    Continuous,
    Discrete,
    Categorical
};

enum class BucketAggregation {
    First,
    Last,
    Average,
    Maximum,
    Minimum
};

enum class GapFillMethod {
    Near,
    Previous,
    Next,
    Linear
};

struct SequenceAlignmentConfig {
    SequenceId sequence_id;
    VariableRole role{VariableRole::Independent};
    // An omitted strategy is resolved from the registered SeriesKind. An
    // explicitly supplied strategy always takes precedence.
    std::optional<BucketAggregation> aggregation;
    std::optional<GapFillMethod> fill_method;
};

struct AlignmentConfig {
    std::vector<SequenceAlignmentConfig> sequences;
    // If omitted, AlignmentService infers the smallest positive timestamp
    // gap in the current window.
    std::optional<std::int64_t> bucket_interval;
    ProjectId project_id;
};

// A range used by incremental alignment. prefix_samples keeps the samples
// needed by constraint terms whose sample_offset points before the affected
// anchor range.
struct AlignmentRange {
    Timestamp start_time{};
    Timestamp end_time{};
    std::size_t prefix_samples{0};
};

enum class ConstraintAggregation {
    Sample,
    Average,
    Maximum,
    Minimum
};

struct ConstraintTerm {
    std::string variable;
    double coefficient{};
    std::size_t sample_offset{};
    ConstraintAggregation aggregation{ConstraintAggregation::Sample};
};

struct ConstraintRule {
    std::string constraint_id;
    std::unordered_map<std::string, SequenceId> variable_mapping;
    std::optional<double> lower_bound;
    std::optional<double> upper_bound;
    std::vector<ConstraintTerm> terms;
    ProjectId project_id;
    std::string or_group_id;
};

struct ConstraintTermValue {
    std::string variable;
    SequenceId sequence_id;
    double coefficient{};
    std::size_t sample_offset{};
    Timestamp sample_time{};
    double value{};
    ConstraintAggregation aggregation{ConstraintAggregation::Sample};
};

struct ConstraintViolation {
    std::string constraint_id;
    Timestamp anchor_time{};
    std::optional<double> lower_bound;
    std::optional<double> upper_bound;
    double evaluated_value{};
    std::vector<ConstraintTermValue> term_values;
    std::string or_group_id;
};

struct ConstraintCheckResult {
    OperationResult operation;
    ProjectId project_id;
    std::size_t evaluated_count{};
    // Aligned samples that could not be evaluated yet because one or more
    // mapped sequences were not present in that sample. Continuous ingest
    // retries these samples when the affected sequence arrives later.
    std::size_t pending_count{};
    bool satisfied{false};
    std::vector<ConstraintViolation> violations;
};

struct SequenceWindowStatistics {
    std::size_t count{};
    double average{};
    double maximum{};
    double minimum{};
};

struct WindowStatisticsData {
    Timestamp window_start_time{};
    Timestamp window_end_time{};
    std::unordered_map<SequenceId, SequenceWindowStatistics>
        sequence_statistics;
    ProjectId project_id;
};

struct WindowStatisticsResult {
    OperationResult operation;
    WindowStatisticsData data;
};

struct WindowQuery {
    std::vector<SequenceId> sequence_ids;
    std::optional<Timestamp> start_time;
    std::optional<Timestamp> end_time;
    // Optional interpolation/fill context. Returned context points may lie
    // just outside [start_time, end_time); callers must filter them when
    // they only need the requested range. The context is intentionally
    // expressed in points, not timestamps, because constraint sample_offset
    // is positional and input timestamps need not be uniform.
    std::size_t preceding_points{0};
    std::size_t following_points{0};
    // Internal consumers such as incremental alignment can request a partial
    // point set while retaining the actual live-window bounds as metadata.
    bool preserve_window_bounds{false};
    ProjectId project_id;
};

struct HistoryQuery {
    std::vector<SequenceId> sequence_ids;
    Timestamp start_time{};
    Timestamp end_time{};
    std::optional<std::int64_t> granularity;
    ProjectId project_id;
};

// 历史数据总体信息查询条件。序列和时间边界均可省略；省略时查询
// TDengine 中当前已有数据的全部序列和全部时间范围。
struct HistoryOverviewQuery {
    std::vector<SequenceId> sequence_ids;
    std::optional<Timestamp> start_time;
    std::optional<Timestamp> end_time;
    ProjectId project_id;
};

struct RuntimeInstanceConfig {
    SequenceId sequence_id;
    std::string data_source_id;
    std::string external_sequence_id;
    std::string category_id;
    std::string data_type;
    SeriesKind series_kind{SeriesKind::Unspecified};
    ProjectId project_id;
};

// Confirmed constraint copied from the unified service for runtime use.
// enabled represents only whether the confirmed rule is currently active.
struct RuntimeConstraintConfig {
    ConstraintRule rule;
    bool enabled{false};
    ProjectId project_id;
};

struct RelationLagRange {
    std::int64_t min{};
    std::int64_t max{};
};

// A relation source may have no lag constraint, one fixed lag, or a lag range.
using RelationLagSpec = std::variant<
    std::monostate,
    std::int64_t,
    RelationLagRange>;

struct RuntimeRelationSource {
    SequenceId source_sequence_id;
    double weight{};
    RelationLagSpec lag;
};

struct RuntimeRelationConfig {
    std::string relation_id;
    std::vector<RuntimeRelationSource> sources;
    SequenceId target_sequence_id;
    std::string relation_type;
    double confidence{};
    bool enabled{false};
    ProjectId project_id;
};

// The current Core process owns one hot-window configuration. The unified
// service may replace it at runtime; an instance starts with the default.
struct RuntimeWindowConfig {
    Timestamp window_size{kDefaultWindowSizeMs};
    ProjectId project_id;
};

enum class DerivedOperator {
    Unspecified,
    Add,
    Subtract,
    Multiply,
    Divide
};

struct DerivedExpression;

struct DerivedBinaryExpression {
    DerivedOperator operation{DerivedOperator::Unspecified};
    std::shared_ptr<DerivedExpression> left;
    std::shared_ptr<DerivedExpression> right;
};

struct DerivedExpression {
    enum class NodeKind {
        Sequence,
        Constant,
        Binary
    };

    NodeKind kind{NodeKind::Sequence};
    SequenceId sequence_id;
    double constant{};
    DerivedBinaryExpression binary;
};

struct DerivedLinearTerm {
    SequenceId sequence_id;
    double coefficient{};
};

struct DerivedLinearCombination {
    std::vector<DerivedLinearTerm> terms;
    double bias{};
};

using DerivedFormula = std::variant<DerivedLinearCombination, DerivedExpression>;

struct RuntimeDerivedSeriesConfig {
    SequenceId derived_sequence_id;
    bool enabled{false};
    DerivedFormula formula;
    ProjectId project_id;
};

template <typename T>
struct RuntimeConfigSnapshot {
    std::vector<T> items;
    ProjectId project_id;
};

struct IngestResult {
    OperationResult operation;
    TimeseriesBatch resolved_data;
    ProjectId project_id;
};

struct WindowQueryResult {
    OperationResult operation;
    WindowData data;
    ProjectId project_id;
};

struct AlignmentResult {
    OperationResult operation;
    AlignedWindowData aligned_data;
    ProjectId project_id;
};

struct SequenceCorrelation {
    SequenceId independent_sequence_id;
    double coefficient{};
};

struct CorrelationVector {
    SequenceId dependent_sequence_id;
    std::vector<SequenceCorrelation> correlations;
};

struct StatisticsResult {
    OperationResult operation;
    std::unordered_map<
        SequenceId,
        std::unordered_map<std::string, TimeseriesValue>>
        sequence_metrics;
    std::optional<CorrelationVector> correlation_vector;
    ProjectId project_id;
};

struct HistoryQueryResult {
    OperationResult operation;
    TimeseriesBatch data;
    ProjectId project_id;
};

// 单个表头变量（sequence_id）的历史数据规模摘要。
struct HistorySeriesSummary {
    SequenceId sequence_id;
    std::size_t point_count{0};
    std::optional<Timestamp> first_time;
    std::optional<Timestamp> last_time;
};

// 历史数据总体信息。column_names 是当前查询范围内实际出现数据的
// sequence_id，可直接作为表格结果的逻辑表头变量名使用。
struct HistoryOverview {
    std::size_t total_point_count{0};
    std::size_t sequence_count{0};
    std::vector<SequenceId> column_names;
    std::optional<Timestamp> first_time;
    std::optional<Timestamp> last_time;
    std::vector<HistorySeriesSummary> series;
    ProjectId project_id;
};

struct HistoryOverviewResult {
    OperationResult operation;
    HistoryOverview overview;
    ProjectId project_id;
};

}  // namespace sfkg::timeseries::core
