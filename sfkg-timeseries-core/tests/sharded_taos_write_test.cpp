#include <cassert>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <unistd.h>

#include "sfkg/timeseries/core/internal/taos_client.hpp"

namespace core = sfkg::timeseries::core;

class DatabaseCleanup {
public:
    explicit DatabaseCleanup(core::internal::TaosClient& client)
        : client_(client) {}

    ~DatabaseCleanup() {
        const auto result = client_.dropDatabaseForTesting();
        if (result.code != core::OperationCode::Ok) {
            std::cerr << "sharded test database cleanup failed: "
                      << result.message << '\n';
        }
    }

private:
    core::internal::TaosClient& client_;
};

int main() {
    setenv(
        "SFKG_TAOS_DB",
        (std::string("sfkg_sharded_write_test_") +
         std::to_string(static_cast<long long>(getpid()))).c_str(),
        1);
    setenv("SFKG_TAOS_HOST", "127.0.0.1", 1);
    setenv("SFKG_TAOS_PORT", "6030", 1);
    setenv("SFKG_TAOS_USER", "root", 1);
    setenv("SFKG_TAOS_PASSWORD", "taosdata", 1);
    setenv("SFKG_TAOS_KEEP_DAYS", "20000", 1);
    setenv("SFKG_TAOS_WRITE_CONNECTIONS", "4", 1);

    core::internal::TaosClient client;
    DatabaseCleanup cleanup(client);
    assert(client.ensureSchema().code == core::OperationCode::Ok);

    constexpr std::size_t kWriterCount = 4;
    constexpr std::size_t kSequenceCount = 4;
    constexpr std::size_t kPointsPerSequence = 250;
    constexpr core::Timestamp kBaseTime = 1'900'000'000'000;
    std::vector<std::string> sequence_ids;
    sequence_ids.reserve(kSequenceCount);
    std::unordered_map<std::size_t, core::TimeseriesBatch> shards;
    for (std::size_t sequence_index = 0;
         sequence_index < kSequenceCount;
         ++sequence_index) {
        const auto sequence_id =
            "sharded-sequence-" + std::to_string(sequence_index);
        sequence_ids.push_back(sequence_id);
        const auto shard = std::hash<core::SequenceId>{}(sequence_id) %
            kWriterCount;
        auto& batch = shards[shard];
        for (std::size_t point_index = 0;
             point_index < kPointsPerSequence;
             ++point_index) {
            batch.points.push_back({
                kBaseTime + static_cast<core::Timestamp>(point_index),
                sequence_id,
                static_cast<double>(sequence_index * 1000 + point_index)});
        }
    }

    std::vector<core::OperationResult> results(kWriterCount);
    std::vector<std::thread> writers;
    for (std::size_t shard = 0; shard < kWriterCount; ++shard) {
        writers.emplace_back([&, shard] {
            const auto found = shards.find(shard);
            if (found == shards.end()) {
                results[shard] = core::OperationResult{
                    core::OperationCode::Ok, 0, 0, "empty shard"};
                return;
            }
            results[shard] = client.insertRawOnConnection(
                shard, found->second);
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    for (const auto& result : results) {
        assert(result.code == core::OperationCode::Ok);
    }

    core::TimeseriesBatch read;
    const auto queried = client.queryRaw(
        sequence_ids,
        kBaseTime,
        kBaseTime + static_cast<core::Timestamp>(kPointsPerSequence),
        &read);
    assert(queried.code == core::OperationCode::Ok);
    assert(read.points.size() == kSequenceCount * kPointsPerSequence);

    std::cout << "sharded TDengine write test passed: "
              << read.points.size() << " points\n";
    return 0;
}
