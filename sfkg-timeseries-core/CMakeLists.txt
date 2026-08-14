cmake_minimum_required(VERSION 3.20)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR
        "In-source builds are not supported. "
        "Use: cmake -S . -B build")
endif()

project(sfkg_timeseries_core LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SFKG_BUILD_GRPC "Build the gRPC server and smoke client" ON)
option(SFKG_BUILD_TESTS "Build local shell-stage tests" ON)
option(SFKG_BUILD_DEMOS "Build local data import demonstrations" ON)
option(SFKG_BUILD_BENCHMARKS "Build manual gRPC benchmark tools" OFF)
option(SFKG_WITH_TAOS "Build with the TDengine native client" ON)
set(SFKG_TAOS_ROOT "$ENV{HOME}/sfkg/tdengine" CACHE PATH
    "TDengine installation root containing include/taos.h and lib/libtaos.so")

find_package(Threads REQUIRED)

if(SFKG_WITH_TAOS)
    find_path(SFKG_TAOS_INCLUDE_DIR
        NAMES taos.h
        PATHS ${SFKG_TAOS_ROOT}/include ${SFKG_TAOS_ROOT}/inc
        NO_DEFAULT_PATH)
    find_library(SFKG_TAOS_LIBRARY
        NAMES taos libtaos.so
        PATHS ${SFKG_TAOS_ROOT}/lib ${SFKG_TAOS_ROOT}/driver
        NO_DEFAULT_PATH)
    if(NOT SFKG_TAOS_INCLUDE_DIR)
        message(FATAL_ERROR
            "SFKG_WITH_TAOS=ON but taos.h was not found below SFKG_TAOS_ROOT='${SFKG_TAOS_ROOT}'. "
            "Set -DSFKG_TAOS_ROOT=/path/to/tdengine.")
    endif()
    if(NOT SFKG_TAOS_LIBRARY)
        message(FATAL_ERROR
            "SFKG_WITH_TAOS=ON but libtaos.so was not found below SFKG_TAOS_ROOT='${SFKG_TAOS_ROOT}'. "
            "Install the TDengine C client or set -DSFKG_TAOS_ROOT=/path/to/tdengine.")
    endif()
endif()

add_library(sfkg_timeseries_domain
    src/alignment_service.cpp
    src/constraint_check_engine.cpp
    src/derived_series_service.cpp
    src/history_query_service.cpp
    src/ingest_service.cpp
    src/runtime_config_registry.cpp
    src/statistics_service.cpp
    src/storage_service.cpp
    src/taos_client.cpp
    src/window_service.cpp)

target_include_directories(sfkg_timeseries_domain
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(sfkg_timeseries_domain PUBLIC Threads::Threads)

if(SFKG_WITH_TAOS)
    target_compile_definitions(sfkg_timeseries_domain PUBLIC SFKG_WITH_TAOS=1)
    target_compile_definitions(sfkg_timeseries_domain PRIVATE
        SFKG_TAOS_CONFIG_DIR="${SFKG_TAOS_ROOT}/cfg")
    target_include_directories(sfkg_timeseries_domain
        PRIVATE ${SFKG_TAOS_INCLUDE_DIR})
    target_link_libraries(sfkg_timeseries_domain PUBLIC ${SFKG_TAOS_LIBRARY})
else()
    target_compile_definitions(sfkg_timeseries_domain PUBLIC SFKG_WITH_TAOS=0)
endif()

if(MSVC)
    target_compile_options(sfkg_timeseries_domain PRIVATE /W4 /permissive-)
else()
    target_compile_options(sfkg_timeseries_domain PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(SFKG_BUILD_GRPC)
    find_package(Protobuf CONFIG REQUIRED)
    find_package(gRPC CONFIG REQUIRED)

    set(SFKG_PROTO_FILE
        ${CMAKE_CURRENT_SOURCE_DIR}/proto/timeseries_core.proto)
    set(SFKG_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${SFKG_GENERATED_DIR})

    set(SFKG_PROTO_SRCS
        ${SFKG_GENERATED_DIR}/timeseries_core.pb.cc
        ${SFKG_GENERATED_DIR}/timeseries_core.grpc.pb.cc)
    set(SFKG_PROTO_HDRS
        ${SFKG_GENERATED_DIR}/timeseries_core.pb.h
        ${SFKG_GENERATED_DIR}/timeseries_core.grpc.pb.h)

    add_custom_command(
        OUTPUT ${SFKG_PROTO_SRCS} ${SFKG_PROTO_HDRS}
        COMMAND protobuf::protoc
        ARGS
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
            --experimental_allow_proto3_optional
            --cpp_out=${SFKG_GENERATED_DIR}
            --grpc_out=${SFKG_GENERATED_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            ${SFKG_PROTO_FILE}
        DEPENDS ${SFKG_PROTO_FILE}
        VERBATIM)

    add_library(sfkg_timeseries_proto ${SFKG_PROTO_SRCS} ${SFKG_PROTO_HDRS})
    target_include_directories(sfkg_timeseries_proto
        PUBLIC ${SFKG_GENERATED_DIR})
    target_link_libraries(sfkg_timeseries_proto
        PUBLIC protobuf::libprotobuf gRPC::grpc++)

    add_library(sfkg_timeseries_grpc
        src/grpc/constraint_result_receiver_client.cpp
        src/grpc/constraint_result_notification_executor.cpp
        src/grpc/ingest_task_executor.cpp
        src/grpc/proto_conversion.cpp
        src/grpc/timeseries_core_grpc_service.cpp)
    target_include_directories(sfkg_timeseries_grpc
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(sfkg_timeseries_grpc
        PUBLIC sfkg_timeseries_domain sfkg_timeseries_proto)

    add_executable(sfkg-timeseries-core-server app/main.cpp)
    target_link_libraries(sfkg-timeseries-core-server
        PRIVATE sfkg_timeseries_grpc)

    if(SFKG_WITH_TAOS)
        add_executable(etth1-temp-core-server
            tools/etth1_temp_server.cpp)
        target_link_libraries(etth1-temp-core-server
            PRIVATE sfkg_timeseries_grpc)
    endif()

    add_executable(sfkg-timeseries-core-smoke tests/smoke_client.cpp)
    target_link_libraries(sfkg-timeseries-core-smoke
        PRIVATE sfkg_timeseries_proto)

    add_executable(etth1-grpc-history-demo
        tools/etth1_grpc_history_demo.cpp)
    target_link_libraries(etth1-grpc-history-demo
        PRIVATE sfkg_timeseries_proto)

    if(SFKG_BUILD_BENCHMARKS)
        add_executable(grpc_ingest_benchmark
            tools/grpc_ingest_benchmark.cpp)
        target_link_libraries(grpc_ingest_benchmark
            PRIVATE sfkg_timeseries_proto Threads::Threads)

        add_executable(grpc_mixed_benchmark
            tools/grpc_mixed_benchmark.cpp)
        target_link_libraries(grpc_mixed_benchmark
            PRIVATE sfkg_timeseries_proto Threads::Threads)

        add_executable(grpc_stage_benchmark
            tools/grpc_stage_benchmark.cpp)
        target_link_libraries(grpc_stage_benchmark
            PRIVATE sfkg_timeseries_proto Threads::Threads)

        add_executable(etth1_grpc_ingest_benchmark
            tools/etth1_grpc_ingest_benchmark.cpp)
        target_link_libraries(etth1_grpc_ingest_benchmark
            PRIVATE sfkg_timeseries_proto Threads::Threads)

        add_executable(grpc_ordered_load_test
            tools/grpc_ordered_load_test.cpp)
        target_link_libraries(grpc_ordered_load_test
            PRIVATE sfkg_timeseries_proto Threads::Threads)

    endif()

endif()

if(SFKG_BUILD_BENCHMARKS)
    add_executable(window_service_benchmark
        tools/window_service_benchmark.cpp)
    target_link_libraries(window_service_benchmark
        PRIVATE sfkg_timeseries_domain)
endif()

if(SFKG_BUILD_TESTS)
    enable_testing()
    add_executable(runtime_config_registry_test
        tests/runtime_config_registry_test.cpp)
    target_link_libraries(runtime_config_registry_test
        PRIVATE sfkg_timeseries_domain)
    add_test(NAME runtime_config_registry_test
        COMMAND runtime_config_registry_test)

    add_executable(constraint_check_engine_test
        tests/constraint_check_engine_test.cpp)
    target_link_libraries(constraint_check_engine_test
        PRIVATE sfkg_timeseries_domain)
    add_test(NAME constraint_check_engine_test
        COMMAND constraint_check_engine_test)

    add_executable(alignment_service_test
        tests/alignment_service_test.cpp)
    target_link_libraries(alignment_service_test
        PRIVATE sfkg_timeseries_domain)
    add_test(NAME alignment_service_test
        COMMAND alignment_service_test)

    add_executable(statistics_service_test
        tests/statistics_service_test.cpp)
    target_link_libraries(statistics_service_test
        PRIVATE sfkg_timeseries_domain)
    add_test(NAME statistics_service_test
        COMMAND statistics_service_test)

    add_executable(window_service_test
        tests/window_service_test.cpp)
    target_link_libraries(window_service_test
        PRIVATE sfkg_timeseries_domain)
    add_test(NAME window_service_test
        COMMAND window_service_test)

    add_executable(window_service_differential_test
        tests/window_service_differential_test.cpp)
    target_link_libraries(window_service_differential_test
        PRIVATE sfkg_timeseries_domain)
    add_test(NAME window_service_differential_test
        COMMAND window_service_differential_test)

    add_executable(derived_series_service_test
        tests/derived_series_service_test.cpp)
    target_link_libraries(derived_series_service_test
        PRIVATE sfkg_timeseries_domain)
    add_test(NAME derived_series_service_test
        COMMAND derived_series_service_test)

    if(SFKG_BUILD_GRPC)
        add_executable(runtime_instance_config_proto_test
            tests/runtime_instance_config_proto_test.cpp)
        target_include_directories(runtime_instance_config_proto_test
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(runtime_instance_config_proto_test
            PRIVATE sfkg_timeseries_grpc)
        add_test(
            NAME runtime_instance_config_proto_test
            COMMAND runtime_instance_config_proto_test)

        add_executable(constraint_result_receiver_client_test
            tests/constraint_result_receiver_client_test.cpp)
        target_link_libraries(constraint_result_receiver_client_test
            PRIVATE sfkg_timeseries_grpc)
        add_test(
            NAME constraint_result_receiver_client_test
            COMMAND constraint_result_receiver_client_test)

        add_executable(ingest_task_executor_test
            tests/ingest_task_executor_test.cpp)
        target_link_libraries(ingest_task_executor_test
            PRIVATE sfkg_timeseries_grpc)
        add_test(
            NAME ingest_task_executor_test
            COMMAND ingest_task_executor_test)
    endif()

    if(SFKG_WITH_TAOS)
        add_executable(taos_client_smoke tests/taos_client_smoke.cpp)
        target_link_libraries(taos_client_smoke
            PRIVATE sfkg_timeseries_domain)
        add_test(NAME taos_client_smoke COMMAND taos_client_smoke)

        add_executable(direct_ingest_history_roundtrip_test
            tests/direct_ingest_history_roundtrip_test.cpp)
        target_link_libraries(direct_ingest_history_roundtrip_test
            PRIVATE sfkg_timeseries_domain)
        add_test(
            NAME direct_ingest_history_roundtrip_test
            COMMAND direct_ingest_history_roundtrip_test)

        add_executable(sharded_taos_write_test
            tests/sharded_taos_write_test.cpp)
        target_link_libraries(sharded_taos_write_test
            PRIVATE sfkg_timeseries_domain)
        add_test(
            NAME sharded_taos_write_test
            COMMAND sharded_taos_write_test)
    endif()
endif()

if(SFKG_BUILD_DEMOS AND SFKG_WITH_TAOS)
    add_executable(etth1-taos-demo tools/etth1_taos_demo.cpp)
    target_link_libraries(etth1-taos-demo
        PRIVATE sfkg_timeseries_domain)

    add_executable(etth1-direct-ingest-demo
        tools/etth1_direct_ingest_demo.cpp)
    target_link_libraries(etth1-direct-ingest-demo
        PRIVATE sfkg_timeseries_domain)
endif()

if(SFKG_BUILD_GRPC AND SFKG_WITH_TAOS)
    add_executable(local-ingest-parallel-benchmark
        tools/local_ingest_parallel_benchmark.cpp)
    target_link_libraries(local-ingest-parallel-benchmark
        PRIVATE sfkg_timeseries_grpc)
endif()
