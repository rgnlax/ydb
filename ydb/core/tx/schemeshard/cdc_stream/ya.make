LIBRARY()

SRCS(
    impl/at_table.cpp
    impl/operation_alter.cpp
    impl/operation_common.cpp
    impl/operation_create.cpp
    impl/operation_drop.cpp
    impl/operation_rotate.cpp
    impl/registry.cpp
    impl/repository.cpp
    impl/scan.cpp
    impl/stream_factory.cpp
    impl/topic_partitioning.cpp
    impl/topic_pipeline.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/deprecated/enum_codegen
    library/cpp/json
    ydb/core/base
    ydb/core/engine
    ydb/core/metering
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/scheme
    ydb/core/statistics
    ydb/core/tablet_flat
    ydb/core/tx
    ydb/core/tx/datashard
)

END()
