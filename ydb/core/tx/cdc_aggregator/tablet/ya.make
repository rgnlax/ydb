LIBRARY()

SRCS(
    stream.cpp
    tablet.cpp
    publish_heartbeats.cpp
    tx_write_heartbeats.cpp
    tx_configure_tracking.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_partitioning_changed.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/cdc_aggregator/public
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
