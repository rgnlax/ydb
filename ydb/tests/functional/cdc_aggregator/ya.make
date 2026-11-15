PY3TEST()

SIZE(LARGE)
TIMEOUT(3600)
REQUIREMENTS(cpu:4)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

ENV(YDB_TEST_DISABLE_MLOCK=1)
ENV(YDB_USE_IN_MEMORY_PDISKS=false)

TEST_SRCS(
    test_aggregator.py
)

PEERDIR(
    ydb/tests/library
)

END()
