import contextlib
import json
import os
import pathlib
import platform
import statistics
import subprocess
import time

import pytest

from ydb.core.protos import flat_scheme_op_pb2, msgbus_pb2
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness import daemon
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


def write_evidence(directory, name, value):
    path = directory / name
    path.write_text(json.dumps(value, indent=2) + "\n")
    path.chmod(0o600)


@contextlib.contextmanager
def start_cluster(mode, name):
    os.umask(0o077)
    root = pathlib.Path(os.environ["YDB_CDC_ARTIFACTS_PATH"])
    directory = root / f"{name}-{mode}-{time.time_ns()}"
    directory.mkdir(mode=0o700, parents=True)
    binary = os.environ["YDB_CDC_EXPERIMENT_BINARY"]
    config = KikimrConfigGenerator(
        nodes=1,
        erasure=Erasure.NONE,
        binary_paths=[binary],
        output_path=str(directory),
        pdisk_store_path=str(directory),
        use_in_memory_pdisks=False,
        static_pdisk_size=8 * 1024**3,
        extra_feature_flags=["enable_cdc_aggregator"] if mode == "aggregated" else [],
    )
    config.yaml_config["feature_flags"]["enable_cdc_aggregator"] = mode == "aggregated"
    cluster = KiKiMR(configurator=config)
    driver = None
    try:
        cluster.start()
        driver = ydb.Driver(endpoint=f"grpc://{cluster.nodes[1].endpoint}", database="/Root")
        driver.wait(timeout=60)
        write_evidence(directory, "manifest.json", {
            "mode": mode,
            "binary": binary,
            "platform": platform.platform(),
            "pid": cluster.nodes[1].pid,
            "scope": "single YDB process including compute and storage",
        })
        yield cluster, driver, directory
    finally:
        try:
            if driver is not None:
                driver.stop(timeout=30)
        finally:
            for node in cluster.nodes.values():
                daemon.Daemon.kill(node)
            cluster.stop()


def prepare_stream(cluster, driver, mode, source_count, partition_count, interval):
    with ydb.SessionPool(driver) as pool:
        pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `/Root/source` (id Uint64 NOT NULL, value Utf8, PRIMARY KEY(id))
            WITH (UNIFORM_PARTITIONS={source_count}, AUTO_PARTITIONING_BY_SIZE=DISABLED,
                  AUTO_PARTITIONING_BY_LOAD=DISABLED);
        """, settings=ydb.BaseRequestSettings().with_timeout(180)))
        pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            ALTER TABLE `/Root/source` ADD CHANGEFEED updates WITH (
                MODE='UPDATES', FORMAT='JSON', RETENTION_PERIOD=Interval('PT1H'),
                TOPIC_MIN_ACTIVE_PARTITIONS={partition_count}, TOPIC_AUTO_PARTITIONING='DISABLED',
                RESOLVED_TIMESTAMPS=Interval('PT{interval}S')
            );
        """, settings=ydb.BaseRequestSettings().with_timeout(180)))
        settings = ydb.DescribeTableSettings().with_include_shard_key_bounds(True)
        description = pool.retry_operation_sync(
            lambda session: session.describe_table("/Root/source", settings=settings)
        )
        assert len(description.shard_key_ranges) == source_count

    response = cluster.client.send(msgbus_pb2.TSchemeDescribe(Path="/Root/source/updates"), "SchemeDescribe")
    stream = response.PathDescription.CdcStreamDescription
    expected = (
        flat_scheme_op_pb2.ECdcStreamHeartbeatModeAggregated if mode == "aggregated"
        else flat_scheme_op_pb2.ECdcStreamHeartbeatModeDirect
    )
    assert stream.HeartbeatMode == expected, response
    assert bool(stream.AggregatorTabletId) == (mode == "aggregated"), response

    path = "/Root/source/updates"
    driver.topic_client.alter_topic(path, add_consumers=["experiment"])
    partitions = driver.topic_client.describe_topic(path).partitions
    assert len([partition for partition in partitions if partition.active]) == partition_count
    row_type = ydb.BulkUpsertColumns().add_column("id", ydb.PrimitiveType.Uint64)
    row_type.add_column("value", ydb.PrimitiveType.Utf8)
    rows = [{"id": ((2 * index + 1) * 2**64) // (2 * source_count), "value": "seed"}
            for index in range(source_count)]
    driver.table_client.bulk_upsert("/Root/source", rows, column_types=row_type)
    return path


def read_progress(driver, path, partition_count, minimum=None, expected_rows=0):
    minimum = minimum or {}
    progress = {}
    rows = set()
    deadline = time.monotonic() + 120
    selector = ydb.TopicReaderSelector(path, partitions=list(range(partition_count)))
    with driver.topic_client.reader(topic=selector, consumer="experiment") as reader:
        while len(progress) < partition_count or len(rows) < expected_rows:
            assert time.monotonic() < deadline, {"progress": progress, "rows": len(rows)}
            try:
                message = reader.receive_message(timeout=5)
            except TimeoutError:
                continue
            body = json.loads(message.data)
            if "resolved" in body:
                version = tuple(int(value) for value in body["resolved"])
                partition_id = message.partition_id
                if version > tuple(minimum.get(partition_id, (0, 0))):
                    progress[partition_id] = max(progress.get(partition_id, (0, 0)), version)
            elif "update" in body or "erase" in body:
                rows.add(message.data)
    return progress


def read_offsets(driver, path, partition_count):
    topic = driver.topic_client.describe_topic(path, include_stats=True)
    assert len(topic.partitions) == partition_count
    return {partition.partition_id: partition.partition_stats.partition_end for partition in topic.partitions}


def read_resources(pid):
    output = subprocess.check_output(["ps", "-p", str(pid), "-o", "time=", "-o", "rss="], text=True)
    cpu_text, rss_text = output.split()
    days = 0
    if "-" in cpu_text:
        day_text, cpu_text = cpu_text.split("-", 1)
        days = int(day_text)
    cpu_seconds = 0.0
    for component in cpu_text.split(":"):
        cpu_seconds = cpu_seconds * 60 + float(component)
    return {"at": time.monotonic(), "cpu_seconds": days * 86400 + cpu_seconds,
            "rss_mib": int(rss_text) / 1024}


@pytest.mark.parametrize("mode", ["direct", "aggregated"])
def test_resolved_delivery_and_restart(mode):
    with start_cluster(mode, "delivery-10-10") as (cluster, driver, directory):
        path = prepare_stream(cluster, driver, mode, 10, 10, 1)
        before = read_progress(driver, path, 10, expected_rows=10)
        cluster.nodes[1].kill()
        after = read_progress(driver, path, 10, minimum=before)
        following = read_progress(driver, path, 10, minimum=after)
        write_evidence(directory, "delivery.json", {
            "before_restart": before, "after_restart": after, "continued_progress": following,
        })


@pytest.mark.skipif(os.environ.get("YDB_CDC_RUN_BENCHMARK") != "1", reason="explicit resource experiment")
@pytest.mark.parametrize("mode", ["direct", "aggregated"])
@pytest.mark.parametrize("source_count,partition_count", [(100, 100), (1000, 100)])
def test_idle_cpu(mode, source_count, partition_count):
    with start_cluster(mode, f"idle-{source_count}-{partition_count}") as (cluster, driver, directory):
        path = prepare_stream(cluster, driver, mode, source_count, partition_count, 10)
        progress = read_progress(driver, path, partition_count, expected_rows=source_count)
        time.sleep(120)
        before = read_offsets(driver, path, partition_count)
        pid = cluster.nodes[1].pid
        samples = [read_resources(pid)]
        deadline = samples[0]["at"] + 120
        while time.monotonic() < deadline:
            time.sleep(1)
            sample = read_resources(pid)
            samples.append(sample)
            assert sample["rss_mib"] < 8192, sample
        after = read_offsets(driver, path, partition_count)
        assert cluster.nodes[1].pid == pid
        assert all(after[partition] - offset >= 8 for partition, offset in before.items()), (before, after)
        following = read_progress(driver, path, partition_count, minimum=progress)
        elapsed = samples[-1]["at"] - samples[0]["at"]
        summary = {
            "mode": mode, "source_count": source_count, "partition_count": partition_count,
            "heartbeat_seconds": 10, "warmup_seconds": 120, "measured_seconds": elapsed,
            "cpu_cores": (samples[-1]["cpu_seconds"] - samples[0]["cpu_seconds"]) / elapsed,
            "rss_mean_mib": statistics.mean(sample["rss_mib"] for sample in samples),
            "offsets_before": before, "offsets_after": after, "resolved": following,
            "scope": "single YDB process including compute and storage",
        }
        write_evidence(directory, "resources.json", samples)
        write_evidence(directory, "summary.json", summary)
        print(json.dumps(summary), flush=True)
