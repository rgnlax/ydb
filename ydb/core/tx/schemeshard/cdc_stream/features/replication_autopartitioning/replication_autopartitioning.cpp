#include "replication_autopartitioning.h"

#include <ydb/core/tx/schemeshard/cdc_stream/features/replication_capabilities.h>

namespace NKikimr::NSchemeShard::NCdc {

std::optional<TCdcPqPartParams> TReplicationAutoPartitioningCdcStreamFeature::OnCreateTopic(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath) const
{
    if (!IsReplicationSupport(op.GetStreamDescription(), "supports_topic_autopartitioning")) {
        return std::nullopt;
    }

    // Autopartitioned replication topics are hash-partitioned. Cap that split
    // range by the path shard limit so a large table still gets a creatable topic.
    const ui64 maxPartitionCountLimit = std::min<ui64>(maxShardsInPath, Max<ui32>());
    const ui64 minPartitionCountLimit = maxPartitionCountLimit / 4;

    ui64 minParts = std::min<ui64>(std::max<ui64>(tablePartitionCount / 16, 1), minPartitionCountLimit);
    ui64 maxParts = std::min<ui64>(std::max<ui64>(tablePartitionCount * 16, 50), maxPartitionCountLimit);

    ui64 total = op.HasTopicPartitions() ? op.GetTopicPartitions() : tablePartitionCount;
    if (op.HasTopicPartitions()) {
        // TopicPartitions is min_active_partitions: the initial count and the
        // strategy minimum. Only the path shard ceiling applies, so a smaller
        // request is not raised to the formula minimum.
        if (total > maxParts) {
            total = maxParts;
        }
        minParts = total;
    } else if (total > maxParts) {
        total = maxParts;
    }

    return TCdcPqPartParams{
        .TotalGroupCount = static_cast<ui32>(total),
        .ReplicationAutoPartitioning = true,
        .MinPartitionCount = static_cast<ui32>(minParts),
        .MaxPartitionCount = static_cast<ui32>(maxParts),
    };
}

} // namespace NKikimr::NSchemeShard::NCdc
