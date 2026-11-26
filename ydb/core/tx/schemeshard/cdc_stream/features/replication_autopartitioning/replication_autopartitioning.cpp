#include "replication_autopartitioning.h"

#include <ydb/core/tx/schemeshard/cdc_stream/features/replication_capabilities.h>

namespace NKikimr::NSchemeShard::NCdc {

TStringBuf TReplicationAutoPartitioningCdcStreamFeature::GetName() const {
    return "replication_autopartitioning";
}

TStringBuf TReplicationAutoPartitioningCdcStreamFeature::GetTitle() const {
    return "replication topic autopartitioning";
}

bool TReplicationAutoPartitioningCdcStreamFeature::IsRequestedBy(const TCdcStreamRequest& request) const {
    return IsReplicationSupport(request.Op.GetStreamDescription(), "supports_topic_autopartitioning");
}

TCdcStreamDecisions TReplicationAutoPartitioningCdcStreamFeature::GetDecisions() const {
    return TCdcStreamDecisions{.TopicShape = true};
}

TVector<TStringBuf> TReplicationAutoPartitioningCdcStreamFeature::GetOverrides() const {
    return {"topic_autopartitioning", "topic_partitions"};
}

TCdcPqPartParams TReplicationAutoPartitioningCdcStreamFeature::MakeTopicShape(
        const TCdcStreamRequest& request,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath) const
{
    const auto& op = request.Op;

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
