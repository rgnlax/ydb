#include "topic_autopartitioning.h"

namespace NKikimr::NSchemeShard::NCdc {

TStringBuf TTopicAutoPartitioningCdcStreamFeature::GetName() const {
    return "topic_autopartitioning";
}

TStringBuf TTopicAutoPartitioningCdcStreamFeature::GetTitle() const {
    return "TopicAutoPartitioning";
}

bool TTopicAutoPartitioningCdcStreamFeature::IsRequestedBy(const TCdcStreamRequest& request) const {
    return request.Op.GetTopicAutoPartitioning();
}

TCdcStreamDecisions TTopicAutoPartitioningCdcStreamFeature::GetDecisions() const {
    return TCdcStreamDecisions{.TopicShape = true};
}

TVector<TStringBuf> TTopicAutoPartitioningCdcStreamFeature::GetOverrides() const {
    return {"topic_partitions"};
}

TCdcPqPartParams TTopicAutoPartitioningCdcStreamFeature::MakeTopicShape(
        const TCdcStreamRequest& request,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath) const
{
    Y_UNUSED(maxShardsInPath);

    const auto& op = request.Op;
    const ui32 minPartitionCount = op.HasTopicPartitions() ? op.GetTopicPartitions() : 1;
    // An explicit max is kept as-is: min > max is rejected in CreatePersQueueGroup.
    // An absent max keeps the historical ceiling of 50, but never below the min.
    const ui32 maxPartitionCount = op.HasMaxPartitionCount()
        ? op.GetMaxPartitionCount()
        : std::max<ui32>(minPartitionCount, 50);

    return TCdcPqPartParams{
        .TotalGroupCount = static_cast<ui32>(op.HasTopicPartitions() ? op.GetTopicPartitions() : tablePartitionCount),
        .TopicAutoPartitioning = true,
        .MinPartitionCount = minPartitionCount,
        .MaxPartitionCount = maxPartitionCount,
    };
}

} // namespace NKikimr::NSchemeShard::NCdc
