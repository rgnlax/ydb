#include "topic_autopartitioning.h"

namespace NKikimr::NSchemeShard::NCdc {

std::optional<TCdcPqPartParams> TTopicAutoPartitioningCdcStreamFeature::OnCreateTopic(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath) const
{
    Y_UNUSED(maxShardsInPath);

    if (!op.GetTopicAutoPartitioning()) {
        return std::nullopt;
    }

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
