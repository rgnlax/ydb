#include "topic_partitions.h"

namespace NKikimr::NSchemeShard::NCdc {

TStringBuf TTopicPartitionsCdcStreamFeature::GetName() const {
    return "topic_partitions";
}

TStringBuf TTopicPartitionsCdcStreamFeature::GetTitle() const {
    return "TopicPartitions";
}

bool TTopicPartitionsCdcStreamFeature::IsRequestedBy(const TCdcStreamRequest& request) const {
    return request.Op.HasTopicPartitions();
}

TCdcStreamDecisions TTopicPartitionsCdcStreamFeature::GetDecisions() const {
    return TCdcStreamDecisions{.TopicShape = true};
}

TCdcPqPartParams TTopicPartitionsCdcStreamFeature::MakeTopicShape(
        const TCdcStreamRequest& request,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath) const
{
    Y_UNUSED(tablePartitionCount, maxShardsInPath);
    return TCdcPqPartParams{.TotalGroupCount = request.Op.GetTopicPartitions()};
}

} // namespace NKikimr::NSchemeShard::NCdc
