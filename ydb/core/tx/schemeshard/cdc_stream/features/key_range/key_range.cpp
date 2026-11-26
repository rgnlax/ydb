#include "key_range.h"

#include <ydb/core/tx/schemeshard/cdc_stream/features/replication_capabilities.h>

#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr::NSchemeShard::NCdc {

namespace {

TString MakeFullKeyBound(const TString& bound, size_t keySize) {
    const TSerializedCellVec key(bound);
    TVector<TCell> cells(key.GetCells().begin(), key.GetCells().end());
    cells.resize(keySize);
    return TSerializedCellVec::Serialize(cells);
}

} // anonymous

TStringBuf TKeyRangeCdcStreamFeature::GetName() const {
    return "key_range";
}

TStringBuf TKeyRangeCdcStreamFeature::GetTitle() const {
    return "key range heartbeats";
}

bool TKeyRangeCdcStreamFeature::IsRequestedBy(const TCdcStreamRequest& request) const {
    const auto& streamDesc = request.Op.GetStreamDescription();
    return request.FeatureFlags.GetEnableCdcKeyRangeHeartbeats()
        && streamDesc.GetResolvedTimestampsIntervalMs() > 0
        && IsReplicationSupport(streamDesc, "supports_key_range_autopartitioning");
}

TCdcStreamDecisions TKeyRangeCdcStreamFeature::GetDecisions() const {
    return TCdcStreamDecisions{
        .HeartbeatMode = NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange,
        .TopicShape = true,
    };
}

TVector<TStringBuf> TKeyRangeCdcStreamFeature::GetOverrides() const {
    return {"replication_autopartitioning"};
}

TCdcPqPartParams TKeyRangeCdcStreamFeature::MakeTopicShape(
        const TCdcStreamRequest& request,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath) const
{
    Y_UNUSED(request);

    const ui64 maxPartitionCountLimit = std::min<ui64>(maxShardsInPath, Max<ui32>());

    // Start a key range CDC topic with one partition per table shard and let it split up to the path shard limit
    const auto totalGroupCount = static_cast<ui32>(tablePartitionCount);
    return TCdcPqPartParams{
        .TotalGroupCount = totalGroupCount,
        .KeyRangeAutoPartitioning = true,
        .MinPartitionCount = totalGroupCount,
        .MaxPartitionCount = static_cast<ui32>(std::max<ui64>(totalGroupCount, maxPartitionCountLimit)),
    };
}

std::optional<NKikimrPQ::TPartitionKeyRange> TKeyRangeCdcStreamFeature::MakeSourceKeyRange(
        const TTableInfo& table,
        size_t shardIndex) const
{
    const auto& partitions = table.GetPartitions();
    Y_ABORT_UNLESS(shardIndex < partitions.size());

    if (partitions.size() == 1) {
        return std::nullopt;
    }

    // Pass the DataShard key range to a key range CDC topic: PQ registers the source only in its partitions
    NKikimrPQ::TPartitionKeyRange keyRange;
    if (shardIndex > 0) {
        keyRange.SetFromBound(MakeFullKeyBound(partitions[shardIndex - 1]->EndOfRange, table.KeyColumnIds.size()));
    }
    if (shardIndex + 1 < partitions.size()) {
        keyRange.SetToBound(MakeFullKeyBound(partitions[shardIndex]->EndOfRange, table.KeyColumnIds.size()));
    }
    return keyRange;
}

} // namespace NKikimr::NSchemeShard::NCdc
