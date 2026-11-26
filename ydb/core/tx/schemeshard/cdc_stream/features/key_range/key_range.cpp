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

TKeyRangeCdcStreamFeature::TKeyRangeCdcStreamFeature(const TFeatureFlags& featureFlags)
    : KeyRangeHeartbeatsEnabled(featureFlags.GetEnableCdcKeyRangeHeartbeats())
{
}

TString TKeyRangeCdcStreamFeature::OnCreateStream(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        TCdcStreamInfo& stream) const
{
    if (!IsRequestedBy(op)) {
        return {};
    }

    if (op.HasTopicPartitions() || op.GetTopicAutoPartitioning()) {
        return "TopicPartitions and TopicAutoPartitioning are not supported for CDC streams with key range heartbeats";
    }

    stream.HeartbeatMode = NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange;
    return {};
}

std::optional<TCdcPqPartParams> TKeyRangeCdcStreamFeature::OnCreateTopic(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath) const
{
    if (!IsRequestedBy(op)) {
        return std::nullopt;
    }

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

void TKeyRangeCdcStreamFeature::OnTopicSources(
        const TCdcStreamInfo& stream,
        const TTableInfo& table,
        NKikimrPQ::TBootstrapConfig& sources) const
{
    if (stream.HeartbeatMode != NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange) {
        return;
    }

    const auto& partitions = table.GetPartitions();
    Y_ABORT_UNLESS(static_cast<size_t>(sources.ExplicitMessageGroupsSize()) == partitions.size());

    for (size_t i = 0; i < partitions.size(); ++i) {
        auto& mg = *sources.MutableExplicitMessageGroups(i);

        // Pass the DataShard key range to a key range CDC topic: PQ registers the source only in its partitions
        if (i > 0) {
            mg.MutableKeyRange()->SetFromBound(MakeFullKeyBound(partitions[i - 1]->EndOfRange, table.KeyColumnIds.size()));
        }
        if (i + 1 < partitions.size()) {
            mg.MutableKeyRange()->SetToBound(MakeFullKeyBound(partitions[i]->EndOfRange, table.KeyColumnIds.size()));
        }
    }
}

bool TKeyRangeCdcStreamFeature::IsRequestedBy(const NKikimrSchemeOp::TCreateCdcStream& op) const {
    const auto& streamDesc = op.GetStreamDescription();
    return KeyRangeHeartbeatsEnabled
        && streamDesc.GetResolvedTimestampsIntervalMs() > 0
        && IsReplicationSupport(streamDesc, "supports_key_range_autopartitioning");
}

} // namespace NKikimr::NSchemeShard::NCdc
