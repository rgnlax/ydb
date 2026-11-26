#include "schemeshard_cdc_stream_topic_pipeline.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NSchemeShard::NCdc {

namespace {

void SetCanSplitStrategy(NKikimrPQ::TPQTabletConfig& pqConfig, const TCdcStreamTopicAutoPartitioning& autoPartitioning) {
    auto* ps = pqConfig.MutablePartitionStrategy();
    ps->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
    ps->SetMinPartitionCount(autoPartitioning.MinPartitionCount);
    ps->SetMaxPartitionCount(autoPartitioning.MaxPartitionCount);
    ps->SetScaleThresholdSeconds(30);
}

void AddPartitionKeySchemaAndBoundaries(
        NKikimrSchemeOp::TPersQueueGroupDescription& desc,
        const TTableInfo& table,
        const TVector<TString>& boundaries)
{
    auto& pqConfig = *desc.MutablePQTabletConfig();
    for (const auto& tag : table.KeyColumnIds) {
        Y_ABORT_UNLESS(table.Columns.contains(tag));
        const auto& column = table.Columns.at(tag);

        auto& keyComponent = *pqConfig.AddPartitionKeySchema();
        keyComponent.SetName(column.Name);
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.PType, column.PTypeMod);
        keyComponent.SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *keyComponent.MutableTypeInfo() = *columnType.TypeInfo;
        }
    }

    for (const auto& serialized : boundaries) {
        TSerializedCellVec endKey(serialized);
        Y_ABORT_UNLESS(endKey.GetCells().size() <= table.KeyColumnIds.size());

        TString errStr;
        auto& boundary = *desc.AddPartitionBoundaries();
        for (ui32 ki = 0; ki < endKey.GetCells().size(); ++ki) {
            const auto& cell = endKey.GetCells()[ki];
            const auto tag = table.KeyColumnIds.at(ki);
            Y_ABORT_UNLESS(table.Columns.contains(tag));
            const auto typeId = table.Columns.at(tag).PType;
            const bool ok = NMiniKQL::CellToValue(typeId, cell, *boundary.AddTuple(), errStr);
            Y_ABORT_UNLESS(ok, "Failed to build key tuple at position %" PRIu32 " error: %s", ki, errStr.data());
        }
    }
}

TString MakeFullKeyBound(const TString& bound, size_t keySize) {
    const TSerializedCellVec key(bound);
    TVector<TCell> cells(key.GetCells().begin(), key.GetCells().end());
    cells.resize(keySize);
    return TSerializedCellVec::Serialize(cells);
}

} // anonymous

TCdcStreamTopicPipeline::TCdcStreamTopicPipeline(std::unique_ptr<ICdcStreamTopicPartitioning> partitioning)
    : Partitioning(std::move(partitioning))
{
}

NKikimrSchemeOp::TPersQueueGroupDescription TCdcStreamTopicPipeline::MakePersQueueGroup(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TString& streamName,
        const TString& topicPath,
        const TTableInfo& table,
        const TVector<TString>& boundaries,
        ui64 maxShardsInPath) const
{
    const auto params = Partitioning->MakePartParams(op, table.GetPartitions().size(), maxShardsInPath);

    NKikimrSchemeOp::TPersQueueGroupDescription desc;
    desc.SetName("streamImpl");
    desc.SetPartitionPerTablet(params.PartitionPerTablet);
    desc.SetTotalGroupCount(params.TotalGroupCount);

    auto& pqConfig = *desc.MutablePQTabletConfig();
    pqConfig.SetTopicName(streamName);
    pqConfig.SetTopicPath(topicPath);
    pqConfig.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);

    auto& partitionConfig = *pqConfig.MutablePartitionConfig();
    partitionConfig.SetLifetimeSeconds(op.GetRetentionPeriodSeconds());
    partitionConfig.SetWriteSpeedInBytesPerSecond(1_MB); // TODO: configurable write speed
    partitionConfig.SetBurstSize(1_MB); // TODO: configurable burst
    partitionConfig.SetMaxCountInPartition(Max<i32>());

    if (const auto autoPartitioning = Partitioning->MakeAutoPartitioning(op, params)) {
        SetCanSplitStrategy(pqConfig, *autoPartitioning);
    }

    if (Partitioning->IsSplitByTableKey()) {
        pqConfig.SetSplitByPartitionKey(true);
    }

    if (Partitioning->IsPartitionedByTableKey()) {
        AddPartitionKeySchemaAndBoundaries(desc, table, boundaries);
    }

    return desc;
}

NKikimrPQ::TBootstrapConfig MakeCdcStreamTopicSources(
        const TTableInfo& table,
        const THashMap<TShardIdx, TShardInfo>& shardInfos,
        bool splitByTableKey)
{
    NKikimrPQ::TBootstrapConfig bootstrapConfig;
    const auto& partitions = table.GetPartitions();
    for (size_t i = 0; i < partitions.size(); ++i) {
        const auto* shardInfo = shardInfos.FindPtr(partitions[i]->ShardIdx);
        Y_ABORT_UNLESS(shardInfo != nullptr);

        auto& mg = *bootstrapConfig.AddExplicitMessageGroups();
        mg.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(shardInfo->TabletID)));

        // Pass the DataShard key range to a key range CDC topic: PQ registers the source only in its partitions
        if (splitByTableKey) {
            if (i > 0) {
                mg.MutableKeyRange()->SetFromBound(MakeFullKeyBound(partitions[i - 1]->EndOfRange, table.KeyColumnIds.size()));
            }
            if (i + 1 < partitions.size()) {
                mg.MutableKeyRange()->SetToBound(MakeFullKeyBound(partitions[i]->EndOfRange, table.KeyColumnIds.size()));
            }
        }

        //NOTE: Other CDC topics get no KeyRange on the group: nothing consumes the key ranges on the pqtablet side.
        // Routing is datashard-side over the topic partitioning, split continuity is
        // the change-exchange re-registration, and the table split/merge re-registration
        // produces rangeless ids anyway.
    }
    return bootstrapConfig;
}

std::optional<NKikimrPQ::TBootstrapConfig> MakeCdcStreamTopicBootstrapConfig(
        const TOperationContext& context,
        const TPath& topicPath)
{
    const auto streamPath = topicPath.Parent();
    if (!streamPath.IsCdcStream()) {
        return std::nullopt;
    }

    auto tablePath = streamPath.Parent(); // table/cdc_stream/topic
    Y_ABORT_UNLESS(tablePath.IsResolved());

    Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
    auto table = context.SS->Tables.at(tablePath.Base()->PathId);

    const auto heartbeatMode = context.SS->CdcStreamRegistry.FindPendingCdcStreamHeartbeatMode(streamPath.Base()->PathId)
        .value_or(NKikimrSchemeOp::ECdcStreamHeartbeatModeDirect);
    return MakeCdcStreamTopicSources(*table, context.SS->ShardInfos, IsSplitByTableKey(heartbeatMode));
}

} // namespace NKikimr::NSchemeShard::NCdc
