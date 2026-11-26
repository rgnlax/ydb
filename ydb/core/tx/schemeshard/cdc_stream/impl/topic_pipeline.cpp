#include "topic_pipeline.h"

#include "stream_changes.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NSchemeShard::NCdc {

namespace {

void SetCanSplitStrategy(NKikimrPQ::TPQTabletConfig& pqConfig, ui32 minPartitionCount, ui32 maxPartitionCount) {
    auto* ps = pqConfig.MutablePartitionStrategy();
    ps->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
    ps->SetMinPartitionCount(minPartitionCount);
    ps->SetMaxPartitionCount(maxPartitionCount);
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

} // anonymous

TCdcPqPartParams MakeCdcStreamTopicShape(
        const TCdcStreamFeatures& features,
        const TCdcStreamRequest& request,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath)
{
    const auto [choice, _] = ChooseCdcStreamFeatures(features, request);
    if (choice && choice->TopicShape) {
        return choice->TopicShape->MakeTopicShape(request, tablePartitionCount, maxShardsInPath);
    }

    return TCdcPqPartParams{.TotalGroupCount = static_cast<ui32>(tablePartitionCount)};
}

NKikimrSchemeOp::TPersQueueGroupDescription MakeCdcStreamTopic(
        const TCdcStreamFeatures& features,
        const TCdcStreamRequest& request,
        const TString& streamName,
        const TString& topicPath,
        const TTableInfo& table,
        const TVector<TString>& boundaries,
        ui64 maxShardsInPath)
{
    const auto& op = request.Op;
    const auto shape = MakeCdcStreamTopicShape(features, request, table.GetPartitions().size(), maxShardsInPath);

    NKikimrSchemeOp::TPersQueueGroupDescription desc;
    desc.SetName("streamImpl");
    desc.SetPartitionPerTablet(shape.PartitionPerTablet);
    desc.SetTotalGroupCount(shape.TotalGroupCount);

    auto& pqConfig = *desc.MutablePQTabletConfig();
    pqConfig.SetTopicName(streamName);
    pqConfig.SetTopicPath(topicPath);
    pqConfig.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);

    auto& partitionConfig = *pqConfig.MutablePartitionConfig();
    partitionConfig.SetLifetimeSeconds(op.GetRetentionPeriodSeconds());
    partitionConfig.SetWriteSpeedInBytesPerSecond(1_MB); // TODO: configurable write speed
    partitionConfig.SetBurstSize(1_MB); // TODO: configurable burst
    partitionConfig.SetMaxCountInPartition(Max<i32>());

    if (shape.ReplicationAutoPartitioning || shape.KeyRangeAutoPartitioning || shape.TopicAutoPartitioning) {
        SetCanSplitStrategy(pqConfig, shape.MinPartitionCount, shape.MaxPartitionCount);
    }

    if (shape.KeyRangeAutoPartitioning) {
        pqConfig.SetSplitByPartitionKey(true);
    }

    if (!shape.ReplicationAutoPartitioning && !shape.TopicAutoPartitioning) {
        AddPartitionKeySchemaAndBoundaries(desc, table, boundaries);
    }

    return desc;
}

NKikimrPQ::TBootstrapConfig MakeCdcStreamTopicSources(
        const TCdcStreamFeatures& features,
        const TCdcStreamInfo* stream,
        const TTableInfo& table,
        const THashMap<TShardIdx, TShardInfo>& shardInfos)
{
    const ICdcStreamFeature* heartbeatModeFeature = stream
        ? FindHeartbeatModeFeature(features, stream->HeartbeatMode)
        : nullptr;

    NKikimrPQ::TBootstrapConfig sources;
    const auto& partitions = table.GetPartitions();
    for (size_t i = 0; i < partitions.size(); ++i) {
        const auto* shardInfo = shardInfos.FindPtr(partitions[i]->ShardIdx);
        Y_ABORT_UNLESS(shardInfo != nullptr);

        auto& mg = *sources.AddExplicitMessageGroups();
        mg.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(shardInfo->TabletID)));

        if (heartbeatModeFeature) {
            if (auto keyRange = heartbeatModeFeature->MakeSourceKeyRange(table, i)) {
                *mg.MutableKeyRange() = std::move(*keyRange);
            }
        }

        //NOTE: Other CDC topics get no KeyRange on the group: nothing consumes the key ranges on the pqtablet side.
        // Routing is datashard-side over the topic partitioning, split continuity is
        // the change-exchange re-registration, and the table split/merge re-registration
        // produces rangeless ids anyway.
    }

    return sources;
}

TCdcPqPartParams MakeCdcPqPartParams(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath,
        const TFeatureFlags& featureFlags)
{
    return MakeCdcStreamTopicShape(MakeCdcStreamFeatures(), TCdcStreamRequest{op, featureFlags}, tablePartitionCount, maxShardsInPath);
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

    const auto stream = FindPendingCdcStream(*context.SS, streamPath.Base()->PathId);
    return MakeCdcStreamTopicSources(
        MakeCdcStreamFeatures(),
        stream.Get(),
        *table,
        context.SS->ShardInfos);
}

} // namespace NKikimr::NSchemeShard::NCdc
