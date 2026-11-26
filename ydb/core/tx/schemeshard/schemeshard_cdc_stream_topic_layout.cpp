#include "schemeshard_cdc_stream_topic_layout.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"
#include "user_attributes.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr::NSchemeShard::NCdc {

namespace {

bool IsReplicationSupport(const NKikimrSchemeOp::TCdcStreamDescription& streamDesc, const TString& capability) {
    for (auto& attribute : streamDesc.GetUserAttributes()) {
        if (attribute.GetKey() == ATTR_ASYNC_REPLICATION) {
            if (!attribute.HasValue()) {
                break;
            }

            NJson::TJsonValue result;
            if (!NJson::ReadJsonFastTree(attribute.GetValue(), &result)) {
                break;
            }

            auto map = result.GetMap();
            return map[capability].GetBoolean();
        }
    }

    return false;
}

bool ShouldUseKeyRangeHeartbeats(const NKikimrSchemeOp::TCdcStreamDescription& streamDesc, const TFeatureFlags& featureFlags) {
    return featureFlags.GetEnableCdcKeyRangeHeartbeats()
        && streamDesc.GetResolvedTimestampsIntervalMs() > 0
        && IsReplicationSupport(streamDesc, "supports_key_range_autopartitioning");
}

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

NKikimrPQ::TMessageGroup& AddDataShardMessageGroup(
        NKikimrPQ::TBootstrapConfig& bootstrapConfig,
        const TShardIdx& shardIdx,
        const THashMap<TShardIdx, TShardInfo>& shardInfos)
{
    const auto* shardInfo = shardInfos.FindPtr(shardIdx);
    Y_ABORT_UNLESS(shardInfo != nullptr);

    auto& mg = *bootstrapConfig.AddExplicitMessageGroups();
    mg.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(shardInfo->TabletID)));
    return mg;
}

TString MakeFullKeyBound(const TString& bound, size_t keySize) {
    const TSerializedCellVec key(bound);
    TVector<TCell> cells(key.GetCells().begin(), key.GetCells().end());
    cells.resize(keySize);
    return TSerializedCellVec::Serialize(cells);
}

class TDirectCdcStreamTopicLayout: public ICdcStreamTopicLayout {
public:
    TCdcPqPartParams MakePartParams(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const override
    {
        // Autopartitioned replication topics are hash-partitioned. Cap that split
        // range by the path shard limit so a large table still gets a creatable topic.
        const ui64 maxPartitionCountLimit = std::min<ui64>(maxShardsInPath, Max<ui32>());
        const ui64 minPartitionCountLimit = maxPartitionCountLimit / 4;

        TCdcPqPartParams params;
        params.TotalGroupCount = static_cast<ui32>(op.HasTopicPartitions() ? op.GetTopicPartitions() : tablePartitionCount);
        params.PartitionPerTablet = 2;

        if (!IsReplicationSupport(op.GetStreamDescription(), "supports_topic_autopartitioning")) {
            return params;
        }

        ui64 minParts = std::min<ui64>(std::max<ui64>(tablePartitionCount / 16, 1), minPartitionCountLimit);
        ui64 maxParts = std::min<ui64>(std::max<ui64>(tablePartitionCount * 16, 50), maxPartitionCountLimit);

        ui64 total = params.TotalGroupCount;
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

        params.ReplicationAutoPartitioning = true;
        params.TotalGroupCount = static_cast<ui32>(total);
        params.MinPartitionCount = static_cast<ui32>(minParts);
        params.MaxPartitionCount = static_cast<ui32>(maxParts);
        return params;
    }

    NKikimrSchemeOp::TPersQueueGroupDescription MakePersQueueGroup(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            const TTableInfo& table,
            const TVector<TString>& boundaries,
            ui64 maxShardsInPath) const override
    {
        const auto params = MakePartParams(op, table.GetPartitions().size(), maxShardsInPath);

        NKikimrSchemeOp::TPersQueueGroupDescription desc;
        desc.SetPartitionPerTablet(params.PartitionPerTablet);
        auto& pqConfig = *desc.MutablePQTabletConfig();

        if (params.ReplicationAutoPartitioning) {
            SetCanSplitStrategy(pqConfig, params.MinPartitionCount, params.MaxPartitionCount);
        } else if (op.GetTopicAutoPartitioning()) {
            const ui32 minPartitionCount = op.HasTopicPartitions() ? op.GetTopicPartitions() : 1;
            // An explicit max is kept as-is: min > max is rejected in CreatePersQueueGroup.
            // An absent max keeps the historical ceiling of 50, but never below the min.
            const ui32 maxPartitionCount = op.HasMaxPartitionCount()
                ? op.GetMaxPartitionCount()
                : std::max<ui32>(minPartitionCount, 50);
            SetCanSplitStrategy(pqConfig, minPartitionCount, maxPartitionCount);
        }

        if (!params.ReplicationAutoPartitioning && !op.GetTopicAutoPartitioning()) {
            AddPartitionKeySchemaAndBoundaries(desc, table, boundaries);
        }

        desc.SetTotalGroupCount(params.TotalGroupCount);
        return desc;
    }

    NKikimrPQ::TBootstrapConfig MakeBootstrapConfig(
            const TTableInfo& table,
            const THashMap<TShardIdx, TShardInfo>& shardInfos) const override
    {
        NKikimrPQ::TBootstrapConfig bootstrapConfig;
        for (const auto* partition : table.GetPartitions()) {
            AddDataShardMessageGroup(bootstrapConfig, partition->ShardIdx, shardInfos);

            //NOTE: Other CDC topics get no KeyRange on the group: nothing consumes the key ranges on the pqtablet side.
            // Routing is datashard-side over the topic partitioning, split continuity is
            // the change-exchange re-registration, and the table split/merge re-registration
            // produces rangeless ids anyway.
        }
        return bootstrapConfig;
    }
};

class TKeyRangeCdcStreamTopicLayout: public ICdcStreamTopicLayout {
public:
    TCdcPqPartParams MakePartParams(
            const NKikimrSchemeOp::TCreateCdcStream&,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const override
    {
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

    NKikimrSchemeOp::TPersQueueGroupDescription MakePersQueueGroup(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            const TTableInfo& table,
            const TVector<TString>& boundaries,
            ui64 maxShardsInPath) const override
    {
        const auto params = MakePartParams(op, table.GetPartitions().size(), maxShardsInPath);

        NKikimrSchemeOp::TPersQueueGroupDescription desc;
        desc.SetPartitionPerTablet(params.PartitionPerTablet);
        auto& pqConfig = *desc.MutablePQTabletConfig();

        SetCanSplitStrategy(pqConfig, params.MinPartitionCount, params.MaxPartitionCount);
        pqConfig.SetSplitByPartitionKey(true);
        AddPartitionKeySchemaAndBoundaries(desc, table, boundaries);

        desc.SetTotalGroupCount(params.TotalGroupCount);
        return desc;
    }

    NKikimrPQ::TBootstrapConfig MakeBootstrapConfig(
            const TTableInfo& table,
            const THashMap<TShardIdx, TShardInfo>& shardInfos) const override
    {
        NKikimrPQ::TBootstrapConfig bootstrapConfig;
        const auto& partitions = table.GetPartitions();
        for (size_t i = 0; i < partitions.size(); ++i) {
            auto& mg = AddDataShardMessageGroup(bootstrapConfig, partitions[i]->ShardIdx, shardInfos);

            // Pass the DataShard key range to a key range CDC topic: PQ registers the source only in its partitions
            if (i > 0) {
                mg.MutableKeyRange()->SetFromBound(MakeFullKeyBound(partitions[i - 1]->EndOfRange, table.KeyColumnIds.size()));
            }
            if (i + 1 < partitions.size()) {
                mg.MutableKeyRange()->SetToBound(MakeFullKeyBound(partitions[i]->EndOfRange, table.KeyColumnIds.size()));
            }
        }
        return bootstrapConfig;
    }
};

NKikimrSchemeOp::ECdcStreamHeartbeatMode RequestedHeartbeatMode(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags)
{
    return ShouldUseKeyRangeHeartbeats(op.GetStreamDescription(), featureFlags)
        ? NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange
        : NKikimrSchemeOp::ECdcStreamHeartbeatModeDirect;
}

} // anonymous

std::unique_ptr<ICdcStreamTopicLayout> MakeCdcStreamTopicLayout(NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode) {
    switch (heartbeatMode) {
        case NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange:
            return std::make_unique<TKeyRangeCdcStreamTopicLayout>();
        default:
            return std::make_unique<TDirectCdcStreamTopicLayout>();
    }
}

std::unique_ptr<ICdcStreamTopicLayout> MakeCdcStreamTopicLayout(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags)
{
    return MakeCdcStreamTopicLayout(RequestedHeartbeatMode(op, featureFlags));
}

std::pair<std::optional<NKikimrSchemeOp::ECdcStreamHeartbeatMode>, TString> ChooseHeartbeatMode(
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TFeatureFlags& featureFlags)
{
    const auto heartbeatMode = RequestedHeartbeatMode(op, featureFlags);
    if (heartbeatMode != NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange) {
        return {heartbeatMode, TString()};
    }

    if (op.HasTopicPartitions() || op.GetTopicAutoPartitioning()) {
        return {std::nullopt,
            "TopicPartitions and TopicAutoPartitioning are not supported for CDC streams with key range heartbeats"};
    }

    return {heartbeatMode, TString()};
}

TCdcPqPartParams MakeCdcPqPartParams(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath,
        const TFeatureFlags& featureFlags)
{
    return MakeCdcStreamTopicLayout(op, featureFlags)->MakePartParams(op, tablePartitionCount, maxShardsInPath);
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

    auto heartbeatMode = NKikimrSchemeOp::ECdcStreamHeartbeatModeDirect;
    if (const auto it = context.SS->CdcStreams.find(streamPath.Base()->PathId); it != context.SS->CdcStreams.end()) {
        const auto& stream = it->second->AlterData ? it->second->AlterData : it->second;
        heartbeatMode = stream->HeartbeatMode;
    }

    return MakeCdcStreamTopicLayout(heartbeatMode)->MakeBootstrapConfig(*table, context.SS->ShardInfos);
}

} // namespace NKikimr::NSchemeShard::NCdc
