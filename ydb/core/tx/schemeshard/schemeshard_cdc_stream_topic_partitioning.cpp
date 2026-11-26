#include "schemeshard_cdc_stream_topic_partitioning.h"

#include "user_attributes.h"

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

TCdcPqPartParams MakeRequestedPartParams(const NKikimrSchemeOp::TCreateCdcStream& op, ui64 tablePartitionCount) {
    TCdcPqPartParams params;
    params.TotalGroupCount = static_cast<ui32>(op.HasTopicPartitions() ? op.GetTopicPartitions() : tablePartitionCount);
    params.PartitionPerTablet = 2;
    return params;
}

class TCdcStreamTopicFixedKeyPartitioning: public ICdcStreamTopicPartitioning {
public:
    TCdcPqPartParams MakePartParams(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            ui64 tablePartitionCount,
            ui64) const override
    {
        return MakeRequestedPartParams(op, tablePartitionCount);
    }

    std::optional<TCdcStreamTopicAutoPartitioning> MakeAutoPartitioning(
            const NKikimrSchemeOp::TCreateCdcStream&,
            const TCdcPqPartParams&) const override
    {
        return std::nullopt;
    }

    bool IsPartitionedByTableKey() const override {
        return true;
    }

    bool IsSplitByTableKey() const override {
        return false;
    }
};

class TCdcStreamTopicHashPartitioning: public ICdcStreamTopicPartitioning {
public:
    TCdcPqPartParams MakePartParams(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            ui64 tablePartitionCount,
            ui64) const override
    {
        return MakeRequestedPartParams(op, tablePartitionCount);
    }

    std::optional<TCdcStreamTopicAutoPartitioning> MakeAutoPartitioning(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            const TCdcPqPartParams&) const override
    {
        const ui32 minPartitionCount = op.HasTopicPartitions() ? op.GetTopicPartitions() : 1;
        // An explicit max is kept as-is: min > max is rejected in CreatePersQueueGroup.
        // An absent max keeps the historical ceiling of 50, but never below the min.
        const ui32 maxPartitionCount = op.HasMaxPartitionCount()
            ? op.GetMaxPartitionCount()
            : std::max<ui32>(minPartitionCount, 50);
        return TCdcStreamTopicAutoPartitioning{
            .MinPartitionCount = minPartitionCount,
            .MaxPartitionCount = maxPartitionCount,
        };
    }

    bool IsPartitionedByTableKey() const override {
        return false;
    }

    bool IsSplitByTableKey() const override {
        return false;
    }
};

class TCdcStreamTopicReplicationHashPartitioning: public ICdcStreamTopicPartitioning {
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

        auto params = MakeRequestedPartParams(op, tablePartitionCount);

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

    std::optional<TCdcStreamTopicAutoPartitioning> MakeAutoPartitioning(
            const NKikimrSchemeOp::TCreateCdcStream&,
            const TCdcPqPartParams& params) const override
    {
        return TCdcStreamTopicAutoPartitioning{
            .MinPartitionCount = params.MinPartitionCount,
            .MaxPartitionCount = params.MaxPartitionCount,
        };
    }

    bool IsPartitionedByTableKey() const override {
        return false;
    }

    bool IsSplitByTableKey() const override {
        return false;
    }
};

class TCdcStreamTopicKeyRangePartitioning: public ICdcStreamTopicPartitioning {
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

    std::optional<TCdcStreamTopicAutoPartitioning> MakeAutoPartitioning(
            const NKikimrSchemeOp::TCreateCdcStream&,
            const TCdcPqPartParams& params) const override
    {
        return TCdcStreamTopicAutoPartitioning{
            .MinPartitionCount = params.MinPartitionCount,
            .MaxPartitionCount = params.MaxPartitionCount,
        };
    }

    bool IsPartitionedByTableKey() const override {
        return true;
    }

    bool IsSplitByTableKey() const override {
        return true;
    }
};

} // anonymous

std::unique_ptr<ICdcStreamTopicPartitioning> MakeCdcStreamTopicPartitioning(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags)
{
    const auto& streamDesc = op.GetStreamDescription();
    if (ShouldUseKeyRangeHeartbeats(streamDesc, featureFlags)) {
        return std::make_unique<TCdcStreamTopicKeyRangePartitioning>();
    }
    if (IsReplicationSupport(streamDesc, "supports_topic_autopartitioning")) {
        return std::make_unique<TCdcStreamTopicReplicationHashPartitioning>();
    }
    if (op.GetTopicAutoPartitioning()) {
        return std::make_unique<TCdcStreamTopicHashPartitioning>();
    }
    return std::make_unique<TCdcStreamTopicFixedKeyPartitioning>();
}

std::pair<std::optional<NKikimrSchemeOp::ECdcStreamHeartbeatMode>, TString> ChooseHeartbeatMode(
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TFeatureFlags& featureFlags)
{
    if (!ShouldUseKeyRangeHeartbeats(op.GetStreamDescription(), featureFlags)) {
        return {NKikimrSchemeOp::ECdcStreamHeartbeatModeDirect, TString()};
    }

    if (op.HasTopicPartitions() || op.GetTopicAutoPartitioning()) {
        return {std::nullopt,
            "TopicPartitions and TopicAutoPartitioning are not supported for CDC streams with key range heartbeats"};
    }

    return {NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange, TString()};
}

bool IsSplitByTableKey(NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode) {
    return heartbeatMode == NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange;
}

TCdcPqPartParams MakeCdcPqPartParams(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath,
        const TFeatureFlags& featureFlags)
{
    return MakeCdcStreamTopicPartitioning(op, featureFlags)->MakePartParams(op, tablePartitionCount, maxShardsInPath);
}

} // namespace NKikimr::NSchemeShard::NCdc
