#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_info_types.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <memory>
#include <optional>

namespace NKikimr::NSchemeShard {

class TPath;

}

namespace NKikimr::NSchemeShard::NCdc {

    struct TCdcPqPartParams {
        ui32 TotalGroupCount = 0;
        ui32 PartitionPerTablet = 2;
        bool ReplicationAutoPartitioning = false;
        bool KeyRangeAutoPartitioning = false;
        ui32 MinPartitionCount = 0;
        ui32 MaxPartitionCount = 0;
    };

    class ICdcStreamTopicLayout {
    public:
        virtual ~ICdcStreamTopicLayout() = default;

        virtual TCdcPqPartParams MakePartParams(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const = 0;

        virtual NKikimrSchemeOp::TPersQueueGroupDescription MakePersQueueGroup(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            const TTableInfo& table,
            const TVector<TString>& boundaries,
            ui64 maxShardsInPath) const = 0;

        virtual NKikimrPQ::TBootstrapConfig MakeBootstrapConfig(
            const TTableInfo& table,
            const THashMap<TShardIdx, TShardInfo>& shardInfos) const = 0;
    };

    std::unique_ptr<ICdcStreamTopicLayout> MakeCdcStreamTopicLayout(NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode);

    std::unique_ptr<ICdcStreamTopicLayout> MakeCdcStreamTopicLayout(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags);

    std::pair<std::optional<NKikimrSchemeOp::ECdcStreamHeartbeatMode>, TString> ChooseHeartbeatMode(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags);

    // Decides the changefeed topic shape from the source table size.
    // Replication autopartitioning caps max at maxShardsInPath and min at that limit / 4.
    // An explicit TopicPartitions stays the initial count and the strategy minimum, capped by that max.
    TCdcPqPartParams MakeCdcPqPartParams(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath,
        const TFeatureFlags& featureFlags);

    std::optional<NKikimrPQ::TBootstrapConfig> MakeCdcStreamTopicBootstrapConfig(
        const TOperationContext& context,
        const TPath& topicPath);

} // namespace NKikimr::NSchemeShard::NCdc
