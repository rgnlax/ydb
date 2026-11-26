#pragma once

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <memory>
#include <optional>

namespace NKikimr::NSchemeShard::NCdc {

    struct TCdcPqPartParams {
        ui32 TotalGroupCount = 0;
        ui32 PartitionPerTablet = 2;
        bool ReplicationAutoPartitioning = false;
        bool KeyRangeAutoPartitioning = false;
        ui32 MinPartitionCount = 0;
        ui32 MaxPartitionCount = 0;
    };

    struct TCdcStreamTopicAutoPartitioning {
        ui32 MinPartitionCount = 0;
        ui32 MaxPartitionCount = 0;
    };

    class ICdcStreamTopicPartitioning {
    public:
        virtual ~ICdcStreamTopicPartitioning() = default;

        virtual TCdcPqPartParams MakePartParams(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const = 0;

        virtual std::optional<TCdcStreamTopicAutoPartitioning> MakeAutoPartitioning(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            const TCdcPqPartParams& params) const = 0;

        virtual bool IsPartitionedByTableKey() const = 0;
        virtual bool IsSplitByTableKey() const = 0;
    };

    std::unique_ptr<ICdcStreamTopicPartitioning> MakeCdcStreamTopicPartitioning(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags);

    std::pair<std::optional<NKikimrSchemeOp::ECdcStreamHeartbeatMode>, TString> ChooseHeartbeatMode(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags);

    bool IsSplitByTableKey(NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode);

    // Decides the changefeed topic shape from the source table size.
    // Replication autopartitioning caps max at maxShardsInPath and min at that limit / 4.
    // An explicit TopicPartitions stays the initial count and the strategy minimum, capped by that max.
    TCdcPqPartParams MakeCdcPqPartParams(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath,
        const TFeatureFlags& featureFlags);

} // namespace NKikimr::NSchemeShard::NCdc
