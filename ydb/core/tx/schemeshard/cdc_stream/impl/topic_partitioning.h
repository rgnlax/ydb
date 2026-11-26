#pragma once

#include <ydb/core/tx/schemeshard/cdc_stream/topic.h>

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <memory>
#include <optional>

namespace NKikimr::NSchemeShard::NCdc {

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

} // namespace NKikimr::NSchemeShard::NCdc
