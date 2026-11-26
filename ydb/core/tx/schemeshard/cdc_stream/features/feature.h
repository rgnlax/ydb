#pragma once

#include <ydb/core/tx/schemeshard/cdc_stream/topic.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <optional>

namespace NKikimr::NSchemeShard::NCdc {

    struct TCdcStreamRequest {
        const NKikimrSchemeOp::TCreateCdcStream& Op;
        const TFeatureFlags& FeatureFlags;
    };

    struct TCdcStreamDecisions {
        std::optional<NKikimrSchemeOp::ECdcStreamHeartbeatMode> HeartbeatMode;
        bool TopicShape = false;
    };

    class ICdcStreamFeature {
    public:
        virtual ~ICdcStreamFeature() = default;

        virtual TStringBuf GetName() const = 0;
        virtual TStringBuf GetTitle() const = 0;
        virtual bool IsRequestedBy(const TCdcStreamRequest& request) const = 0;
        virtual TCdcStreamDecisions GetDecisions() const = 0;

        virtual TVector<TStringBuf> GetOverrides() const {
            return {};
        }

        virtual TCdcPqPartParams MakeTopicShape(
            const TCdcStreamRequest& request,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const
        {
            Y_UNUSED(request, tablePartitionCount, maxShardsInPath);
            Y_ABORT_S("CDC stream feature " << GetName() << " decides topic_shape but does not make it");
        }

        virtual std::optional<NKikimrPQ::TPartitionKeyRange> MakeSourceKeyRange(
            const TTableInfo& table,
            size_t shardIndex) const
        {
            Y_UNUSED(table, shardIndex);
            return std::nullopt;
        }
    };

} // namespace NKikimr::NSchemeShard::NCdc
