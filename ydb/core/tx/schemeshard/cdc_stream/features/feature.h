#pragma once

#include <ydb/core/tx/schemeshard/cdc_stream/topic.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <optional>

namespace NKikimr::NSchemeShard::NCdc {

    class ICdcStreamFeature {
    public:
        virtual ~ICdcStreamFeature() = default;

        virtual TString OnCreateStream(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            TCdcStreamInfo& stream) const
        {
            Y_UNUSED(op, stream);
            return {};
        }

        virtual std::optional<TCdcPqPartParams> OnCreateTopic(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const
        {
            Y_UNUSED(op, tablePartitionCount, maxShardsInPath);
            return std::nullopt;
        }

        virtual void OnTopicSources(
            const TCdcStreamInfo& stream,
            const TTableInfo& table,
            NKikimrPQ::TBootstrapConfig& sources) const
        {
            Y_UNUSED(stream, table, sources);
        }
    };

} // namespace NKikimr::NSchemeShard::NCdc
