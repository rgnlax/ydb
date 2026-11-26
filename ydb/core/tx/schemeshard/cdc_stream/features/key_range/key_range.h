#pragma once

#include <ydb/core/tx/schemeshard/cdc_stream/features/feature.h>

#include <ydb/core/base/feature_flags.h>

namespace NKikimr::NSchemeShard::NCdc {

    class TKeyRangeCdcStreamFeature final: public ICdcStreamFeature {
    public:
        explicit TKeyRangeCdcStreamFeature(const TFeatureFlags& featureFlags);

        TString OnCreateStream(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            TCdcStreamInfo& stream) const override;

        std::optional<TCdcPqPartParams> OnCreateTopic(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const override;

        void OnTopicSources(
            const TCdcStreamInfo& stream,
            const TTableInfo& table,
            NKikimrPQ::TBootstrapConfig& sources) const override;

    private:
        bool IsRequestedBy(const NKikimrSchemeOp::TCreateCdcStream& op) const;

        const bool KeyRangeHeartbeatsEnabled;
    };

} // namespace NKikimr::NSchemeShard::NCdc
