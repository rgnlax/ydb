#pragma once

#include <ydb/core/tx/schemeshard/cdc_stream/features/feature.h>

namespace NKikimr::NSchemeShard::NCdc {

    class TKeyRangeCdcStreamFeature final: public ICdcStreamFeature {
    public:
        TStringBuf GetName() const override;
        TStringBuf GetTitle() const override;
        bool IsRequestedBy(const TCdcStreamRequest& request) const override;
        TCdcStreamDecisions GetDecisions() const override;
        TVector<TStringBuf> GetOverrides() const override;

        TCdcPqPartParams MakeTopicShape(
            const TCdcStreamRequest& request,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const override;

        std::optional<NKikimrPQ::TPartitionKeyRange> MakeSourceKeyRange(
            const TTableInfo& table,
            size_t shardIndex) const override;
    };

} // namespace NKikimr::NSchemeShard::NCdc
