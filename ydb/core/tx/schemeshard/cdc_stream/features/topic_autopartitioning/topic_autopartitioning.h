#pragma once

#include <ydb/core/tx/schemeshard/cdc_stream/features/feature.h>

namespace NKikimr::NSchemeShard::NCdc {

    class TTopicAutoPartitioningCdcStreamFeature final: public ICdcStreamFeature {
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
    };

} // namespace NKikimr::NSchemeShard::NCdc
