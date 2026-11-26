#pragma once

#include <ydb/core/tx/schemeshard/cdc_stream/features/feature.h>

namespace NKikimr::NSchemeShard::NCdc {

    class TReplicationAutoPartitioningCdcStreamFeature final: public ICdcStreamFeature {
    public:
        std::optional<TCdcPqPartParams> OnCreateTopic(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            ui64 tablePartitionCount,
            ui64 maxShardsInPath) const override;
    };

} // namespace NKikimr::NSchemeShard::NCdc
