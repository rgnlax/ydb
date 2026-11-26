#include "stream_factory.h"

#include <ydb/core/tx/schemeshard/cdc_stream/features/features.h>

namespace NKikimr::NSchemeShard::NCdc {

std::pair<TCdcStreamInfo::TPtr, TString> MakeCdcStream(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags)
{
    const auto features = MakeCdcStreamFeatures();
    const auto [choice, error] = ChooseCdcStreamFeatures(features, TCdcStreamRequest{op, featureFlags});
    if (!choice) {
        return {nullptr, error};
    }

    auto stream = TCdcStreamInfo::Create(op.GetStreamDescription());
    Y_ABORT_UNLESS(stream);

    if (choice->HeartbeatMode) {
        stream->AlterData->HeartbeatMode = *choice->HeartbeatMode->GetDecisions().HeartbeatMode;
    }

    return {stream, TString()};
}

} // namespace NKikimr::NSchemeShard::NCdc
