#include "stream_factory.h"

#include "topic_partitioning.h"

namespace NKikimr::NSchemeShard::NCdc {

std::pair<TCdcStreamInfo::TPtr, TString> MakeCdcStream(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags)
{
    const auto [heartbeatMode, heartbeatModeError] = ChooseHeartbeatMode(op, featureFlags);
    if (!heartbeatMode) {
        return {nullptr, heartbeatModeError};
    }

    auto stream = TCdcStreamInfo::Create(op.GetStreamDescription());
    Y_ABORT_UNLESS(stream);
    stream->AlterData->HeartbeatMode = *heartbeatMode;
    return {stream, TString()};
}

} // namespace NKikimr::NSchemeShard::NCdc
