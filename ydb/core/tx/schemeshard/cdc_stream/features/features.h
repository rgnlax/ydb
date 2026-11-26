#pragma once

#include "feature.h"

#include <memory>

namespace NKikimr::NSchemeShard::NCdc {

    using TCdcStreamFeatures = TVector<std::unique_ptr<ICdcStreamFeature>>;

    struct TCdcStreamFeatureChoice {
        const ICdcStreamFeature* HeartbeatMode = nullptr;
        const ICdcStreamFeature* TopicShape = nullptr;
    };

    TCdcStreamFeatures MakeCdcStreamFeatures();

    std::pair<std::optional<TCdcStreamFeatureChoice>, TString> ChooseCdcStreamFeatures(
        const TCdcStreamFeatures& features,
        const TCdcStreamRequest& request);

    const ICdcStreamFeature* FindHeartbeatModeFeature(
        const TCdcStreamFeatures& features,
        NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode);

} // namespace NKikimr::NSchemeShard::NCdc
