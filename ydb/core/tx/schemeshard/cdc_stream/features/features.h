#pragma once

#include "feature.h"

#include <ydb/core/base/feature_flags.h>

#include <memory>

namespace NKikimr::NSchemeShard::NCdc {

    using TCdcStreamFeatures = TVector<std::unique_ptr<ICdcStreamFeature>>;

    TCdcStreamFeatures MakeCdcStreamFeatures(const TFeatureFlags& featureFlags);

} // namespace NKikimr::NSchemeShard::NCdc
