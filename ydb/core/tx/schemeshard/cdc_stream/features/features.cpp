#include "features.h"

#include "key_range/key_range.h"
#include "replication_autopartitioning/replication_autopartitioning.h"
#include "topic_autopartitioning/topic_autopartitioning.h"

namespace NKikimr::NSchemeShard::NCdc {

TCdcStreamFeatures MakeCdcStreamFeatures(const TFeatureFlags& featureFlags) {
    TCdcStreamFeatures features;
    features.push_back(std::make_unique<TKeyRangeCdcStreamFeature>(featureFlags));
    features.push_back(std::make_unique<TReplicationAutoPartitioningCdcStreamFeature>());
    features.push_back(std::make_unique<TTopicAutoPartitioningCdcStreamFeature>());
    return features;
}

} // namespace NKikimr::NSchemeShard::NCdc
