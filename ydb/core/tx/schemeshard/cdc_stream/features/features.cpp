#include "features.h"

#include "key_range/key_range.h"
#include "replication_autopartitioning/replication_autopartitioning.h"
#include "topic_autopartitioning/topic_autopartitioning.h"
#include "topic_partitions/topic_partitions.h"

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard::NCdc {

namespace {

using TAxisChoice = std::optional<const ICdcStreamFeature*>;

bool Overrides(const ICdcStreamFeature& feature, const ICdcStreamFeature& other) {
    const auto overrides = feature.GetOverrides();
    return Find(overrides, other.GetName()) != overrides.end();
}

std::pair<const ICdcStreamFeature*, const ICdcStreamFeature*> FindConflictingPair(
        const TVector<const ICdcStreamFeature*>& candidates)
{
    for (size_t i = 0; i < candidates.size(); ++i) {
        for (size_t j = i + 1; j < candidates.size(); ++j) {
            if (!Overrides(*candidates[i], *candidates[j]) && !Overrides(*candidates[j], *candidates[i])) {
                return {candidates[i], candidates[j]};
            }
        }
    }
    return {candidates[0], candidates[1]};
}

std::pair<TAxisChoice, TString> ChooseAxisFeature(const TVector<const ICdcStreamFeature*>& candidates) {
    if (candidates.empty()) {
        return {TAxisChoice(nullptr), TString()};
    }

    for (const auto* candidate : candidates) {
        const bool overridesAll = AllOf(candidates, [&](const ICdcStreamFeature* other) {
            return other == candidate || Overrides(*candidate, *other);
        });
        if (overridesAll) {
            return {TAxisChoice(candidate), TString()};
        }
    }

    const auto [first, second] = FindConflictingPair(candidates);
    return {std::nullopt, TStringBuilder()
        << first->GetTitle() << " and " << second->GetTitle()
        << " cannot be used together for a CDC stream"};
}

} // anonymous

TCdcStreamFeatures MakeCdcStreamFeatures() {
    TCdcStreamFeatures features;
    features.push_back(std::make_unique<TKeyRangeCdcStreamFeature>());
    features.push_back(std::make_unique<TReplicationAutoPartitioningCdcStreamFeature>());
    features.push_back(std::make_unique<TTopicAutoPartitioningCdcStreamFeature>());
    features.push_back(std::make_unique<TTopicPartitionsCdcStreamFeature>());
    return features;
}

std::pair<std::optional<TCdcStreamFeatureChoice>, TString> ChooseCdcStreamFeatures(
        const TCdcStreamFeatures& features,
        const TCdcStreamRequest& request)
{
    TVector<const ICdcStreamFeature*> heartbeatModeCandidates;
    TVector<const ICdcStreamFeature*> topicShapeCandidates;
    for (const auto& feature : features) {
        if (!feature->IsRequestedBy(request)) {
            continue;
        }

        const auto decisions = feature->GetDecisions();
        if (decisions.HeartbeatMode) {
            heartbeatModeCandidates.push_back(feature.get());
        }
        if (decisions.TopicShape) {
            topicShapeCandidates.push_back(feature.get());
        }
    }

    const auto [heartbeatMode, heartbeatModeError] = ChooseAxisFeature(heartbeatModeCandidates);
    if (!heartbeatMode) {
        return {std::nullopt, heartbeatModeError};
    }

    const auto [topicShape, topicShapeError] = ChooseAxisFeature(topicShapeCandidates);
    if (!topicShape) {
        return {std::nullopt, topicShapeError};
    }

    return {TCdcStreamFeatureChoice{.HeartbeatMode = *heartbeatMode, .TopicShape = *topicShape}, TString()};
}

const ICdcStreamFeature* FindHeartbeatModeFeature(
        const TCdcStreamFeatures& features,
        NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode)
{
    for (const auto& feature : features) {
        if (feature->GetDecisions().HeartbeatMode == heartbeatMode) {
            return feature.get();
        }
    }
    return nullptr;
}

} // namespace NKikimr::NSchemeShard::NCdc
