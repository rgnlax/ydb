#include "stream.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

bool ParseTrackingConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TTrackingConfig& config, TString& error) {
    config.StreamPathId = TPathId::FromProto(proto.GetStreamPathId());
    if (!config.StreamPathId.OwnerId || !config.StreamPathId.LocalPathId) {
        error = "Stream path id is not set";
        return false;
    }
    config.StreamPath = proto.GetStreamPath();

    auto& sources = config.SourceDataShardIds;
    sources.assign(proto.GetSourceDataShardIds().begin(), proto.GetSourceDataShardIds().end());
    std::sort(sources.begin(), sources.end());
    if (sources.empty() || sources.front() == 0) {
        error = "Source DataShard ids are missing or zero";
        return false;
    }
    if (std::adjacent_find(sources.begin(), sources.end()) != sources.end()) {
        error = "Duplicate source DataShard id";
        return false;
    }

    auto& partitions = config.TargetPartitions;
    for (const auto& partition : proto.GetTargetPartitions()) {
        if (!partition.GetTabletId()) {
            error = TStringBuilder() << "Partition " << partition.GetPartitionId() << " has no tablet id";
            return false;
        }
        partitions.push_back({.PartitionId = partition.GetPartitionId(), .TabletId = partition.GetTabletId()});
    }
    if (partitions.empty()) {
        error = "Target partitions are missing";
        return false;
    }
    std::sort(partitions.begin(), partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId < right.PartitionId;
    });
    const auto duplicate = std::adjacent_find(partitions.begin(), partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId == right.PartitionId;
    });
    if (duplicate != partitions.end()) {
        error = TStringBuilder() << "Duplicate partition " << duplicate->PartitionId;
        return false;
    }

    return true;
}

bool TStreamChanges::IsEmpty() const {
    return !ConfigChanged
        && UpsertedSources.empty()
        && ErasedSources.empty()
        && UpsertedHeartbeats.empty()
        && ErasedHeartbeats.empty()
        && !CurrentHeartbeatChanged;
}

TStream TStream::Restore(
        TTrackingConfig config,
        THashMap<ui64, std::optional<TRowVersion>> sources,
        const TMap<TRowVersion, TString>& heartbeats,
        std::optional<NPQ::THeartbeat> currentHeartbeat)
{
    TStream stream;
    stream.Config = std::move(config);
    stream.CurrentHeartbeat = std::move(currentHeartbeat);

    for (const auto& [source, version] : sources) {
        if (!version) {
            continue;
        }
        auto it = heartbeats.find(*version);
        Y_ENSURE(it != heartbeats.end(), "Source " << source << " has no heartbeat data for version " << *version);
        auto& held = stream.Heartbeats[*version];
        held.Data = it->second;
        ++held.SourceCount;
        ++stream.SourcesWithHeartbeat;
    }
    Y_ENSURE(stream.Heartbeats.size() == heartbeats.size(), "Heartbeat data is not held by any source");
    stream.Sources = std::move(sources);

    return stream;
}

const TTrackingConfig& TStream::GetConfig() const {
    Y_ENSURE(Config, "Tracking is not configured");
    return *Config;
}

EConfigureOutcome TStream::Configure(const TTrackingConfig& config, TStreamChanges& changes) {
    if (Config) {
        return *Config == config ? EConfigureOutcome::Success : EConfigureOutcome::Conflict;
    }

    Config = config;
    changes.ConfigChanged = true;
    for (const auto source : config.SourceDataShardIds) {
        AddSource(source, changes);
    }

    return EConfigureOutcome::Success;
}

ESourceOutcome TStream::WriteHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat, TStreamChanges& changes) {
    Y_ENSURE(Config, "Tracking is not configured");

    auto it = Sources.find(sourceDataShardId);
    if (it == Sources.end()) {
        return ESourceOutcome::UnknownSource;
    }

    auto& version = it->second;
    if (version && heartbeat.Version <= *version) {
        return ESourceOutcome::Success;
    }

    if (version) {
        ReleaseHeartbeat(*version, changes);
    } else {
        ++SourcesWithHeartbeat;
    }
    version = heartbeat.Version;
    HoldHeartbeat(heartbeat, changes);
    changes.UpsertedSources.insert(sourceDataShardId);

    AdvanceCurrentHeartbeat(changes);
    return ESourceOutcome::Success;
}

ESourceOutcome TStream::ChangePartitioning(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds, TStreamChanges& changes) {
    Y_ENSURE(Config, "Tracking is not configured");

    if (!Sources.contains(parentDataShardId)) {
        const bool applied = std::all_of(childDataShardIds.begin(), childDataShardIds.end(), [this](ui64 childDataShardId) {
            return Sources.contains(childDataShardId);
        });
        return applied ? ESourceOutcome::Success : ESourceOutcome::UnknownSource;
    }

    EraseSource(parentDataShardId, changes);
    for (const auto childDataShardId : childDataShardIds) {
        if (!Sources.contains(childDataShardId)) {
            AddSource(childDataShardId, changes);
        }
    }

    AdvanceCurrentHeartbeat(changes);
    return ESourceOutcome::Success;
}

void TStream::AddSource(ui64 sourceDataShardId, TStreamChanges& changes) {
    const bool inserted = Sources.emplace(sourceDataShardId, std::nullopt).second;
    Y_ENSURE(inserted, "Duplicate source DataShard " << sourceDataShardId);
    changes.UpsertedSources.insert(sourceDataShardId);
}

void TStream::EraseSource(ui64 sourceDataShardId, TStreamChanges& changes) {
    auto it = Sources.find(sourceDataShardId);
    Y_ENSURE(it != Sources.end(), "Unknown source DataShard " << sourceDataShardId);
    if (const auto& version = it->second) {
        ReleaseHeartbeat(*version, changes);
        --SourcesWithHeartbeat;
    }
    Sources.erase(it);
    changes.ErasedSources.insert(sourceDataShardId);
}

void TStream::HoldHeartbeat(const NPQ::THeartbeat& heartbeat, TStreamChanges& changes) {
    auto [it, inserted] = Heartbeats.try_emplace(heartbeat.Version);
    if (inserted) {
        it->second.Data = heartbeat.Data;
        changes.UpsertedHeartbeats[heartbeat.Version] = heartbeat.Data;
        changes.ErasedHeartbeats.erase(heartbeat.Version);
    }
    ++it->second.SourceCount;
}

void TStream::ReleaseHeartbeat(const TRowVersion& version, TStreamChanges& changes) {
    auto it = Heartbeats.find(version);
    Y_ENSURE(it != Heartbeats.end() && it->second.SourceCount > 0, "Heartbeat " << version << " is not held");
    if (--it->second.SourceCount == 0) {
        Heartbeats.erase(it);
        changes.UpsertedHeartbeats.erase(version);
        changes.ErasedHeartbeats.insert(version);
    }
}

void TStream::AdvanceCurrentHeartbeat(TStreamChanges& changes) {
    if (Sources.empty() || SourcesWithHeartbeat != Sources.size()) {
        return;
    }

    const auto& [version, held] = *Heartbeats.begin();
    if (CurrentHeartbeat && version <= CurrentHeartbeat->Version) {
        return;
    }

    CurrentHeartbeat = NPQ::THeartbeat{.Version = version, .Data = held.Data};
    changes.CurrentHeartbeatChanged = true;
}

}
