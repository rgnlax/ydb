#include "schemeshard_cdc_stream_registry.h"

#include "schemeshard__operation_part.h"
#include "schemeshard_cdc_stream_topic_partitioning.h"
#include "schemeshard_impl.h"

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

TCdcStreamRegistry::TCdcStreamRegistry(TSchemeShard& self)
    : Self(self)
{
}

bool TCdcStreamRegistry::HasCdcStream(const TPathId& streamPathId) const {
    return Self.CdcStreams.contains(streamPathId);
}

std::optional<TCdcStreamSettings> TCdcStreamRegistry::FindCdcStreamSettings(const TPathId& streamPathId) const {
    const auto it = Self.CdcStreams.find(streamPathId);
    if (it == Self.CdcStreams.end()) {
        return std::nullopt;
    }
    return static_cast<const TCdcStreamSettings&>(*it->second);
}

ui64 TCdcStreamRegistry::GetCdcStreamVersion(const TPathId& streamPathId) const {
    return GetCdcStream(streamPathId)->AlterVersion;
}

std::optional<NKikimrSchemeOp::ECdcStreamHeartbeatMode> TCdcStreamRegistry::FindPendingCdcStreamHeartbeatMode(
        const TPathId& streamPathId) const
{
    const auto it = Self.CdcStreams.find(streamPathId);
    if (it == Self.CdcStreams.end()) {
        return std::nullopt;
    }

    const auto& stream = it->second->AlterData ? it->second->AlterData : it->second;
    return stream->HeartbeatMode;
}

NKikimrSchemeOp::TCdcStreamDescription TCdcStreamRegistry::MakeCdcStreamDescription(
        const TPathId& streamPathId,
        const TString& name) const
{
    Y_VERIFY_S(Self.CdcStreams.contains(streamPathId), "Cdc stream not found"
        << ": pathId# " << streamPathId
        << ", name# " << name);
    return MakeCdcStreamDescription(streamPathId, name, *Self.CdcStreams.at(streamPathId));
}

NKikimrSchemeOp::TCdcStreamDescription TCdcStreamRegistry::MakeCdcStreamAlterDescription(
        const TPathId& streamPathId,
        const TString& name) const
{
    const auto stream = GetCdcStream(streamPathId);
    Y_VERIFY_S(stream->AlterData, "Empty cdc stream info"
        << ": pathId# " << streamPathId
        << ", name# " << name);
    return MakeCdcStreamDescription(streamPathId, name, *stream->AlterData);
}

void TCdcStreamRegistry::AddCdcStream(
        TOperationContext& context,
        const TPathId& streamPathId,
        TCdcStreamInfo::TPtr stream)
{
    context.MemChanges.GrabNewCdcStream(&Self, streamPathId);
    context.DbChanges.PersistAlterCdcStream(streamPathId);
    Self.CdcStreams.Set(streamPathId, stream);
}

TCdcStreamInfo::TPtr TCdcStreamRegistry::BeginCdcStreamAlter(
        TOperationContext& context,
        const TPathId& streamPathId)
{
    context.MemChanges.GrabCdcStream(&Self, streamPathId);
    context.DbChanges.PersistAlterCdcStream(streamPathId);

    auto streamAlter = GetCdcStream(streamPathId)->CreateNextVersion();
    Y_ABORT_UNLESS(streamAlter);
    return streamAlter;
}

void TCdcStreamRegistry::FinishCdcStreamCreate(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    const auto stream = GetCdcStream(streamPathId);
    PersistCdcStream(db, streamPathId);
    Self.CdcStreams.Set(streamPathId, stream->AlterData);
}

void TCdcStreamRegistry::FinishCdcStreamAlter(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    PersistCdcStream(db, streamPathId);
    GetCdcStream(streamPathId)->FinishAlter();
}

void TCdcStreamRegistry::RemoveCdcStream(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    Y_ABORT_UNLESS(Self.PathsById.contains(streamPathId));

    const auto it = Self.CdcStreams.find(streamPathId);
    if (it == Self.CdcStreams.end()) {
        return;
    }

    const auto stream = it->second;
    Repository.DeleteCdcStream(db, streamPathId, bool(stream->AlterData));

    for (const auto& [shardIdx, _] : stream->ScanShards) {
        Self.RemoveCdcStreamScanShardStatus(db, streamPathId, shardIdx);
    }

    Self.CdcStreams.erase(streamPathId);
}

void TCdcStreamRegistry::PersistCdcStream(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    Y_ABORT_UNLESS(Self.PathsById.contains(streamPathId));
    auto path = Self.PathsById.at(streamPathId);
    const auto stream = GetCdcStream(streamPathId);

    Y_ABORT_UNLESS(Self.IsLocalId(streamPathId));
    Y_ABORT_UNLESS(path->IsCdcStream());

    auto alterData = stream->AlterData;
    Y_ABORT_UNLESS(alterData);
    Y_ABORT_UNLESS(stream->AlterVersion < alterData->AlterVersion);

    Repository.UpdateCdcStream(db, streamPathId, *alterData);
}

void TCdcStreamRegistry::PersistCdcStreamAlterData(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    Y_ABORT_UNLESS(Self.PathsById.contains(streamPathId));
    auto path = Self.PathsById.at(streamPathId);
    const auto stream = GetCdcStream(streamPathId);

    Y_ABORT_UNLESS(Self.IsLocalId(streamPathId));
    Y_ABORT_UNLESS(path->IsCdcStream());

    auto alterData = stream->AlterData;
    Y_ABORT_UNLESS(alterData);

    Repository.UpdateCdcStreamAlterData(db, streamPathId, *alterData);
}

std::pair<bool, THashMap<TPathId, TVector<TPathId>>> TCdcStreamRegistry::LoadCdcStreams(NIceDb::TNiceDb& db) {
    auto [streamsReady, streamRows] = Repository.LoadCdcStreams(db);
    if (!streamsReady) {
        return {false, {}};
    }

    auto [alterDataReady, alterDataRows] = Repository.LoadCdcStreamAlterData(db);
    if (!alterDataReady) {
        return {false, {}};
    }

    THashMap<TPathId, TVector<TPathId>> scansToResume;
    for (auto& row : streamRows) {
        const auto& pathId = row.PathId;

        Y_VERIFY_S(Self.PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
        auto path = Self.PathsById.at(pathId);

        Y_VERIFY_S(path->IsCdcStream(), "Path is not a cdc stream"
            << ", pathId: " << pathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(path->PathType));

        Y_ABORT_UNLESS(!Self.CdcStreams.contains(pathId));
        const auto& stream = Self.CdcStreams.Set(pathId, new TCdcStreamInfo(row.AlterVersion, std::move(row.Settings)));

        if (stream->State == NKikimrSchemeOp::ECdcStreamStateScan) {
            Y_VERIFY_S(Self.PathsById.contains(path->ParentPathId), "Parent path is not found"
                << ", cdc stream pathId: " << pathId
                << ", parent pathId: " << path->ParentPathId);
            scansToResume[path->ParentPathId].push_back(pathId);
        }
    }

    for (auto& row : alterDataRows) {
        const auto& pathId = row.PathId;

        Y_VERIFY_S(Self.PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
        auto path = Self.PathsById.at(pathId);

        Y_VERIFY_S(path->IsCdcStream(), "Path is not a cdc stream"
            << ", pathId: " << pathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(path->PathType));

        if (!Self.CdcStreams.contains(pathId)) {
            Y_ABORT_UNLESS(row.AlterVersion == 1);
            Self.CdcStreams.Set(pathId, TCdcStreamInfo::New(row.Settings));
        }

        auto stream = Self.CdcStreams.at(pathId);
        Y_ABORT_UNLESS(stream->AlterData == nullptr);
        Y_ABORT_UNLESS(stream->AlterVersion < row.AlterVersion);
        stream->AlterData = new TCdcStreamInfo(row.AlterVersion, std::move(row.Settings));

        Y_VERIFY_S(Self.PathsById.contains(path->ParentPathId), "Parent path is not found"
            << ", cdc stream pathId: " << pathId
            << ", parent pathId: " << path->ParentPathId);
        auto parent = Self.PathsById.at(path->ParentPathId);

        Y_VERIFY_S(parent->IsTable(), "Parent path is not a table"
            << ", cdc stream pathId: " << pathId
            << ", parent pathId: " << path->ParentPathId);
        Y_VERIFY_S(Self.Tables.contains(path->ParentPathId), "Parent path is not found in Tables map"
            << ", cdc stream pathId: " << pathId
            << ", parent pathId: " << path->ParentPathId);
    }

    return {true, std::move(scansToResume)};
}

TCdcStreamInfo::TPtr TCdcStreamRegistry::GetCdcStream(const TPathId& streamPathId) const {
    Y_ABORT_UNLESS(Self.CdcStreams.contains(streamPathId));
    return Self.CdcStreams.at(streamPathId);
}

NKikimrSchemeOp::TCdcStreamDescription TCdcStreamRegistry::MakeCdcStreamDescription(
        const TPathId& streamPathId,
        const TString& name,
        const TCdcStreamInfo& stream) const
{
    NKikimrSchemeOp::TCdcStreamDescription desc;
    desc.SetName(name);
    streamPathId.ToProto(desc.MutablePathId());
    stream.Serialize(desc);

    Y_ABORT_UNLESS(Self.PathsById.contains(streamPathId));
    auto path = Self.PathsById.at(streamPathId);

    for (const auto& [key, value] : path->UserAttrs->Attrs) {
        auto& attr = *desc.AddUserAttributes();
        attr.SetKey(key);
        attr.SetValue(value);
    }

    return desc;
}

} // namespace NKikimr::NSchemeShard::NCdc
