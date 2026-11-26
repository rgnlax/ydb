#include <ydb/core/tx/schemeshard/cdc_stream/streams.h>

#include "repository.h"
#include "stream_changes.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NCdc {

namespace {

const TCdcStreamRepository Repository;

} // anonymous

TCdcStreams::TCdcStreams(TSchemeShard& self)
    : Self(self)
{
}

TIntrusiveConstPtr<TCdcStreamInfo> TCdcStreams::Find(const TPathId& streamPathId) const {
    const auto it = Self.CdcStreamInfos.find(streamPathId);
    if (it == Self.CdcStreamInfos.end()) {
        return nullptr;
    }
    return it->second;
}

TVector<TPathId> TCdcStreams::List() const {
    TVector<TPathId> streamPathIds;
    streamPathIds.reserve(Self.CdcStreamInfos.size());
    for (const auto& [streamPathId, _] : Self.CdcStreamInfos) {
        streamPathIds.push_back(streamPathId);
    }
    return streamPathIds;
}

NKikimrSchemeOp::TCdcStreamDescription TCdcStreams::Describe(const TPathId& streamPathId, const TString& name) const {
    Y_VERIFY_S(Self.CdcStreamInfos.contains(streamPathId), "Cdc stream not found"
        << ": pathId# " << streamPathId
        << ", name# " << name);
    return MakeCdcStreamDescription(Self, streamPathId, name, *Self.CdcStreamInfos.at(streamPathId));
}

void TCdcStreams::Persist(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    Y_ABORT_UNLESS(Self.PathsById.contains(streamPathId));
    auto path = Self.PathsById.at(streamPathId);

    Y_ABORT_UNLESS(Self.CdcStreamInfos.contains(streamPathId));
    auto stream = Self.CdcStreamInfos.at(streamPathId);

    Y_ABORT_UNLESS(Self.IsLocalId(streamPathId));
    Y_ABORT_UNLESS(path->IsCdcStream());

    auto alterData = stream->AlterData;
    Y_ABORT_UNLESS(alterData);
    Y_ABORT_UNLESS(stream->AlterVersion < alterData->AlterVersion);

    Repository.UpdateCdcStream(db, streamPathId, *alterData);
}

void TCdcStreams::PersistAlterData(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    Y_ABORT_UNLESS(Self.PathsById.contains(streamPathId));
    auto path = Self.PathsById.at(streamPathId);

    Y_ABORT_UNLESS(Self.CdcStreamInfos.contains(streamPathId));
    auto stream = Self.CdcStreamInfos.at(streamPathId);

    Y_ABORT_UNLESS(Self.IsLocalId(streamPathId));
    Y_ABORT_UNLESS(path->IsCdcStream());

    auto alterData = stream->AlterData;
    Y_ABORT_UNLESS(alterData);

    Repository.UpdateCdcStreamAlterData(db, streamPathId, *alterData);
}

std::pair<bool, THashMap<TPathId, TVector<TPathId>>> TCdcStreams::Load(NIceDb::TNiceDb& db) {
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

        Y_ABORT_UNLESS(!Self.CdcStreamInfos.contains(pathId));
        const auto& stream = Self.CdcStreamInfos.Set(pathId, new TCdcStreamInfo(row.AlterVersion, std::move(row.Settings)));

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

        if (!Self.CdcStreamInfos.contains(pathId)) {
            Y_ABORT_UNLESS(row.AlterVersion == 1);
            Self.CdcStreamInfos.Set(pathId, TCdcStreamInfo::New(row.Settings));
        }

        auto stream = Self.CdcStreamInfos.at(pathId);
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

} // namespace NKikimr::NSchemeShard::NCdc
