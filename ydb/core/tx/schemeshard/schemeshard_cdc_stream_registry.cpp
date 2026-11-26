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
    Self.PersistCdcStream(db, streamPathId);
    Self.CdcStreams.Set(streamPathId, stream->AlterData);
}

void TCdcStreamRegistry::FinishCdcStreamAlter(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    Self.PersistCdcStream(db, streamPathId);
    GetCdcStream(streamPathId)->FinishAlter();
}

void TCdcStreamRegistry::RemoveCdcStream(NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    Self.PersistRemoveCdcStream(db, streamPathId);
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
