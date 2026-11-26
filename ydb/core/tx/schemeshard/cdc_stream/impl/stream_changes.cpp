#include "stream_changes.h"

#include "repository.h"

#include <ydb/core/tx/schemeshard/cdc_stream/at_table.h>
#include <ydb/core/tx/schemeshard/cdc_stream/streams.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NCdc {

namespace {

TCdcStreamInfo::TPtr GetCdcStream(TSchemeShard& ss, const TPathId& streamPathId) {
    Y_ABORT_UNLESS(ss.CdcStreamInfos.contains(streamPathId));
    return ss.CdcStreamInfos.at(streamPathId);
}

} // anonymous

void AddCdcStream(TOperationContext& context, const TPathId& streamPathId, TCdcStreamInfo::TPtr stream) {
    context.MemChanges.GrabNewCdcStream(context.SS, streamPathId);
    context.DbChanges.PersistAlterCdcStream(streamPathId);
    context.SS->CdcStreamInfos.Set(streamPathId, stream);
}

TCdcStreamInfo::TPtr BeginCdcStreamAlter(TOperationContext& context, const TPathId& streamPathId) {
    context.MemChanges.GrabCdcStream(context.SS, streamPathId);
    context.DbChanges.PersistAlterCdcStream(streamPathId);

    auto streamAlter = GetCdcStream(*context.SS, streamPathId)->CreateNextVersion();
    Y_ABORT_UNLESS(streamAlter);
    return streamAlter;
}

void FinishCdcStreamCreate(TSchemeShard& ss, NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    const auto stream = GetCdcStream(ss, streamPathId);
    ss.CdcStreams.Persist(db, streamPathId);
    ss.CdcStreamInfos.Set(streamPathId, stream->AlterData);
}

TCdcStreamInfo::TPtr FindPendingCdcStream(TSchemeShard& ss, const TPathId& streamPathId) {
    const auto it = ss.CdcStreamInfos.find(streamPathId);
    if (it == ss.CdcStreamInfos.end()) {
        return nullptr;
    }
    return it->second->AlterData ? it->second->AlterData : it->second;
}

NKikimrSchemeOp::TCdcStreamDescription MakeCdcStreamDescription(
        TSchemeShard& ss,
        const TPathId& streamPathId,
        const TString& name,
        const TCdcStreamInfo& stream)
{
    NKikimrSchemeOp::TCdcStreamDescription desc;
    desc.SetName(name);
    streamPathId.ToProto(desc.MutablePathId());
    stream.Serialize(desc);

    Y_ABORT_UNLESS(ss.PathsById.contains(streamPathId));
    auto path = ss.PathsById.at(streamPathId);

    for (const auto& [key, value] : path->UserAttrs->Attrs) {
        auto& attr = *desc.AddUserAttributes();
        attr.SetKey(key);
        attr.SetValue(value);
    }

    return desc;
}

NKikimrSchemeOp::TCdcStreamDescription DescribeCdcStreamAlter(
        TSchemeShard& ss,
        const TPathId& streamPathId,
        const TString& name)
{
    const auto stream = GetCdcStream(ss, streamPathId);
    Y_VERIFY_S(stream->AlterData, "Empty cdc stream info"
        << ": pathId# " << streamPathId
        << ", name# " << name);
    return MakeCdcStreamDescription(ss, streamPathId, name, *stream->AlterData);
}

} // namespace NKikimr::NSchemeShard::NCdc

namespace NKikimr::NSchemeShard::NCdcStreamAtTable {

void FinishCdcStreamAlter(TSchemeShard& ss, NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    ss.CdcStreams.Persist(db, streamPathId);
    NCdc::GetCdcStream(ss, streamPathId)->FinishAlter();
}

void RemoveCdcStream(TSchemeShard& ss, NIceDb::TNiceDb& db, const TPathId& streamPathId) {
    Y_ABORT_UNLESS(ss.PathsById.contains(streamPathId));

    const auto it = ss.CdcStreamInfos.find(streamPathId);
    if (it == ss.CdcStreamInfos.end()) {
        return;
    }

    const auto stream = it->second;
    NCdc::TCdcStreamRepository().DeleteCdcStream(db, streamPathId, bool(stream->AlterData));

    for (const auto& [shardIdx, _] : stream->ScanShards) {
        ss.RemoveCdcStreamScanShardStatus(db, streamPathId, shardIdx);
    }

    ss.CdcStreamInfos.erase(streamPathId);
}

} // namespace NKikimr::NSchemeShard::NCdcStreamAtTable
