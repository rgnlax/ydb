#include "repository.h"

#include <ydb/core/tx/schemeshard/schemeshard_schema.h>

namespace NKikimr::NSchemeShard::NCdc {

namespace {

template <typename TTable, typename TRowset>
TCdcStreamRow ReadCdcStreamRow(TRowset& rowset) {
    return TCdcStreamRow{
        .PathId = TPathId(
            TOwnerId(rowset.template GetValue<typename TTable::OwnerPathId>()),
            TLocalPathId(rowset.template GetValue<typename TTable::LocalPathId>())
        ),
        .AlterVersion = rowset.template GetValue<typename TTable::AlterVersion>(),
        .Settings = TCdcStreamSettings()
            .WithMode(rowset.template GetValue<typename TTable::Mode>())
            .WithFormat(rowset.template GetValue<typename TTable::Format>())
            .WithVirtualTimestamps(rowset.template GetValueOrDefault<typename TTable::VirtualTimestamps>(false))
            .WithResolvedTimestamps(TDuration::MilliSeconds(rowset.template GetValueOrDefault<typename TTable::ResolvedTimestampsIntervalMs>(0)))
            .WithSchemaChanges(rowset.template GetValueOrDefault<typename TTable::SchemaChanges>(false))
            .WithAwsRegion(rowset.template GetValue<typename TTable::AwsRegion>())
            .WithState(rowset.template GetValue<typename TTable::State>())
            .WithUserSIDs(rowset.template GetValueOrDefault<typename TTable::UserSIDs>(false))
            .WithTraceIds(rowset.template GetValueOrDefault<typename TTable::TraceIds>(false))
            .WithCompression(rowset.template GetValueOrDefault<typename TTable::Compression>(NKikimrSchemeOp::ECdcStreamCompressionNone))
            .WithCompressionLevel(rowset.template GetValueOrDefault<typename TTable::CompressionLevel>(0))
            .WithHeartbeatMode(rowset.template GetValueOrDefault<typename TTable::HeartbeatMode>(NKikimrSchemeOp::ECdcStreamHeartbeatModeDirect)),
    };
}

template <typename TTable>
std::pair<bool, TVector<TCdcStreamRow>> LoadCdcStreamRows(NIceDb::TNiceDb& db) {
    auto rowset = db.Table<TTable>().Range().Select();
    if (!rowset.IsReady()) {
        return {false, {}};
    }

    TVector<TCdcStreamRow> rows;
    while (!rowset.EndOfSet()) {
        rows.push_back(ReadCdcStreamRow<TTable>(rowset));
        if (!rowset.Next()) {
            return {false, {}};
        }
    }
    return {true, std::move(rows)};
}

template <typename TTable>
void UpdateCdcStreamRow(NIceDb::TNiceDb& db, const TPathId& streamPathId, const TCdcStreamInfo& alterData) {
    db.Table<TTable>().Key(streamPathId.OwnerId, streamPathId.LocalPathId).Update(
        NIceDb::TUpdate<typename TTable::AlterVersion>(alterData.AlterVersion),
        NIceDb::TUpdate<typename TTable::Mode>(alterData.Mode),
        NIceDb::TUpdate<typename TTable::Format>(alterData.Format),
        NIceDb::TUpdate<typename TTable::VirtualTimestamps>(alterData.VirtualTimestamps),
        NIceDb::TUpdate<typename TTable::ResolvedTimestampsIntervalMs>(alterData.ResolvedTimestamps.MilliSeconds()),
        NIceDb::TUpdate<typename TTable::SchemaChanges>(alterData.SchemaChanges),
        NIceDb::TUpdate<typename TTable::AwsRegion>(alterData.AwsRegion),
        NIceDb::TUpdate<typename TTable::State>(alterData.State),
        NIceDb::TUpdate<typename TTable::UserSIDs>(alterData.UserSIDs),
        NIceDb::TUpdate<typename TTable::TraceIds>(alterData.TraceIds),
        NIceDb::TUpdate<typename TTable::Compression>(alterData.Compression),
        NIceDb::TUpdate<typename TTable::CompressionLevel>(alterData.CompressionLevel),
        NIceDb::TUpdate<typename TTable::HeartbeatMode>(alterData.HeartbeatMode)
    );
}

} // anonymous

void TCdcStreamRepository::UpdateCdcStream(
        NIceDb::TNiceDb& db,
        const TPathId& streamPathId,
        const TCdcStreamInfo& alterData) const
{
    UpdateCdcStreamRow<Schema::CdcStream>(db, streamPathId, alterData);
    db.Table<Schema::CdcStreamAlterData>().Key(streamPathId.OwnerId, streamPathId.LocalPathId).Delete();
}

void TCdcStreamRepository::UpdateCdcStreamAlterData(
        NIceDb::TNiceDb& db,
        const TPathId& streamPathId,
        const TCdcStreamInfo& alterData) const
{
    UpdateCdcStreamRow<Schema::CdcStreamAlterData>(db, streamPathId, alterData);
}

void TCdcStreamRepository::DeleteCdcStream(NIceDb::TNiceDb& db, const TPathId& streamPathId, bool hasAlterData) const {
    if (hasAlterData) {
        db.Table<Schema::CdcStreamAlterData>().Key(streamPathId.OwnerId, streamPathId.LocalPathId).Delete();
    }

    db.Table<Schema::CdcStream>().Key(streamPathId.OwnerId, streamPathId.LocalPathId).Delete();
}

std::pair<bool, TVector<TCdcStreamRow>> TCdcStreamRepository::LoadCdcStreams(NIceDb::TNiceDb& db) const {
    return LoadCdcStreamRows<Schema::CdcStream>(db);
}

std::pair<bool, TVector<TCdcStreamRow>> TCdcStreamRepository::LoadCdcStreamAlterData(NIceDb::TNiceDb& db) const {
    return LoadCdcStreamRows<Schema::CdcStreamAlterData>(db);
}

} // namespace NKikimr::NSchemeShard::NCdc
