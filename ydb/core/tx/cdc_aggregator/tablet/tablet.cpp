#include "tablet.h"
#include "tablet_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include <ydb/library/actors/core/log.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

TCdcAggregator::TCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
}

void TCdcAggregator::OnDetach(const TActorContext& ctx) {
    ClosePublicationPipes(ctx);
    Die(ctx);
}

void TCdcAggregator::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    ClosePublicationPipes(ctx);
    Die(ctx);
}

void TCdcAggregator::OnActivateExecutor(const TActorContext& ctx) {
    RunTxInitSchema(ctx);
}

void TCdcAggregator::DefaultSignalTabletActive(const TActorContext&) {
}

STFUNC(TCdcAggregator::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TCdcAggregator::StateWork) {
    YDB_LOG_CREATE_CONTEXT({"tabletId", TabletID()});
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvCdcAggregator::TEvConfigureTracking, Handle);
        HFunc(TEvCdcAggregator::TEvWriteHeartbeats, Handle);
        HFunc(TEvCdcAggregator::TEvPartitioningChanged, Handle);
        HFunc(TEvCdcAggregator::TEvDescribeTracking, Handle);
        HFunc(TEvPersQueue::TEvResponse, Handle);
        HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFunc(TEvents::TEvWakeup, Handle);
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            YDB_LOG_WARN("Unexpected event",
                        {"eventType", ev->GetTypeRewrite()});
        }
    }
}

void TCdcAggregator::SwitchToWork(const TActorContext& ctx) {
    SignalTabletActive(ctx);
    Become(&TThis::StateWork);
    CommittedHeartbeat = Stream.GetCurrentHeartbeat();
    PublishHeartbeat(ctx);
}

void TCdcAggregator::PersistStreamChanges(NIceDb::TNiceDb& db, const TStreamChanges& changes) {
    for (const auto dataShardId : changes.ErasedSources) {
        db.Table<Schema::Sources>().Key(dataShardId).Delete();
    }
    for (const auto dataShardId : changes.UpsertedSources) {
        const auto& version = Stream.GetSources().at(dataShardId);
        if (version) {
            db.Table<Schema::Sources>().Key(dataShardId).Update(
                NIceDb::TUpdate<Schema::Sources::LastHeartbeatStep>(version->Step),
                NIceDb::TUpdate<Schema::Sources::LastHeartbeatTxId>(version->TxId));
        } else {
            db.Table<Schema::Sources>().Key(dataShardId).Update();
        }
    }
    for (const auto& version : changes.ErasedHeartbeats) {
        db.Table<Schema::Heartbeats>().Key(version.Step, version.TxId).Delete();
    }
    for (const auto& [version, data] : changes.UpsertedHeartbeats) {
        db.Table<Schema::Heartbeats>().Key(version.Step, version.TxId).Update(
            NIceDb::TUpdate<Schema::Heartbeats::Data>(data));
    }
    if (changes.CurrentHeartbeatChanged) {
        const auto& heartbeat = *Stream.GetCurrentHeartbeat();
        db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatStep>(heartbeat.Version.Step),
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatTxId>(heartbeat.Version.TxId),
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatData>(heartbeat.Data));
    }
}

void TCdcAggregator::Handle(TEvCdcAggregator::TEvDescribeTracking::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"ev", ev->Get()->ToString()});

    TVector<ui64> sources;
    for (const auto& [dataShardId, heartbeat] : Stream.GetSources()) {
        sources.push_back(dataShardId);
    }
    std::sort(sources.begin(), sources.end());

    auto result = MakeHolder<TEvCdcAggregator::TEvDescribeTrackingResult>();
    if (CommittedHeartbeat) {
        CommittedHeartbeat->Version.ToProto(result->Record.MutableCurrentHeartbeat());
    }
    for (const auto dataShardId : sources) {
        auto& item = *result->Record.AddSources();
        item.SetDataShardId(dataShardId);
        if (const auto& heartbeat = Stream.GetSources().at(dataShardId)) {
            heartbeat->ToProto(item.MutableLastHeartbeat());
        }
    }
    ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
}

IActor* CreateCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TCdcAggregator(tablet, info);
}

} // namespace NKikimr::NCdcAggregator
