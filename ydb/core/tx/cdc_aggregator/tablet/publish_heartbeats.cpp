#include "tablet_impl.h"

#include <ydb/core/base/tablet_pipe.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

void TCdcAggregator::CommitHeartbeat(const NPQ::THeartbeat& heartbeat, const TActorContext& ctx) {
    if (!CommittedHeartbeat || heartbeat.Version > CommittedHeartbeat->Version) {
        CommittedHeartbeat = heartbeat;
        PublishHeartbeat(ctx);
    }
}

void TCdcAggregator::PublishHeartbeat(const TActorContext& ctx) {
    if (!CommittedHeartbeat) {
        return;
    }

    for (const auto& partition : Stream.GetConfig().TargetPartitions) {
        SendHeartbeat(partition, ctx);
    }

    SchedulePublicationRetry(ctx);
}

void TCdcAggregator::SendHeartbeat(const TTargetPartition& partition, const TActorContext& ctx) {
    auto& acknowledged = AcknowledgedHeartbeats.try_emplace(partition.PartitionId, TRowVersion::Min()).first->second;
    if (Publications.contains(partition.PartitionId) || acknowledged >= CommittedHeartbeat->Version) {
        return;
    }

    auto& client = PublicationPipes[partition.TabletId];
    if (!client) {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {.RetryLimitCount = 3};
        client = ctx.Register(NTabletPipe::CreateClient(SelfId(), partition.TabletId, config));
    }

    auto request = MakeHolder<TEvPersQueue::TEvRequest>();
    auto& body = *request->Record.MutablePartitionRequest();
    body.SetTopic(Stream.GetConfig().StreamPath);
    body.SetPartition(partition.PartitionId);
    body.SetCookie(ui64(partition.PartitionId) + 1);
    CommittedHeartbeat->Serialize(*body.MutableCmdEmitHeartbeat()->MutableHeartbeat());
    Publications.emplace(partition.PartitionId, TPublication{
        .TabletId = partition.TabletId,
        .Version = CommittedHeartbeat->Version,
        .Deadline = ctx.Now() + TDuration::Seconds(10),
    });
    NTabletPipe::SendData(ctx, client, request.Release());
}

void TCdcAggregator::SchedulePublicationRetry(const TActorContext& ctx) {
    if (!PublicationRetryScheduled && !Publications.empty()) {
        PublicationRetryScheduled = true;
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }
}

void TCdcAggregator::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& response = ev->Get()->Record.GetPartitionResponse();
    if (!response.HasCookie() || !response.GetCookie()) {
        return;
    }
    const auto partitionId = ui32(response.GetCookie() - 1);
    auto pending = Publications.find(partitionId);
    if (pending == Publications.end()) {
        return;
    }

    if (!response.HasCmdEmitHeartbeatResult()) {
        YDB_LOG_WARN("Heartbeat publication failed",
            {"tabletId", TabletID()},
            {"partitionId", partitionId},
            {"response", ev->Get()->Record.ShortDebugString()});
        pending->second.Deadline = ctx.Now();
        SchedulePublicationRetry(ctx);
        return;
    }

    const auto& result = response.GetCmdEmitHeartbeatResult();
    const TRowVersion written(result.GetStep(), result.GetTxId());
    if (written < pending->second.Version) {
        return;
    }

    const auto tabletId = pending->second.TabletId;
    auto& acknowledged = AcknowledgedHeartbeats.at(partitionId);
    acknowledged = Max(acknowledged, written);
    Publications.erase(pending);
    SendHeartbeat(TTargetPartition{.PartitionId = partitionId, .TabletId = tabletId}, ctx);
    SchedulePublicationRetry(ctx);
}

void TCdcAggregator::Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    PublicationRetryScheduled = false;
    TVector<TTargetPartition> retry;
    for (auto pending = Publications.begin(); pending != Publications.end();) {
        if (pending->second.Deadline <= ctx.Now()) {
            retry.push_back({.PartitionId = pending->first, .TabletId = pending->second.TabletId});
            Publications.erase(pending++);
        } else {
            ++pending;
        }
    }
    for (const auto& partition : retry) {
        SendHeartbeat(partition, ctx);
    }
    SchedulePublicationRetry(ctx);
}

void TCdcAggregator::ResetPublicationPipe(ui64 tabletId, const TActorId& client, const TActorContext& ctx) {
    const auto pipe = PublicationPipes.find(tabletId);
    if (pipe == PublicationPipes.end() || pipe->second != client) {
        return;
    }

    PublicationPipes.erase(pipe);
    for (auto& [partitionId, publication] : Publications) {
        if (publication.TabletId == tabletId) {
            publication.Deadline = ctx.Now();
        }
    }
    SchedulePublicationRetry(ctx);
}

void TCdcAggregator::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        ResetPublicationPipe(ev->Get()->TabletId, ev->Get()->ClientId, ctx);
    }
}

void TCdcAggregator::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    ResetPublicationPipe(ev->Get()->TabletId, ev->Get()->ClientId, ctx);
}

void TCdcAggregator::ClosePublicationPipes(const TActorContext& ctx) {
    for (const auto& [tabletId, client] : PublicationPipes) {
        NTabletPipe::CloseClient(ctx, client);
    }
}

}
