#pragma once

#include "schema.h"
#include "stream.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/counters_cdc_aggregator.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NCdcAggregator {

class TCdcAggregator
    : public TActor<TCdcAggregator>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
    using Schema = TCdcAggregatorSchema;
    using TTxBase = NTabletFlatExecutor::TTransactionBase<TCdcAggregator>;

    class TTxInitSchema;
    class TTxInit;
    class TTxConfigureTracking;
    class TTxPartitioningChanged;
    class TTxWriteHeartbeats;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_TABLET;
    }

    TCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info);

private:
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;

    void RunTxInitSchema(const TActorContext& ctx);
    void RunTxInit(const TActorContext& ctx);
    void SwitchToWork(const TActorContext& ctx);

    void PersistStreamChanges(NIceDb::TNiceDb& db, const TStreamChanges& changes);

    void Handle(TEvCdcAggregator::TEvConfigureTracking::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvPartitioningChanged::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvDescribeTracking::TPtr& ev, const TActorContext& ctx);

    void CommitHeartbeat(const NPQ::THeartbeat& heartbeat, const TActorContext& ctx);
    void PublishHeartbeat(const TActorContext& ctx);
    void SendHeartbeat(const TTargetPartition& partition, const TActorContext& ctx);
    void SchedulePublicationRetry(const TActorContext& ctx);
    void ResetPublicationPipe(ui64 tabletId, const TActorId& client, const TActorContext& ctx);
    void ClosePublicationPipes(const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    STFUNC(StateInit);
    STFUNC(StateWork);

private:
    struct TPublication {
        ui64 TabletId;
        TRowVersion Version;
        TInstant Deadline;
    };

    TStream Stream;
    std::optional<NPQ::THeartbeat> CommittedHeartbeat;
    THashMap<ui32, TPublication> Publications;
    THashMap<ui32, TRowVersion> AcknowledgedHeartbeats;
    THashMap<ui64, TActorId> PublicationPipes;
    bool PublicationRetryScheduled = false;
};

} // namespace NKikimr::NCdcAggregator
