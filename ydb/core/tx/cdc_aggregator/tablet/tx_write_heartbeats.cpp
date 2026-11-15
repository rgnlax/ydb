#include "tablet_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxWriteHeartbeats : public TTxBase {
    using TResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

    TEvCdcAggregator::TEvWriteHeartbeats::TPtr Ev;
    THolder<TEvCdcAggregator::TEvWriteHeartbeatsResult> Result;
    std::optional<NPQ::THeartbeat> CommittedHeartbeat;

public:
    TTxWriteHeartbeats(TCdcAggregator* self, TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev)
        : TTxBase(self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_WRITE_HEARTBEATS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Result = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeatsResult>();
        Result->Record.SetOrigin(Self->TabletID());
        if (!Self->Stream.IsConfigured()) {
            Result->Record.SetStatus(TResult::NOT_READY);
            return true;
        }
        if (Ev->Get()->Record.HeartbeatsSize() == 0) {
            Result->Record.SetStatus(TResult::BAD_REQUEST);
            Result->Record.SetError("Heartbeat batch is empty");
            return true;
        }

        Result->Record.SetStatus(TResult::SUCCESS);
        TStreamChanges changes;
        for (const auto& input : Ev->Get()->Record.GetHeartbeats()) {
            auto& item = *Result->Record.AddResults();
            item.SetSourceDataShardId(input.GetSourceDataShardId());
            if (!input.GetSourceDataShardId() || !input.HasHeartbeat()
                    || !input.GetHeartbeat().HasStep() || !input.GetHeartbeat().HasTxId()
                    || input.GetHeartbeat().GetData().empty()) {
                item.SetStatus(TResult::THeartbeatResult::BAD_REQUEST);
                continue;
            }

            const auto heartbeat = NPQ::THeartbeat::Parse(input.GetHeartbeat());
            if (Self->Stream.WriteHeartbeat(input.GetSourceDataShardId(), heartbeat, changes)
                    == ESourceOutcome::UnknownSource) {
                item.SetStatus(TResult::THeartbeatResult::UNKNOWN_SOURCE);
                continue;
            }

            item.SetStatus(TResult::THeartbeatResult::SUCCESS);
            Self->Stream.GetSources().at(input.GetSourceDataShardId())->ToProto(item.MutableWrittenVersion());
        }

        if (!changes.IsEmpty()) {
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistStreamChanges(db, changes);
        }
        if (changes.CurrentHeartbeatChanged) {
            CommittedHeartbeat = Self->Stream.GetCurrentHeartbeat();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        if (CommittedHeartbeat) {
            Self->CommitHeartbeat(*CommittedHeartbeat, ctx);
        }
    }
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxWriteHeartbeats(this, ev), ctx);
}

}
