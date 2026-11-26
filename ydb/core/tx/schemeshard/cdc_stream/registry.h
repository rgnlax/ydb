#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <optional>

namespace NKikimr::NSchemeShard {

class TSchemeShard;
struct TOperationContext;

}

namespace NKikimr::NSchemeShard::NCdc {

    class TCdcStreamRegistry {
    public:
        explicit TCdcStreamRegistry(TSchemeShard& self);

        bool HasCdcStream(const TPathId& streamPathId) const;
        bool HasCdcStreamAlter(const TPathId& streamPathId) const;
        std::optional<TCdcStreamSettings> FindCdcStreamSettings(const TPathId& streamPathId) const;
        ui64 GetCdcStreamVersion(const TPathId& streamPathId) const;
        std::optional<NKikimrSchemeOp::ECdcStreamHeartbeatMode> FindPendingCdcStreamHeartbeatMode(
            const TPathId& streamPathId) const;

        NKikimrSchemeOp::TCdcStreamDescription MakeCdcStreamDescription(
            const TPathId& streamPathId,
            const TString& name) const;
        NKikimrSchemeOp::TCdcStreamDescription MakeCdcStreamAlterDescription(
            const TPathId& streamPathId,
            const TString& name) const;

        void AddCdcStream(
            TOperationContext& context,
            const TPathId& streamPathId,
            TCdcStreamInfo::TPtr stream);
        TCdcStreamInfo::TPtr BeginCdcStreamAlter(
            TOperationContext& context,
            const TPathId& streamPathId);
        void FinishCdcStreamCreate(NIceDb::TNiceDb& db, const TPathId& streamPathId);
        void FinishCdcStreamAlter(NIceDb::TNiceDb& db, const TPathId& streamPathId);
        void RemoveCdcStream(NIceDb::TNiceDb& db, const TPathId& streamPathId);

        void PersistCdcStream(NIceDb::TNiceDb& db, const TPathId& streamPathId);
        void PersistCdcStreamAlterData(NIceDb::TNiceDb& db, const TPathId& streamPathId);
        std::pair<bool, THashMap<TPathId, TVector<TPathId>>> LoadCdcStreams(NIceDb::TNiceDb& db);

    private:
        TCdcStreamInfo::TPtr GetCdcStream(const TPathId& streamPathId) const;
        NKikimrSchemeOp::TCdcStreamDescription MakeCdcStreamDescription(
            const TPathId& streamPathId,
            const TString& name,
            const TCdcStreamInfo& stream) const;

        TSchemeShard& Self;
    };

} // namespace NKikimr::NSchemeShard::NCdc
