#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

}

namespace NKikimr::NSchemeShard::NCdc {

    class TCdcStreams {
    public:
        explicit TCdcStreams(TSchemeShard& self);

        TIntrusiveConstPtr<TCdcStreamInfo> Find(const TPathId& streamPathId) const;
        TVector<TPathId> List() const;
        NKikimrSchemeOp::TCdcStreamDescription Describe(const TPathId& streamPathId, const TString& name) const;

        std::pair<bool, THashMap<TPathId, TVector<TPathId>>> Load(NIceDb::TNiceDb& db);
        void Persist(NIceDb::TNiceDb& db, const TPathId& streamPathId);
        void PersistAlterData(NIceDb::TNiceDb& db, const TPathId& streamPathId);

    private:
        TSchemeShard& Self;
    };

} // namespace NKikimr::NSchemeShard::NCdc
