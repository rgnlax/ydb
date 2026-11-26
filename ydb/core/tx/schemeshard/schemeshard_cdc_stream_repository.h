#pragma once

#include "schemeshard_info_types.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NSchemeShard::NCdc {

    struct TCdcStreamRow {
        TPathId PathId;
        ui64 AlterVersion = 0;
        TCdcStreamSettings Settings;
    };

    class TCdcStreamRepository {
    public:
        void UpdateCdcStream(NIceDb::TNiceDb& db, const TPathId& streamPathId, const TCdcStreamInfo& alterData) const;
        void UpdateCdcStreamAlterData(NIceDb::TNiceDb& db, const TPathId& streamPathId, const TCdcStreamInfo& alterData) const;
        void DeleteCdcStream(NIceDb::TNiceDb& db, const TPathId& streamPathId, bool hasAlterData) const;

        std::pair<bool, TVector<TCdcStreamRow>> LoadCdcStreams(NIceDb::TNiceDb& db) const;
        std::pair<bool, TVector<TCdcStreamRow>> LoadCdcStreamAlterData(NIceDb::TNiceDb& db) const;
    };

} // namespace NKikimr::NSchemeShard::NCdc
