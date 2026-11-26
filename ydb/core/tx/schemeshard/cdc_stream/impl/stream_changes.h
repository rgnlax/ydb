#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;
struct TOperationContext;

}

namespace NKikimr::NSchemeShard::NCdc {

    void AddCdcStream(TOperationContext& context, const TPathId& streamPathId, TCdcStreamInfo::TPtr stream);
    TCdcStreamInfo::TPtr BeginCdcStreamAlter(TOperationContext& context, const TPathId& streamPathId);
    void FinishCdcStreamCreate(TSchemeShard& ss, NIceDb::TNiceDb& db, const TPathId& streamPathId);

    TCdcStreamInfo::TPtr FindPendingCdcStream(TSchemeShard& ss, const TPathId& streamPathId);

    NKikimrSchemeOp::TCdcStreamDescription MakeCdcStreamDescription(
        TSchemeShard& ss,
        const TPathId& streamPathId,
        const TString& name,
        const TCdcStreamInfo& stream);
    NKikimrSchemeOp::TCdcStreamDescription DescribeCdcStreamAlter(
        TSchemeShard& ss,
        const TPathId& streamPathId,
        const TString& name);

} // namespace NKikimr::NSchemeShard::NCdc
