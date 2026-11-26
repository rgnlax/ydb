#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard::NCdc {

    bool IsReplicationSupport(const NKikimrSchemeOp::TCdcStreamDescription& streamDesc, const TString& capability);

} // namespace NKikimr::NSchemeShard::NCdc
