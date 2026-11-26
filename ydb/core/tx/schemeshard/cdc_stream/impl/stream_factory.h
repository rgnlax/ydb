#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard::NCdc {

    std::pair<TCdcStreamInfo::TPtr, TString> MakeCdcStream(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags);

} // namespace NKikimr::NSchemeShard::NCdc
