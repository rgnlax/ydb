#include "stream_factory.h"

#include <ydb/core/tx/schemeshard/cdc_stream/features/features.h>

namespace NKikimr::NSchemeShard::NCdc {

std::pair<TCdcStreamInfo::TPtr, TString> MakeCdcStream(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        const TFeatureFlags& featureFlags)
{
    auto stream = TCdcStreamInfo::Create(op.GetStreamDescription());
    Y_ABORT_UNLESS(stream);

    for (const auto& feature : MakeCdcStreamFeatures(featureFlags)) {
        if (auto error = feature->OnCreateStream(op, *stream->AlterData); !error.empty()) {
            return {nullptr, std::move(error)};
        }
    }

    return {stream, TString()};
}

} // namespace NKikimr::NSchemeShard::NCdc
