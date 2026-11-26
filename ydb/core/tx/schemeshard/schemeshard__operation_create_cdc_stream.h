#pragma once

#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_cdc_stream_topic_pipeline.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <optional>

namespace NKikimr::NSchemeShard::NCdc {

struct TStreamPaths {
    TPath TablePath;
    TPath StreamPath;
};

std::variant<TStreamPaths, ISubOperation::TPtr> DoNewStreamPathChecks(
    const TOperationContext& context,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TString& tableName,
    const TString& streamName,
    bool acceptExisted,
    bool internal = false);

void DoCreateStreamImpl(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TOperationId& opId,
    const TPath& tablePath,
    const bool acceptExisted,
    const bool initialScan,
    const bool internal = false);

void DoCreateStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath,
    const bool acceptExisted,
    const bool initialScan,
    const bool internal = false);

void DoCreatePqPart(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TOperationId& opId,
    const TPath& streamPath,
    const TString& streamName,
    TTableInfo::TCPtr table,
    const TVector<TString>& boundaries,
    const bool acceptExisted);

} // namespace NKikimr::NSchemesShard::NCdc
