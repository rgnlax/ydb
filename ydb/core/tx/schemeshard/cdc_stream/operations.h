#pragma once

#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <variant>

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

std::variant<TStreamPaths, ISubOperation::TPtr> DoAlterStreamPathChecks(
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TString& tableName,
    const TString& streamName);

void DoAlterStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TAlterCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath);

std::variant<TStreamPaths, ISubOperation::TPtr> DoDropStreamPathChecks(
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TString& tableName,
    const TString& streamName);

ISubOperation::TPtr DoDropStreamChecks(
    const TOperationId& opId,
    const TPath& tablePath,
    const TTxId lockTxId,
    TOperationContext& context);

void DoDropStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TDropCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath,
    const TVector<TPath>& streamPaths,
    const TTxId lockTxId,
    TOperationContext& context);

void DoRotateStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TRotateCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath);

} // namespace NKikimr::NSchemeShard::NCdc
