#pragma once

#include <ydb/core/tx/schemeshard/cdc_stream/features/features.h>
#include <ydb/core/tx/schemeshard/cdc_stream/topic.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NSchemeShard::NCdc {

    TCdcPqPartParams MakeCdcStreamTopicShape(
        const TCdcStreamFeatures& features,
        const TCdcStreamRequest& request,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath);

    NKikimrSchemeOp::TPersQueueGroupDescription MakeCdcStreamTopic(
        const TCdcStreamFeatures& features,
        const TCdcStreamRequest& request,
        const TString& streamName,
        const TString& topicPath,
        const TTableInfo& table,
        const TVector<TString>& boundaries,
        ui64 maxShardsInPath);

    NKikimrPQ::TBootstrapConfig MakeCdcStreamTopicSources(
        const TCdcStreamFeatures& features,
        const TCdcStreamInfo* stream,
        const TTableInfo& table,
        const THashMap<TShardIdx, TShardInfo>& shardInfos);

} // namespace NKikimr::NSchemeShard::NCdc
