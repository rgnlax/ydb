#pragma once

#include "schemeshard_cdc_stream_topic_partitioning.h"
#include "schemeshard_info_types.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <memory>
#include <optional>

namespace NKikimr::NSchemeShard {

class TPath;
struct TOperationContext;

}

namespace NKikimr::NSchemeShard::NCdc {

    class TCdcStreamTopicPipeline {
    public:
        explicit TCdcStreamTopicPipeline(std::unique_ptr<ICdcStreamTopicPartitioning> partitioning);

        NKikimrSchemeOp::TPersQueueGroupDescription MakePersQueueGroup(
            const NKikimrSchemeOp::TCreateCdcStream& op,
            const TString& streamName,
            const TString& topicPath,
            const TTableInfo& table,
            const TVector<TString>& boundaries,
            ui64 maxShardsInPath) const;

    private:
        std::unique_ptr<ICdcStreamTopicPartitioning> Partitioning;
    };

    NKikimrPQ::TBootstrapConfig MakeCdcStreamTopicSources(
        const TTableInfo& table,
        const THashMap<TShardIdx, TShardInfo>& shardInfos,
        bool splitByTableKey);

    std::optional<NKikimrPQ::TBootstrapConfig> MakeCdcStreamTopicBootstrapConfig(
        const TOperationContext& context,
        const TPath& topicPath);

} // namespace NKikimr::NSchemeShard::NCdc
