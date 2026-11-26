#pragma once

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <optional>

namespace NKikimr::NSchemeShard {

class TPath;
struct TOperationContext;

}

namespace NKikimr::NSchemeShard::NCdc {

    struct TCdcPqPartParams {
        ui32 TotalGroupCount = 0;
        ui32 PartitionPerTablet = 2;
        bool ReplicationAutoPartitioning = false;
        bool KeyRangeAutoPartitioning = false;
        ui32 MinPartitionCount = 0;
        ui32 MaxPartitionCount = 0;
    };

    // Decides the changefeed topic shape from the source table size.
    // Replication autopartitioning caps max at maxShardsInPath and min at that limit / 4.
    // An explicit TopicPartitions stays the initial count and the strategy minimum, capped by that max.
    TCdcPqPartParams MakeCdcPqPartParams(
        const NKikimrSchemeOp::TCreateCdcStream& op,
        ui64 tablePartitionCount,
        ui64 maxShardsInPath,
        const TFeatureFlags& featureFlags);

    std::optional<NKikimrPQ::TBootstrapConfig> MakeCdcStreamTopicBootstrapConfig(
        const TOperationContext& context,
        const TPath& topicPath);

} // namespace NKikimr::NSchemeShard::NCdc
