#include "replication_capabilities.h"

#include <ydb/core/tx/schemeshard/user_attributes.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr::NSchemeShard::NCdc {

bool IsReplicationSupport(const NKikimrSchemeOp::TCdcStreamDescription& streamDesc, const TString& capability) {
    for (auto& attribute : streamDesc.GetUserAttributes()) {
        if (attribute.GetKey() == ATTR_ASYNC_REPLICATION) {
            if (!attribute.HasValue()) {
                break;
            }

            NJson::TJsonValue result;
            if (!NJson::ReadJsonFastTree(attribute.GetValue(), &result)) {
                break;
            }

            auto map = result.GetMap();
            return map[capability].GetBoolean();
        }
    }

    return false;
}

} // namespace NKikimr::NSchemeShard::NCdc
