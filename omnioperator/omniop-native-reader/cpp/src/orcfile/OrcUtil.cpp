#include "OrcUtil.h"

namespace omniruntime::reader {
    omniruntime::type::DataTypeId OrcUtil::tranOrcTypeToOmniType(const orc::Type *type) {
        constexpr int32_t OMNI_MAX_DECIMAL64_DIGITS = 18;
        switch (type->getKind()) {
            case orc::TypeKind::BOOLEAN:
                return omniruntime::type::OMNI_BOOLEAN;
            case orc::TypeKind::SHORT:
                return omniruntime::type::OMNI_SHORT;
            case orc::TypeKind::DATE:
                return omniruntime::type::OMNI_DATE32;
            case orc::TypeKind::INT:
                return omniruntime::type::OMNI_INT;
            case orc::TypeKind::LONG:
                return omniruntime::type::OMNI_LONG;
            case orc::TypeKind::TIMESTAMP:
            case orc::TypeKind::TIMESTAMP_INSTANT:
                return omniruntime::type::OMNI_TIMESTAMP;
            case orc::TypeKind::FLOAT:
                return omniruntime::type::OMNI_FLOAT;
            case orc::TypeKind::DOUBLE:
                return omniruntime::type::OMNI_DOUBLE;
            case orc::TypeKind::CHAR:
            case orc::TypeKind::STRING:
            case orc::TypeKind::VARCHAR:
                return omniruntime::type::OMNI_VARCHAR;
            case orc::TypeKind::DECIMAL:
                if (type->getPrecision() > OMNI_MAX_DECIMAL64_DIGITS) {
                    return omniruntime::type::OMNI_DECIMAL128;
                } else {
                    return omniruntime::type::OMNI_DECIMAL64;
                }
            case orc::TypeKind::LIST:
                return omniruntime::type::OMNI_ARRAY;
            case orc::TypeKind::STRUCT:
                return omniruntime::type::OMNI_ROW;
            default:
                throw omniruntime::exception::OmniException(
                    "EXPRESSION_NOT_SUPPORT", "OmniMapColumnReader Not Supported Type: " + type->getKind());
        }
    }
}