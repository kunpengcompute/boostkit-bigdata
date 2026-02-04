#ifndef CPP_ORCUTIL_H
#define CPP_ORCUTIL_H

#include "orc/Type.hh"
#include "vector/vector_common.h"

namespace omniruntime::reader {
    class OrcUtil {
    public:
        static omniruntime::type::DataTypeId tranOrcTypeToOmniType(const orc::Type *type);
    };
}
#endif CPP_ORCUTIL_H