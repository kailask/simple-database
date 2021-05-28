
#include "qe.h"

#include <math.h>

// Value =====================================================================

//Compare two values with given operation
bool Value::compare(CompOp op, Value other) {
    if (type != other.type) return false;

    switch (type) {
        case AttrType::TypeInt: {
            signed lhs, rhs;
            memcpy(&lhs, data, INT_SIZE);
            memcpy(&rhs, other.data, INT_SIZE);
            auto comp = getOperator<signed>(op);
            return comp(lhs, rhs);
        }
        case AttrType::TypeReal: {
            float lhs, rhs;
            memcpy(&lhs, data, REAL_SIZE);
            memcpy(&rhs, other.data, REAL_SIZE);
            auto comp = getOperator<float>(op);
            return comp(lhs, rhs);
        }
        case AttrType::TypeVarChar: {
            unsigned rhs_len, lhs_len;
            char *rhs, *lhs;
            memcpy(&rhs_len, data, VARCHAR_LENGTH_SIZE);
            memcpy(&lhs_len, other.data, VARCHAR_LENGTH_SIZE);
            rhs = static_cast<char*>(data) + rhs_len + VARCHAR_LENGTH_SIZE;
            lhs = static_cast<char*>(other.data) + lhs_len + VARCHAR_LENGTH_SIZE;
            auto comp = getOperator<string>(op);
            return comp({rhs, rhs_len}, {lhs, lhs_len});
        }
        default:
            return false;
    }
}

//Get functor for operation type
template <typename t>
function<bool(t, t)> Value::getOperator(CompOp op) {
    switch (op) {
        case EQ_OP:
            return [](t lhs, t rhs) { return lhs == rhs };
        case LT_OP:
            return [](t lhs, t rhs) { return lhs < rhs };
        case LE_OP:
            return [](t lhs, t rhs) { return lhs <= rhs };
        case GT_OP:
            return [](t lhs, t rhs) { return lhs > rhs };
        case GE_OP:
            return [](t lhs, t rhs) { return lhs >= rhs };
        case NE_OP:
            return [](t lhs, t rhs) { return lhs != rhs };
        case NO_OP:
        default:
            return [](t lhs, t rhs) { return true };
    }
}

// Tuple =====================================================================

//Get given attribute from tuple
Value Tuple::getAttr(const string& attr_name) {
    char* curr_data = data + static_cast<size_t>(ceil(attrs.size() / double(CHAR_BIT)));

    for (size_t i = 0; i < attrs.size(); i++) {
        auto curr_attr = attrs.at(i);
        if (curr_attr.name == attr_name) return {curr_attr.type, (isNull(i) ? NULL : curr_data)};

        if (!isNull(i)) {
            switch (curr_attr.type) {
                case TypeReal:
                    curr_data += REAL_SIZE;
                    break;
                case TypeInt:
                    curr_data += INT_SIZE;
                    break;
                case TypeVarChar:
                    unsigned len;
                    memcpy(&len, curr_data, VARCHAR_LENGTH_SIZE);
                    curr_data += len + VARCHAR_LENGTH_SIZE;
                default:
                    break;
            }
        }
    }

    //Invalid attribute name
    return {AttrType::TypeInt, NULL};
}

// Filter ====================================================================

Filter::Filter(Iterator* input_, const Condition& condition_) : input(input_), condition(condition_) {
    input->getAttributes(attrs);
}

RC Filter::getNextTuple(void* data) {
    do {
        auto ret = input->getNextTuple(data);
        if (ret != SUCCESS) return QE_EOF;
    } while (!isFilteredTuple(data));

    return SUCCESS;
}

//Check if tuple is included in filter
bool Filter::isFilteredTuple(void* data) {
    Tuple t(attrs, static_cast<char*>(data));
    Value v = t.getAttr(condition.lhsAttr);
    return v.compare(condition.op, condition.rhsValue);
}

// ... the rest of your implementations go here
