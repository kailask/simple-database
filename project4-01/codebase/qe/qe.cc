
#include "qe.h"

#include <math.h>

// Value =====================================================================

//Compare two values with given operation
bool Value::compare(CompOp op, const Value& other) const {
    if (type != other.type) return false;

    //Null values can only be compared for equality
    if (data == NULL || other.data == NULL) {
        return (data == NULL && other.data == NULL && op == EQ_OP);
    }

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
function<bool(t, t)> Value::getOperator(CompOp op) const {
    switch (op) {
        case EQ_OP:
            return [](t lhs, t rhs) { return lhs == rhs; };
        case LT_OP:
            return [](t lhs, t rhs) { return lhs < rhs; };
        case LE_OP:
            return [](t lhs, t rhs) { return lhs <= rhs; };
        case GT_OP:
            return [](t lhs, t rhs) { return lhs > rhs; };
        case GE_OP:
            return [](t lhs, t rhs) { return lhs >= rhs; };
        case NE_OP:
            return [](t lhs, t rhs) { return lhs != rhs; };
        case NO_OP:
        default:
            return [](t lhs, t rhs) { return true; };
    }
}

//Get total size of value
size_t Value::getSize() const {
    switch (type) {
        case TypeReal:
            return REAL_SIZE;
        case TypeInt:
            return INT_SIZE;
        case TypeVarChar:
            unsigned len;
            memcpy(&len, data, VARCHAR_LENGTH_SIZE);
            return len + VARCHAR_LENGTH_SIZE;
        default:
            return 0;
    }
}

// Tuple =====================================================================

//Get given attribute from tuple
Value Tuple::getValue(const string& attr_name) const {
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
    return {.data = NULL};
}

// Tuple::Builder ============================================================

Tuple::Builder::Builder(char* data_, size_t num_attrs_) : num_attrs(num_attrs_), data(data_) {
    data_end = data + static_cast<size_t>(ceil(num_attrs / double(CHAR_BIT)));
}

//Build tuple if all attributes are set
Tuple Tuple::Builder::getTuple() const {
    return (num_attrs == attrs.size()) ? Tuple(data, attrs) : Tuple(data, {});
}

//Append a given value to the builder
void Tuple::Builder::appendValue(const Value& v, const string& attr_name) {
    if (num_attrs == attrs.size()) return;

    size_t index = attrs.size();
    attrs.push_back({attr_name, v.type});

    //Set null bit
    char null_bit = (v.data == NULL) ? 0x8 : 0x0;
    *(data + (index / CHAR_BIT)) = *(data + (index / CHAR_BIT)) | null_bit >> (index % CHAR_BIT);

    //Copy data
    if (v.data != NULL) {
        memcpy(data_end, v.data, v.getSize());
        data_end += v.getSize();
    }
}

// Filter ====================================================================

Filter::Filter(Iterator* input_, const Condition& condition_) : input(input_), condition(condition_) {
    input->getAttributes(attrs);
}

RC Filter::getNextTuple(void* data) {
    do {
        if (input->getNextTuple(data) != SUCCESS) return QE_EOF;
    } while (!isFilteredTuple(data));

    return SUCCESS;
}

//Check if tuple is included in filter
bool Filter::isFilteredTuple(void* data) {
    Tuple t(static_cast<char*>(data), attrs);
    Value v = t.getValue(condition.lhsAttr);

    // condition.bRhsIsAttr assumed to be false
    return v.compare(condition.op, condition.rhsValue);
}

void Filter::getAttributes(vector<Attribute>& attrs_) const {
    attrs_.clear();
    attrs_.insert(attrs_.end(), attrs.begin(), attrs.end());
};

// Project ===================================================================

Project::Project(Iterator* input_, const vector<string>& attrNames) : input(input_), output_attrs(attrNames) {
    input->getAttributes(input_attrs);
    source = Tuple(buffer, input_attrs);
}

RC Project::getNextTuple(void* data) {
    if (input->getNextTuple(buffer) != SUCCESS) return QE_EOF;

    //Build new tuple with projected attributes
    Tuple::Builder projection = Tuple::build(static_cast<char*>(data), output_attrs.size());
    for (const string& attr : output_attrs) projection.appendValue(source.getValue(attr), attr);
    return SUCCESS;
}

void Project::getAttributes(vector<Attribute>& attrs) const {
    attrs.clear();
    for (const string& attr_name : output_attrs) {
        for (const Attribute& attr : input_attrs) {
            if (attr_name == attr.name) attrs.emplace_back(attr);
        }
    }
};

// INLJoin ===================================================================

INLJoin::INLJoin(Iterator* leftIn_, IndexScan* rightIn_, const Condition& condition_)
    : leftIn(leftIn_), rightIn(rightIn_), condition(condition_) {
    leftIn->getAttributes(left_attrs);
    rightIn->getAttributes(right_attrs);

    left_tuple = Tuple(NULL);  //Left tuple is initially null
    right_tuple = Tuple(right_buffer, right_attrs);
}

RC INLJoin::getNextTuple(void* data) {
    //getNextTuple is being called for the first time
    if (left_tuple.data == NULL) {
        left_tuple = Tuple(left_buffer, left_attrs);
        if (getNextOuterTuple() != SUCCESS) return QE_EOF;
    }

    //Get next inner tuple
    while (rightIn->getNextTuple(right_buffer) != SUCCESS) {
        if (getNextOuterTuple() != SUCCESS) return QE_EOF;
    }

    //Build result tuple
    Tuple::Builder result = Tuple::build(static_cast<char*>(data), left_attrs.size() + right_attrs.size());
    for (const Attribute& attr : left_attrs) result.appendValue(left_tuple.getValue(attr.name), attr.name);
    for (const Attribute& attr : right_attrs) result.appendValue(right_tuple.getValue(attr.name), attr.name);

    return SUCCESS;
}

//Get next tuple from outer (left) input
RC INLJoin::getNextOuterTuple() {
    if (leftIn->getNextTuple(left_buffer) != SUCCESS) return QE_EOF;
    Value lhs = left_tuple.getValue(condition.lhsAttr);

    //Only handling EQ_OP
    rightIn->setIterator(lhs.data, lhs.data, true, true);
    return SUCCESS;
}

void INLJoin::getAttributes(vector<Attribute>& attrs) const {
    attrs.clear();

    //Output attrs is concatenation of inputs
    attrs.insert(attrs.end(), left_attrs.begin(), left_attrs.end());
    attrs.insert(attrs.end(), right_attrs.begin(), right_attrs.end());
}