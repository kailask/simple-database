#ifndef _qe_h_
#define _qe_h_

#include <functional>
#include <vector>

#include "../ix/ix.h"
#include "../rbf/rbfm.h"
#include "../rm/rm.h"

#define QE_EOF (-1)  // end of the index scan
#define SUCCESS 0

using namespace std;

typedef enum { MIN = 0,
               MAX,
               COUNT,
               SUM,
               AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;  // type of value
    void *data;     // value

    bool compare(CompOp op, const Value &other) const;
    size_t getSize() const;

   private:
    template <typename t>
    function<bool(t, t)> getOperator(CompOp op) const;
};

struct Condition {
    string lhsAttr;   // left-hand side attribute
    CompOp op;        // comparison operator
    bool bRhsIsAttr;  // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;   // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;   // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator {
    // All the relational operators and access methods are iterators.
   public:
    virtual RC getNextTuple(void *data) = 0;
    virtual void getAttributes(vector<Attribute> &attrs) const = 0;
    virtual ~Iterator(){};
};

//Represent an iterator tuple
class Tuple {
   public:
    //Class for building a Tuple by appending values
    class Builder {
       public:
        Tuple getTuple() const;
        void appendValue(const Value &v, const string &attr_name);

       private:
        vector<Attribute> attrs;
        const size_t num_attrs;

        char *data;
        char *data_end;

        friend class Tuple;
        Builder(char *data_, size_t num_attrs_);
    };

    Tuple(char *data_ = NULL, vector<Attribute> attrs_ = {}) : attrs(attrs_), data(data_) {}
    Value getValue(const string &attr_name) const;

    //Return a new Tuple::Builder
    static Builder build(char *data_, size_t num_attrs_) { return Builder(data_, num_attrs_); };

    vector<Attribute> attrs;
    char *data;

    bool isNull(size_t index) const {
        return (*(data + (index / CHAR_BIT)) << (index % CHAR_BIT)) & 0x8;
    }  //Is attribute null at index?
};

class TableScan : public Iterator {
    // A wrapper inheriting Iterator over RM_ScanIterator
   public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    string tableName;
    vector<Attribute> attrs;
    vector<string> attrNames;
    RID rid;

    TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL) : rm(rm) {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        unsigned i;
        for (i = 0; i < attrs.size(); ++i) {
            // convert to char *
            attrNames.push_back(attrs.at(i).name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator() {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data) {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;

        // For attribute in vector<Attribute>, name it as rel.attr
        for (i = 0; i < attrs.size(); ++i) {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs.at(i).name;
            attrs.at(i).name = tmp;
        }
    };

    ~TableScan() {
        iter->close();
    };
};

class IndexScan : public Iterator {
    // A wrapper inheriting Iterator over IX_IndexScan
   public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    string tableName;
    string attrName;
    vector<Attribute> attrs;
    char key[PAGE_SIZE];
    RID rid;

    IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL) : rm(rm) {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey,
                     void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive) {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                     highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data) {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0) {
            rc = rm.readTuple(tableName.c_str(), rid, data);
        }
        return rc;
    };

    void getAttributes(vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;

        // For attribute in vector<Attribute>, name it as rel.attr
        for (i = 0; i < attrs.size(); ++i) {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs.at(i).name;
            attrs.at(i).name = tmp;
        }
    };

    ~IndexScan() {
        iter->close();
    };
};

class Filter : public Iterator {
    // Filter operator
   public:
    Iterator *input;
    const Condition &condition;
    vector<Attribute> attrs;

    Filter(Iterator *input_,            // Iterator of input R
           const Condition &condition_  // Selection condition
    );
    ~Filter(){};

    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs_) const;

   private:
    bool isFilteredTuple(void *data);
};

class Project : public Iterator {
    // Projection operator
   public:
    Iterator *input;
    vector<Attribute> input_attrs;
    const vector<string> &output_attrs;
    char buffer[PAGE_SIZE];  //Buffer for parsing tuples

    Tuple source;

    Project(Iterator *input_,                  // Iterator of input R
            const vector<string> &attrNames);  // vector containing attribute names
    ~Project(){};

    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
   public:
    Iterator *leftIn;
    IndexScan *rightIn;
    const Condition &condition;

    //Current lhs value for comparisions
    Value lhs;

    vector<Attribute> left_attrs;
    vector<Attribute> right_attrs;
    char left_buffer[PAGE_SIZE];  //Buffer for parsing tuples
    char right_buffer[PAGE_SIZE];

    Tuple left_tuple;
    Tuple right_tuple;

    INLJoin(Iterator *leftIn_,           // Iterator of input R
            IndexScan *rightIn_,         // IndexScan Iterator of input S
            const Condition &condition_  // Join condition
    );
    ~INLJoin(){};

    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

   private:
    RC getNextOuterTuple();
};

#endif
