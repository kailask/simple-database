
#ifndef _rm_h_
#define _rm_h_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "../rbf/rbfm.h"

#define SUCCESS 0
#define ERROR -1

using namespace std;

#define RM_EOF (-1)  // end of a scan operator

//System tables
const static string meta_table_name = "tables";
const static string columns_table_name = "columns";
const static vector<Attribute> meta_table_attrs = {
    {"table-id", TypeInt, 4},
    {"table-name", TypeVarChar, 50},
    {"file-name", TypeVarChar, 50}};
const static vector<Attribute> columns_table_attrs = {
    {"table-id", TypeInt, 4},
    {"column-name", TypeVarChar, 50},
    {"column-type", TypeInt, 4},
    {"column-position", TypeInt, 4},
    {"table-name", TypeVarChar, 50}};

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
   public:
    RM_ScanIterator(){};
    ~RM_ScanIterator(){};

    RBFM_ScanIterator my_iter;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) { return my_iter.getNextRecord(rid, data); };
    RC close() { return -1; };
};

// Relation Manager
class RelationManager {
    struct Table {
        vector<Attribute> attrs;
        vector<RID> rids;
        FileHandle file;

        Table(RID rid_) { rids.push_back(rid_); };
        Table(vector<Attribute> attrs_, vector<RID> rids_) : attrs(attrs_), rids(rids_){};
        ~Table() { RecordBasedFileManager::instance()->closeFile(file); }
    };

    class Catalog {
       public:
        unordered_map<string, Table> tables;

        static Catalog *openCatalog();
        static Catalog *createCatalog();

        RC insertTable(const string &tableName, const vector<Attribute> &attrs);
        RC removeTable(const string &tableName);
        RelationManager::Table *getTable(const string &tableName);

        ~Catalog() {
            RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
            rbfm->closeFile(meta_file);
            rbfm->closeFile(columns_file);
        }

       private:
        FileHandle meta_file;
        FileHandle columns_file;

        Catalog(FileHandle _meta_file, FileHandle _columns_file);
        static vector<RID> writeTable(FileHandle &meta_file, FileHandle &columns_file,
                                      const vector<Attribute> &attrs, unsigned table_id, const string &table_name);
        unsigned newTableId() { return ++table_id; }
        unsigned table_id = 0;

        static string parseAttribute(char *data, Attribute &attr);
    };

   public:
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    static RelationManager *instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const string &tableName, const vector<Attribute> &attrs);

    RC deleteTable(const string &tableName);

    RC getAttributes(const string &tableName, vector<Attribute> &attrs);

    RC insertTuple(const string &tableName, const void *data, RID &rid);

    RC deleteTuple(const string &tableName, const RID &rid);

    RC updateTuple(const string &tableName, const void *data, const RID &rid);

    RC readTuple(const string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const vector<Attribute> &attrs, const void *data);

    RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const string &tableName,
            const string &conditionAttribute,
            const CompOp compOp,                   // comparison type such as "<" and "="
            const void *value,                     // used in the comparison
            const vector<string> &attributeNames,  // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

   protected:
    RelationManager();
    ~RelationManager();

   private:
    static RelationManager *_rm;
    Catalog *_cat;

    static void prepareTuple(char *data, const vector<Attribute> &attrs, vector<void *> values);
};

#endif
