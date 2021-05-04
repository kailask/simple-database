
#include "rm.h"

RelationManager *RelationManager::_rm = 0;

RelationManager *RelationManager::instance() {
    if (!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager() {
    _cat = Catalog::openCatalog();
}

RelationManager::~RelationManager() {
    delete _cat;
}

RC RelationManager::createCatalog() {
    rbfm->createFile("Tables.tbl");
    FileHandle fd;
    rbfm->openFile("Table.tbl", fd);
    return -1;
}

RC RelationManager::deleteCatalog() {
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs) {
    return -1;
}

RC RelationManager::deleteTable(const string &tableName) {
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data) {
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data) {
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data) {
    return -1;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    return -1;
}

RelationManager::Catalog *RelationManager::Catalog::createCatalog() {
    if (rbfm->createFile(meta_table_name) != SUCCESS ||
        rbfm->createFile(columns_table_name) != SUCCESS) {
        return NULL;
    }

    FileHandle meta_file;
    FileHandle columns_file;
    if (rbfm->openFile(meta_table_name, meta_file) != SUCCESS ||
        rbfm->openFile(columns_table_name, columns_file) != SUCCESS) {
        return NULL;
    }

    writeTable(meta_file, columns_file, meta_table_attrs, 0, meta_table_name);
    writeTable(meta_file, columns_file, columns_table_attrs, 1, columns_table_name);

    return new Catalog(meta_file, columns_file);
}

RelationManager::Catalog *RelationManager::Catalog::openCatalog() {
    FileHandle meta_file;
    FileHandle columns_file;
    if (rbfm->openFile(meta_table_name, meta_file) != SUCCESS ||
        rbfm->openFile(columns_table_name, columns_file) != SUCCESS) {
        return NULL;
    }

    return new Catalog(meta_file, columns_file);
}

RelationManager::Catalog::Catalog(FileHandle meta_file, FileHandle columns_file) {
    RBFM_ScanIterator it;

    RID rid;
    char data[PAGE_SIZE];
    rbfm->scan(meta_file, meta_table_attrs, "", NO_OP, NULL, {"table-name"}, it);
    while (it.getNextRecord(rid, data) != RBFM_EOF) {
    }
}

RelationManager::Catalog::~Catalog() {
    //free tables
}

RC RelationManager::Catalog::writeTable(FileHandle &meta_file, FileHandle &columns_file,
                                        const vector<Attribute> &attrs, unsigned table_id, const string &table_name) {
    char data[PAGE_SIZE];
    RID rid;

    prepareTuple(data, meta_table_attrs, {(void *)&table_id, (void *)table_name.c_str(), (void *)table_name.c_str()});
    RC ret = rbfm->insertRecord(meta_file, meta_table_attrs, data, rid);
    if (ret != SUCCESS) return ERROR;

    for (size_t i = 0; i < attrs.size(); i++) {
        Attribute attr = attrs[i];
        prepareTuple(data, columns_table_attrs, {(void *)&table_id, (void *)attr.name.c_str(), (void *)&attr.type, &i, (void *)table_name.c_str()});
        ret = rbfm->insertRecord(columns_file, columns_table_attrs, data, rid);
        if (ret != SUCCESS) return ERROR;
    }
}

static void
prepareTuple(char *data, const vector<Attribute> &attrs, vector<void *> values) {
    data += static_cast<int>(ceil((double)attrs.size() / CHAR_BIT));

    for (size_t i = 0; i < attrs.size(); i++) {
        switch (attrs[i].type) {
            case TypeInt:
            case TypeReal:
                memcpy(data, values[i], 4);
                data += 4;
                break;
            case TypeVarChar:
                memcpy(data, &attrs[i].length, 4);
                data += 4;
                memcpy(data, values[i], strlen((char *)values[i]));
                data += attrs[i].length;
        }
    }
}