
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
    if (_cat != NULL) return ERROR;
    _cat = Catalog::createCatalog();
    if (_cat == NULL) return ERROR;
    return SUCCESS;
}

RC RelationManager::deleteCatalog() {
    if (_cat == NULL) return ERROR;
    rbfm->destroyFile(meta_table_name);
    rbfm->destroyFile(columns_table_name);
    delete _cat;
    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs) {
    if (_cat == NULL || _cat->insertTable(tableName, attrs) != SUCCESS) return ERROR;
    return rbfm->createFile(tableName);
}

RC RelationManager::deleteTable(const string &tableName) {
    if (_cat == NULL || _cat->removeTable(tableName) != SUCCESS) return ERROR;
    return rbfm->destroyFile(tableName);
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    if (_cat == NULL) return ERROR;
    Table *t = _cat->getTable(tableName);
    if (t == NULL) return ERROR;
    attrs = t->attrs;
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    if (tableName == meta_table_name || tableName == columns_table_name) return ERROR;
    Table *t = _cat->getTable(tableName);
    return rbfm->insertRecord(t->file, t->attrs, data, rid);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    if (tableName == meta_table_name || tableName == columns_table_name) return ERROR;
    Table *t = _cat->getTable(tableName);
    return rbfm->deleteRecord(t->file, t->attrs, rid);
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
    if (tableName == meta_table_name || tableName == columns_table_name) return ERROR;
    Table *t = _cat->getTable(tableName);
    return rbfm->updateRecord(t->file, t->attrs, data, rid);
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data) {
    Table *t = _cat->getTable(tableName);
    return rbfm->readRecord(t->file, t->attrs, rid, data);
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data) {
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data) {
    Table *t = _cat->getTable(tableName);
    return rbfm->readAttribute(t->file, t->attrs, rid, attributeName, data);
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    Table *t = _cat->getTable(tableName);
    rm_ScanIterator.my_iter = RBFM_ScanIterator();
    return rbfm->scan(t->file, t->attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.my_iter);
}

RelationManager::Catalog *RelationManager::Catalog::createCatalog() {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
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
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle meta_file;
    FileHandle columns_file;
    if (rbfm->openFile(meta_table_name, meta_file) != SUCCESS ||
        rbfm->openFile(columns_table_name, columns_file) != SUCCESS) {
        return NULL;
    }

    return new Catalog(meta_file, columns_file);
}

RelationManager::Catalog::Catalog(FileHandle _meta_file, FileHandle _columns_file)
    : meta_file(_meta_file), columns_file(_columns_file) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RBFM_ScanIterator it;

    RID rid;
    char data[PAGE_SIZE];
    rbfm->scan(meta_file, meta_table_attrs, "", NO_OP, NULL, {"table-name"}, it);
    while (it.getNextRecord(rid, data) != RBFM_EOF) {
        unsigned name_len;
        memcpy(&name_len, (data + 1), sizeof(name_len));
        tables.emplace(string{(data + 1 + sizeof(name_len)), name_len}, Table(rid));
    }

    rbfm->scan(columns_file, columns_table_attrs, "", NO_OP, NULL,
               {"column-name", "column-type", "table-name"}, it);

    while (it.getNextRecord(rid, data) != RBFM_EOF) {
        Attribute attr;
        auto table = parseAttribute(data, attr);
        tables.at(table).attrs.push_back(attr);
        tables.at(table).rids.push_back(rid);
    }
}

RC RelationManager::Catalog::insertTable(const string &tableName, const vector<Attribute> &attrs) {
    if (tables.find(tableName) != tables.end()) return ERROR;
    auto rids = writeTable(meta_file, columns_file, attrs, newTableId(), tableName);
    tables.emplace(tableName, Table(attrs, rids));
    return SUCCESS;
}

RC RelationManager::Catalog::removeTable(const string &tableName) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (tableName == meta_table_name || tableName == columns_table_name ||
        tables.find(tableName) == tables.end()) return ERROR;
    vector<RID> &rids = tables.at(tableName).rids;
    rbfm->deleteRecord(meta_file, tables.at(meta_table_name).attrs, rids[0]);
    for (auto i = 1; i < rids.size(); i++) {
        rbfm->deleteRecord(columns_file, tables.at(columns_table_name).attrs, rids[i]);
    }

    tables.erase(tableName);
    return SUCCESS;
}

RelationManager::Table *RelationManager::Catalog::getTable(const string &tableName) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (tables.find(tableName) == tables.end()) return NULL;
    Table *t = &tables.at(tableName);
    if (t->file.getfd() == NULL && rbfm->openFile(tableName, t->file) != SUCCESS) return NULL;
    return t;
}

vector<RID> RelationManager::Catalog::writeTable(FileHandle &meta_file, FileHandle &columns_file,
                                                 const vector<Attribute> &attrs, unsigned table_id, const string &table_name) {
    char data[PAGE_SIZE];
    vector<RID> rids;
    RID rid;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    prepareTuple(data, meta_table_attrs, {(void *)&table_id, (void *)table_name.c_str(), (void *)table_name.c_str()});
    rbfm->insertRecord(meta_file, meta_table_attrs, data, rid);
    rids.push_back(rid);

    for (size_t i = 0; i < attrs.size(); i++) {
        Attribute attr = attrs[i];
        prepareTuple(data, columns_table_attrs, {(void *)&table_id, (void *)attr.name.c_str(), (void *)&attr.type, &i, (void *)table_name.c_str()});
        rbfm->insertRecord(columns_file, columns_table_attrs, data, rid);
        rids.push_back(rid);
    }
    return rids;
}

string RelationManager::Catalog::parseAttribute(char *data, Attribute &attr) {
    data += static_cast<int>(ceil(4.0 / CHAR_BIT));

    unsigned name_len = 0;
    memcpy(&name_len, data, sizeof(name_len));
    data += sizeof(name_len);
    attr.name = string{data, name_len};
    data += name_len;
    memcpy(&attr.type, data, sizeof(attr.type));
    data += sizeof(attr.type);

    unsigned table_name_len = 0;
    memcpy(&table_name_len, data, sizeof(table_name_len));
    data += sizeof(table_name_len);
    return string{data, table_name_len};
}

void RelationManager::prepareTuple(char *data, const vector<Attribute> &attrs, vector<void *> values) {
    data += static_cast<int>(ceil((double)attrs.size() / CHAR_BIT));

    for (size_t i = 0; i < attrs.size(); i++) {
        switch (attrs[i].type) {
            case TypeInt:
            case TypeReal:
                memcpy(data, values[i], 4);
                data += 4;
                break;
            case TypeVarChar:
                int len = strlen((char *)values[i]);
                memcpy(data, &len, 4);
                data += 4;
                memcpy(data, values[i], len);
                data += attrs[i].length;
        }
    }
}