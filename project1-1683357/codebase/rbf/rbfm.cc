#include "rbfm.h"

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager *RecordBasedFileManager::instance() {
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
    //initialize map of type to length
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    //parse data and package into recordBuff for insertion
    int totalSize = 0;
    vector<Attribute> nonNullFields;

    int nullBitSize = ceil((double) recordDescriptor.size() / 8);
    totalSize += nullBitSize;

    cout << "null bit size is " << nullBitSize << endl;
    char nullBits[nullBitSize];
    memcpy(nullBits, data, nullBitSize);

    for(int i = 0; i < nullBitSize; i++) {
        unsigned length = (i != nullBitSize - 1 ? 8 : recordDescriptor.size() % 8);
        cout << "the length is " << length << endl;
        for(long unsigned int i = 0; i < length; ++i) {
            cout << unsigned(nullBits[i]) << endl;
        }
    }

    //find the free page to insert into and read page into pageBuff

    //insert recordBuff into pageBuff and adjust mini directory, rid

    //write page back into file
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}
