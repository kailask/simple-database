#include <string.h>
#include "ix.h"

IndexManager *IndexManager::_index_manager = 0;

IndexManager *IndexManager::instance() {
    if (!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName) {
    PagedFileManager *pfm = PagedFileManager::instance();
    if (pfm->createFile(fileName) != SUCCESS) return FAILURE;

    FileHandle file;
    if (pfm->openFile(fileName, file) != SUCCESS) return FAILURE;

    IndexPage::page_pointer_t initial_pointer = 134480385;
    IndexPage root(IndexPage::INTERNAL_PAGE, &initial_pointer, sizeof(initial_pointer));
    if (root.write(file) != SUCCESS) return FAILURE;

    IndexPage leaf(IndexPage::LEAF_PAGE, NULL, 0);
    if (leaf.write(file) != SUCCESS) return FAILURE;

    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName) {
    return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
    return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    return -1;
}

RC IX_ScanIterator::close() {
    return -1;
}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    return -1;
}

IndexManager::IndexPage::IndexPage(FileHandle &file, ssize_t page_num) {
    data = static_cast<char *>(aligned_alloc(sizeof(page_metadata_t), PAGE_SIZE));
    metadata = reinterpret_cast<page_metadata_t *>(data);

    //Error not handled
    file.readPage(page_num, data);
}

IndexManager::IndexPage::IndexPage(PAGE_TYPE type, void *initial_data, size_t data_size) {
    data = static_cast<char *>(aligned_alloc(sizeof(page_metadata_t), PAGE_SIZE));
    metadata = reinterpret_cast<page_metadata_t *>(data);

    setMetadata(type, sizeof(page_metadata_t) + data_size);
    memcpy(data + sizeof(page_metadata_t), initial_data, data_size);
}

RC IndexManager::IndexPage::write(FileHandle &file, ssize_t page_num) {
    if (page_num >= 0) return file.writePage(page_num, data);
    return file.appendPage(data);
}

void IndexManager::IndexPage::setMetadata(PAGE_TYPE type, uint32_t offset) {
    uint32_t t = (type == LEAF_PAGE) ? 0b1 : 0b0;
    *metadata = (offset & 0x7FFFFFFF) | (t << ((sizeof(page_metadata_t) * CHAR_BIT) - 1));
}

IndexManager::IndexPage::~IndexPage() {
    delete data;
}