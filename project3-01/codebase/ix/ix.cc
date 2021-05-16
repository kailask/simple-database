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

    IndexPage::page_pointer_t initial_pointer = 1;  //Initial leaf page is #1
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

//IndexPage
IndexManager::IndexPage::IndexPage(FileHandle &file, ssize_t page_num) {
    setupPointers();

    //Error not handled
    file.readPage(page_num, data);
}

IndexManager::IndexPage::IndexPage(PAGE_TYPE type, void *initial_data, size_t data_size, page_pointer_t next_, page_pointer_t prev_) {
    setupPointers();

    uint32_t type_bit = 0b0;
    uint32_t data_offset = sizeof(page_metadata_t);
    if (type == LEAF_PAGE) {
        type_bit = 0b1;
        data_offset += sizeof(page_pointer_t) * 2;  //Leaf pages have page pointers
        *next = next_;
        *prev = prev_;
    }

    *metadata = (data_offset & offset_mask) | (type_bit << ((sizeof(page_metadata_t) * CHAR_BIT) - 1));
    memcpy(data + data_offset, initial_data, data_size);
}

void IndexManager::IndexPage::setupPointers() {
    data = static_cast<char *>(aligned_alloc(sizeof(page_metadata_t), PAGE_SIZE));
    metadata = reinterpret_cast<page_metadata_t *>(data);
    prev = reinterpret_cast<page_pointer_t *>(metadata + sizeof(page_metadata_t));
    next = reinterpret_cast<page_pointer_t *>(prev + sizeof(page_pointer_t));
}

RC IndexManager::IndexPage::write(FileHandle &file, ssize_t page_num) {
    if (page_num >= 0) return file.writePage(page_num, data);
    return file.appendPage(data);
}

void IndexManager::IndexPage::setOffset(uint32_t offset) {
    *metadata = (offset & offset_mask) | (*metadata & type_mask);
}

RC IndexManager::IndexPage::setData(FileHandle &file, size_t page_num) {
    return file.readPage(page_num, data);
}

IndexManager::IndexPage::~IndexPage() {
    delete data;
}

// IndexManager::IndexPage::iterator IndexManager::IndexPage::begin(Attribute &attr) {
//     return iterator(attr, getType(), data);
// }

// IndexManager::IndexPage::iterator IndexManager::IndexPage::end(Attribute &attr) {
//     PAGE_TYPE type = getType();
//     if (type == INTERNAL_PAGE) return iterator(attr, type, data + getOffset() - sizeof(page_pointer_t));
//     return iterator(attr, type, data + getOffset());
// }
