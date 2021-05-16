#include <string.h>
#include "ix.h"

IndexManager *IndexManager::_index_manager = 0;

IndexManager *IndexManager::instance() {
    if (!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager() {
    pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName) {
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
    return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
    return pfm->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
    return pfm->closeFile(ixfileHandle.fileHandle);
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

//Helper functions
IndexManager::IndexPage IndexManager::search(Attribute &attr, void *key, IXFileHandle &ixfileHandle) {
    //create indexPage object
    IndexPage temp(ixfileHandle.fileHandle, 0);

    //start at the root
    while(temp.getType() != IndexPage::LEAF_PAGE) {
        bool matchFound = false;

        //create an iterator
        IndexPage::iterator itor = temp.begin(attr);
        while(itor != temp.end(attr)) {
            void* searchKey = *itor;

            switch (attr.type) {
                case TypeInt:
                    if(*static_cast<int*>(key) <= *static_cast<int*>(searchKey)) {
                        matchFound = true;
                        //TODO: call data to get the pageRef and reuse temp object and use new pageRef
                        break;
                    }

                case TypeReal:
                    if(*static_cast<float*>(key) <= *static_cast<float*>(searchKey)) {
                        matchFound = true;
                        //TODO: call data to get the pageRef and reuse temp object and use new pageRef
                        break;
                    }
                case TypeVarChar:
                    if(strcmp(static_cast<char*>(key + sizeof(unsigned)), static_cast<char*>(searchKey + sizeof(unsigned))) <= 0) {
                        matchFound = true;
                        //TODO: call data to get the pageRef and reuse temp object and use new pageRef
                        break;
                    }
            }

            if(matchFound) break;
            ++itor;
        }

        //TODO: if itor is = to end but match was not found call getData() again to get the right pageRef
    }
    return temp;
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

    *metadata = ((data_offset + data_size) & offset_mask) | (type_bit << ((sizeof(page_metadata_t) * CHAR_BIT) - 1));
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

IndexManager::IndexPage::iterator IndexManager::IndexPage::begin(Attribute &attr) {
    PAGE_TYPE type = getType();
    uint32_t data_start_offset = sizeof(page_metadata_t);
    if (type == LEAF_PAGE) data_start_offset += sizeof(page_pointer_t) * 2;
    return iterator(attr, type, data + data_start_offset);
}

IndexManager::IndexPage::iterator IndexManager::IndexPage::end(Attribute &attr) {
    PAGE_TYPE type = getType();  //internal pages "end" before last page pointer
    if (type == INTERNAL_PAGE) return iterator(attr, type, data + getOffset() - sizeof(page_pointer_t));
    return iterator(attr, type, data + getOffset());
}
