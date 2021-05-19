#include "ix.h"

#include <cstring>

IndexManager *IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::pfm = NULL;

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

    page_pointer_t initial_pointer = 1;  //Initial leaf page is #1
    IndexPage root(InternalPage, &initial_pointer, sizeof(initial_pointer));
    if (root.write(file) != SUCCESS) return FAILURE;

    IndexPage leaf(LeafPage, NULL, 0);
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
//TODO: modify search based on IndexPage API changes
IndexManager::IndexPage IndexManager::search(Attribute &attr, void *key, IXFileHandle &ixfileHandle) {
    //create indexPage object
    IndexPage temp(ixfileHandle.fileHandle, 0);

    //start at the root
    while (temp.getType() != LeafPage) {
        bool matchFound = false;
        page_pointer_t pageRef;

        //create an iterator and search!
        IndexPage::iterator itor = temp.begin(attr.type);
        while (itor != temp.end(attr.type)) {
            switch (attr.type) {
                case TypeInt: {
                    int targetKeyInt;
                    memcpy(&targetKeyInt, key, INT_SIZE);

                    int searchKeyInt;
                    memcpy(&searchKeyInt, *itor, INT_SIZE);

                    if (targetKeyInt < searchKeyInt) {
                        matchFound = true;
                        memcpy(&pageRef, itor.get(), sizeof(page_pointer_t));
                        temp.setData(ixfileHandle.fileHandle, pageRef);
                    }
                    break;
                }

                case TypeReal: {
                    float targetKeyFlt;
                    memcpy(&targetKeyFlt, key, REAL_SIZE);

                    float searchKeyFlt;
                    memcpy(&searchKeyFlt, *itor, REAL_SIZE);

                    if (targetKeyFlt < searchKeyFlt) {
                        matchFound = true;
                        memcpy(&pageRef, itor.get(), sizeof(page_pointer_t));
                        temp.setData(ixfileHandle.fileHandle, pageRef);
                    }
                    break;
                }

                case TypeVarChar: {
                    unsigned targetKeyLength;
                    memcpy(&targetKeyLength, key, VARCHAR_LENGTH_SIZE);
                    char targetKey[targetKeyLength + 1];
                    memcpy(&targetKey, static_cast<char*>(key) + VARCHAR_LENGTH_SIZE, targetKeyLength);
                    targetKey[targetKeyLength] = '\0';

                    unsigned searchKeyLength;
                    memcpy(&searchKeyLength, *itor, VARCHAR_LENGTH_SIZE);
                    char searchKey[searchKeyLength + 1];
                    memcpy(&searchKey, static_cast<const char*>(*itor) + VARCHAR_LENGTH_SIZE, searchKeyLength);
                    searchKey[searchKeyLength] = '\0';

                    if(strcmp(targetKey, searchKey) < 0) {
                        matchFound = true;
                        memcpy(&pageRef, itor.get(), sizeof(page_pointer_t));
                        temp.setData(ixfileHandle.fileHandle, pageRef);
                    }
                    break;
                }
                //TODO: Error not handled in the default case
            }

            //break from iterator loop if match found
            if (matchFound) break;
            ++itor;
        }

        //if match not found, call get() again to get the rightmost pageRef and set the Index page
        if(!matchFound) {
            memcpy(&pageRef, itor.get(), sizeof(page_pointer_t));
            temp.setData(ixfileHandle.fileHandle, pageRef);
        }
    }

    //return the leaf page
    return temp;
}

//IndexPage
IndexManager::IndexPage::IndexPage(FileHandle &file, size_t page_num) {
    setupPointers();

    //Error not handled
    file.readPage(page_num, data);
}

IndexManager::IndexPage::IndexPage(PageType type, void *initial_data, size_t data_size,
                                   page_pointer_t next_, page_pointer_t prev_) {
    setupPointers();

    //Set initial metadata
    uint32_t type_bit = 0b0;
    uint32_t data_offset = sizeof(page_metadata_t);
    if (type == LeafPage) {
        type_bit = 0b1;
        data_offset += sizeof(page_pointer_t) * 2;  //Leaf pages have page pointers
        *next = next_;
        *prev = prev_;
    }
    type_bit <<= (sizeof(page_metadata_t) * CHAR_BIT) - 1;
    *metadata = ((data_offset + data_size) & offset_mask) | (type_bit & type_mask);

    //Copy initial data
    memcpy(data + data_offset, initial_data, data_size);
}

void IndexManager::IndexPage::setupPointers() {
    data = static_cast<char *>(aligned_alloc(sizeof(page_metadata_t), PAGE_SIZE));
    metadata = reinterpret_cast<page_metadata_t *>(data);
    prev = reinterpret_cast<page_pointer_t *>(metadata + sizeof(page_metadata_t));
    next = reinterpret_cast<page_pointer_t *>(prev + sizeof(page_pointer_t));
}

RC IndexManager::IndexPage::write(FileHandle &file, ssize_t page_num) const {
    if (page_num >= 0) return file.writePage(page_num, data);
    return file.appendPage(data);
}

void IndexManager::IndexPage::setOffset(uint32_t offset) {
    *metadata = (offset & offset_mask) | (*metadata & type_mask);
}

IndexManager::IndexPage::iterator IndexManager::IndexPage::begin(AttrType attr_type) {
    PageType page_type = getType();
    size_t data_start_offset = sizeof(page_metadata_t);
    if (page_type == LeafPage) data_start_offset += sizeof(page_pointer_t) * 2;
    return iterator(attr_type, page_type, data + data_start_offset, data);
}

IndexManager::IndexPage::iterator IndexManager::IndexPage::end(AttrType attr_type) {
    PageType page_type = getType();
    char *it_pos = data + getOffset();
    if (page_type == InternalPage) it_pos -= sizeof(page_pointer_t);  //internal pages "end" before last page pointer
    return iterator(attr_type, page_type, it_pos, data);
}

RC IndexManager::IndexPage::erase(iterator &it) {
    if (it == end(it.attr_type)) return FAILURE;  //Can't erase end

    size_t entry_size = it.calcNextEntrySize();
    size_t bytes_to_move = getOffset() - it.getOffset() - entry_size;
    memmove(it.where, it.where + entry_size, bytes_to_move);
    setOffset(getOffset() - entry_size);
    return SUCCESS;
};

void IndexManager::IndexPage::insert(iterator &it, char *entry, size_t entry_size) {
    size_t bytes_to_move = getOffset() - it.getOffset();
    memmove(it.where + entry_size, it.where, bytes_to_move);
    memcpy(it.where, entry, entry_size);
    setOffset(getOffset() + entry_size);
};

//TODO: Only works for leaf pages
IndexManager::IndexPage *IndexManager::IndexPage::split(iterator &it) {
    size_t new_page_size = getOffset() - it.getOffset();
    setOffset(getOffset() - new_page_size);
    return new IndexPage(getType(), it.where, new_page_size);
}