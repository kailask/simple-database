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

    IndexPage leaf(LeafPage, NULL, 0, 0);
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

//IndexManager ====================================================================
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

//IndexManager::IndexPage ==============================================================================

IndexManager::IndexPage::IndexPage(FileHandle &file, size_t page_num) {
    setupPointers();

    //Error not handled
    file.readPage(page_num, data);
}

IndexManager::IndexPage::IndexPage(PageType type, void *initial_data, size_t data_size,
                                   page_pointer_t parent_, page_pointer_t next_, page_pointer_t prev_) {
    setupPointers();

    //Set initial metadata
    uint32_t type_bit = 0b0;
    uint32_t data_offset = sizeof(page_metadata_t) + sizeof(page_pointer_t);
    if (type == LeafPage) {
        type_bit = 0b1;
        data_offset += sizeof(page_pointer_t) * 2;  //Leaf pages have extra page pointers
        *next = next_;
        *prev = prev_;
    }

    type_bit <<= (sizeof(page_metadata_t) * CHAR_BIT) - 1;
    *metadata = ((data_offset + data_size) & offset_mask) | (type_bit & type_mask);
    *parent = parent_;

    //Copy initial data
    memcpy(data + data_offset, initial_data, data_size);
}

void IndexManager::IndexPage::setupPointers() {
    data = static_cast<char *>(aligned_alloc(sizeof(page_metadata_t), PAGE_SIZE));
    metadata = reinterpret_cast<page_metadata_t *>(data);
    parent = reinterpret_cast<page_pointer_t *>(metadata + sizeof(page_metadata_t));
    prev = reinterpret_cast<page_pointer_t *>(parent + sizeof(page_metadata_t));
    next = reinterpret_cast<page_pointer_t *>(prev + sizeof(page_pointer_t));
}

RC IndexManager::IndexPage::write(FileHandle &file, ssize_t page_num) const {
    if (page_num >= 0) return file.writePage(page_num, data);
    return file.appendPage(data);
}

IndexManager::IndexPage::iterator IndexManager::IndexPage::begin(AttrType attr_type) const {
    PageType page_type = getType();

    //Iterator always points to start of key
    size_t it_pos = sizeof(page_metadata_t) + sizeof(page_pointer_t);  //Data for all page types
    if (page_type == LeafPage) {
        it_pos += sizeof(page_pointer_t) * 2;  //Leaf pages have extra page pointers
    } else {
        it_pos += sizeof(page_pointer_t);  //Internal pages point after page pointer
    }

    return iterator(attr_type, page_type, it_pos, data);
}

IndexManager::IndexPage::iterator IndexManager::IndexPage::end(AttrType attr_type) const {
    PageType page_type = getType();
    size_t it_pos = getOffset();
    return iterator(attr_type, page_type, it_pos, data);
}

RC IndexManager::IndexPage::erase(iterator &it) {
    if (it == end(it.attr_type)) return FAILURE;  //Can't erase end

    size_t it_entry_offset = it.getOffset();
    if (it.page_type == InternalPage) it_entry_offset -= sizeof(page_pointer_t);

    size_t entry_size = it.calcNextEntrySize();
    size_t it_next_entry_offset = it_entry_offset + entry_size;
    size_t bytes_to_move = getOffset() - it_entry_offset - entry_size;
    memmove(data + it_entry_offset, data + it_next_entry_offset, bytes_to_move);
    setOffset(getOffset() - entry_size);
    return SUCCESS;
};

void IndexManager::IndexPage::insert(iterator &it, key &k, value &v) {
    size_t bytes_to_move = getOffset() - it.getOffset();
    size_t entry_size = (it.page_type == InternalPage) ? sizeof(page_pointer_t) : sizeof(RID);
    char *insert_pos = it.where;

    switch (it.attr_type) {
        case AttrType::TypeInt:
            entry_size += INT_SIZE;
            memmove(it.where + entry_size, it.where, bytes_to_move);
            memcpy(insert_pos, &k.i, INT_SIZE);
            insert_pos += INT_SIZE;
        case AttrType::TypeReal:
            entry_size += REAL_SIZE;
            memmove(it.where + entry_size, it.where, bytes_to_move);
            memcpy(insert_pos, &k.r, REAL_SIZE);
            insert_pos += REAL_SIZE;
        case AttrType::TypeVarChar:
            unsigned len = k.s.length();
            entry_size += VARCHAR_LENGTH_SIZE + len;
            memmove(it.where + entry_size, it.where, bytes_to_move);
            memcpy(insert_pos, &len, VARCHAR_LENGTH_SIZE);
            memcpy(insert_pos + VARCHAR_LENGTH_SIZE, k.s.c_str(), len);
            insert_pos += VARCHAR_LENGTH_SIZE + len;
    }

    if (it.page_type == LeafPage) {
        memcpy(insert_pos, &v.rid, sizeof(RID));
    } else {
        memcpy(insert_pos, &v.pnum, sizeof(page_pointer_t));
    }

    setOffset(getOffset() + entry_size);
};

IndexManager::IndexPage IndexManager::IndexPage::split(iterator &it) {
    size_t removed_data = getOffset() - it.getOffset();
    setOffset(getOffset() - removed_data);

    if (it.page_type == InternalPage) {
        return {getType(), it.where - sizeof(page_pointer_t), removed_data + sizeof(page_pointer_t)};
    }
    return {getType(), it.where, removed_data};
}

IndexManager::IndexPage::iterator IndexManager::IndexPage::find(AttrType attr_type, key &search_key) const {
    iterator it = begin(attr_type);
    while (it != end(attr_type)) {
        switch (it.attr_type) {
            case AttrType::TypeInt:
                if (it.getKey().i >= search_key.i) return it;
                break;
            case AttrType::TypeReal:
                if (it.getKey().r >= search_key.r) return it;
                break;
            case AttrType::TypeVarChar:
                if (search_key.s.compare(it.getKey().s) >= 0) return it;
                break;
        }
    }
    return it;
};

//IndexManager::IndexPage::iterator ====================================================================

IndexManager::IndexPage::value IndexManager::IndexPage::iterator::getValue() const {
    if (page_type == LeafPage) {
        RID r;
        memcpy(&r, where, sizeof(RID));
        return {.rid = r};
    } else {
        page_pointer_t p;  //In internal pages value is before key
        memcpy(&p, where - sizeof(page_pointer_t), sizeof(page_pointer_t));
        return {.pnum = p};
    }
};

IndexManager::IndexPage::key IndexManager::IndexPage::iterator::getKey() const {
    switch (attr_type) {
        case AttrType::TypeInt: {
            unsigned i;
            memcpy(&i, where, INT_SIZE);
            return {.i = i};
        }
        case AttrType::TypeReal: {
            float r;
            memcpy(&r, where, REAL_SIZE);
            return {.r = r};
        }
        case AttrType::TypeVarChar: {
            unsigned len;
            memcpy(&len, where, VARCHAR_LENGTH_SIZE);
            char *str = where + VARCHAR_LENGTH_SIZE;
            return {.s = {str, len}};
        }
        default:
            return {.i = 0};
    }
};

const size_t IndexManager::IndexPage::iterator::calcNextKeySize() const {
    switch (attr_type) {
        case AttrType::TypeInt:
            return INT_SIZE;
        case AttrType::TypeReal:
            return REAL_SIZE;
        case AttrType::TypeVarChar:
            unsigned varchar_length = 0;
            memcpy(&varchar_length, where, VARCHAR_LENGTH_SIZE);
            return VARCHAR_LENGTH_SIZE + varchar_length;
    }
    return 0;
};

const size_t IndexManager::IndexPage::iterator::calcNextEntrySize() const {
    size_t data_size = (page_type == LeafPage) ? sizeof(RID) : sizeof(page_pointer_t);
    return data_size + calcNextKeySize();
}