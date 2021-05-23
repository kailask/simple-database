#include "ix.h"

#include <cstring>

//IndexManager  ========================================================================================

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
    //get the path taken to get page with key/entry record and init helper variables
    vector<page_pointer_t> path = search(attribute.type, const_cast<void *>(key), ixfileHandle);
    page_pointer_t currPageRef = path.back();
    page_pointer_t splitPageRef = ixfileHandle.fileHandle.getNumberOfPages();
    path.pop_back();
    IndexPage page(ixfileHandle.fileHandle, currPageRef);

    //create key/value structs
    IndexPage::key k = createKey(attribute.type, const_cast<void *>(key));
    IndexPage::value v{.rid = rid};

    //while there's not enough space for new insertion
    PageType type = LeafPage;
    while (getRecordSize(k, attribute.type, v, type) + page.getOffset() > PAGE_SIZE) {
        //find the splitting point
        auto it = page.begin(attribute.type);
        uint32_t middle = ceil((page.getOffset() - it.getOffset()) / 2);
        while (it != page.end(attribute.type)) {
            if (it.getOffset() >= middle) break;
            ++it;
        }

        //split at iterator and modify values for splitPage
        IndexPage splitPage = page.split(it);

        //adjust references in splitPage if leaf page
        if (splitPage.getType() == LeafPage) {
            splitPage.setPrevPage(currPageRef);
            splitPage.setNextPage(page.getNextPage());

            //get the right page of currPage to modify its left reference and commit to disk
            if (page.getNextPage() != NULL_PAGE) {
                IndexPage rightLeaf(ixfileHandle.fileHandle, page.getNextPage());
                rightLeaf.setPrevPage(splitPageRef);
                if (rightLeaf.write(ixfileHandle.fileHandle, page.getNextPage()) != SUCCESS) return FAILURE;
            }

            //modify page's right ref
            page.setNextPage(splitPageRef);
        }

        //figure out which page the new key/value should be inserted at
        it = page.find(attribute.type, k);
        if (it != page.end(attribute.type)) {
            page.insert(it, k, v);
        } else {
            it = splitPage.find(attribute.type, k);
            splitPage.insert(it, k, v);
        }

        //get the beginning of right page to update parent page
        it = splitPage.begin(attribute.type);
        k = it.getKey();
        v = {.pnum = splitPageRef};

        //erase the first entry in the splitPage if internal page
        if (splitPage.getType() == InternalPage) {
            splitPage.erase(it);
        }

        //if the page isn't root
        if (currPageRef != 0) {
            //commit pages to disk
            if (page.write(ixfileHandle.fileHandle, currPageRef) != SUCCESS) return FAILURE;
            if (splitPage.write(ixfileHandle.fileHandle) != SUCCESS) return FAILURE;  //no pageRef assigned since it's being appended

            //update variables
            type = InternalPage;
            currPageRef = path.back();
            page.setData(ixfileHandle.fileHandle, currPageRef);
            splitPageRef = ixfileHandle.fileHandle.getNumberOfPages();
            path.pop_back();
        } else {
            //in this scenario a new root is created with left and right pageRefs referring to the old root and the split page
            page_pointer_t initial_pointer = splitPageRef + 1;
            IndexPage root(InternalPage, &initial_pointer, sizeof(initial_pointer));
            it = root.begin(attribute.type);
            root.insert(it, k, v);

            //commit pages to disk
            if (root.write(ixfileHandle.fileHandle, 0) != SUCCESS) return FAILURE;
            if (splitPage.write(ixfileHandle.fileHandle) != SUCCESS) return FAILURE;
            return page.write(ixfileHandle.fileHandle);
        }
    }

    //at while loop exit find the spot for insertion and insert
    auto it = page.find(attribute.type, k);
    page.insert(it, k, v);

    //write to disk
    return page.write(ixfileHandle.fileHandle, currPageRef);
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
    //have scan pass values to the iterator
    return ix_ScanIterator.scanInit(ixfileHandle, attribute.type, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    printHelper(0, ixfileHandle, attribute.type, 0);
}

//ScanIterator ========================================================================================

IX_ScanIterator::IX_ScanIterator() {
    im = IndexManager::instance();
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    //adjust startPage, start iterator, and temp
    if(start == temp.end(attrType)) {
        if(startPage == endPage) {
            return IX_EOF;
        }
        startPage = temp.getNextPage();
        temp.setData(ix.fileHandle, startPage);
        start = temp.begin(attrType);
    }

    auto key_ = start.getKey();
    auto value = start.getValue();
    //retrive entry as normal
    if(startPage != endPage) {
        rid.pageNum = value.rid.pageNum;
        rid.slotNum = value.rid.slotNum;
        formatKey(key_, key);
        ++start;
        return SUCCESS;
    } else {
        //you are at the last page
        if(highKeyInclusive) {
            if(keyCmpLessEqual(key_, im->createKey(attrType, const_cast<void*>(highKey)))) {
                rid.pageNum = value.rid.pageNum;
                rid.slotNum = value.rid.slotNum;
                formatKey(key_, key);
                ++start;
                return SUCCESS;
            } else {
                return IX_EOF;
            }
        } else {
            if(keyCmpLess(key_, im->createKey(attrType, const_cast<void*>(highKey)))) {
                rid.pageNum = value.rid.pageNum;
                rid.slotNum = value.rid.slotNum;
                formatKey(key_, key);
                ++start;
                return SUCCESS;
            } else {
                return IX_EOF;
            }
        }
    }

    return IX_EOF;
}

RC IX_ScanIterator::close() {
    return -1;
}

RC IX_ScanIterator::scanInit(IXFileHandle &ixfileHandle_,
        const AttrType &attrType_,
        const void *lowKey_,
        const void *highKey_,
        bool lowKeyInclusive_,
        bool highKeyInclusive_) {
    //set all the private variables
    ix = ixfileHandle_;
    attrType = attrType_;
    lowKey = lowKey_;
    highKey = highKey_;
    lowKeyInclusive = lowKeyInclusive_;
    highKeyInclusive = highKeyInclusive_;

    //set the initial state
    if(lowKey == nullptr) {
        startPage = getLeftPage();
    } else {
        startPage = im->search(attrType, const_cast<void*>(lowKey), ix).back();
    }
    temp = IndexManager::IndexPage(ix.fileHandle, startPage);
    start = temp.begin(attrType);

    if(highKey == nullptr) {
        endPage = getRightPage();
    } else {
        endPage = im->search(attrType, const_cast<void*>(highKey), ix).back();
    }

    if(!lowKeyInclusive) {
        auto currKey = start.getKey();
        while(start != temp.end(attrType) && im->areKeysEqual(attrType, currKey, im->createKey(attrType, const_cast<void*>(lowKey)))) {
            ++start;
        } 
    }

    return SUCCESS;
}

page_pointer_t IX_ScanIterator::getLeftPage() {
    IndexManager::IndexPage temp(ix.fileHandle, 0);
    page_pointer_t left = 0;
    while(temp.getType() != LeafPage) {
        auto it = temp.begin(attrType);
        left = it.getValue().pnum;
        temp.setData(ix.fileHandle, left);
    }
    return left;
}

page_pointer_t IX_ScanIterator::getRightPage() {
    IndexManager::IndexPage temp(ix.fileHandle, 0);
    page_pointer_t right = 0;
    while(temp.getType() != LeafPage) {
        auto it = temp.end(attrType);
        right = it.getValue().pnum;
        temp.setData(ix.fileHandle, right);
    }
    return right;
}

void IX_ScanIterator::formatKey(IndexManager::IndexPage::key k, void* dest) {
    switch (attrType) {
        case AttrType::TypeInt:
            memcpy(dest, &k.i, INT_SIZE);
            break;
        case AttrType::TypeReal:
            memcpy(dest, &k.r, REAL_SIZE);
            break;
        case AttrType::TypeVarChar:
            int length = k.s.length();
            memcpy(dest, &length, VARCHAR_LENGTH_SIZE);
            memcpy(static_cast<char*>(dest) + VARCHAR_LENGTH_SIZE, k.s.c_str(), length);
            break;
    }
}

bool IX_ScanIterator::keyCmpLess(IndexManager::IndexPage::key key1, IndexManager::IndexPage::key key2) {
    switch (attrType) {
        case AttrType::TypeInt:
            if (key1.i < key2.i) return true;
            break;
        case AttrType::TypeReal:
            if (key1.r < key2.r) return true;
            break;
        case AttrType::TypeVarChar:
            if (key1.s.compare(key2.s) < 0) return true;
            break;
    }
    return false;
}

bool IX_ScanIterator::keyCmpLessEqual(IndexManager::IndexPage::key key1, IndexManager::IndexPage::key key2) {
    switch (attrType) {
        case AttrType::TypeInt:
            if (key1.i <= key2.i) return true;
            break;
        case AttrType::TypeReal:
            if (key1.r <= key2.r) return true;
            break;
        case AttrType::TypeVarChar:
            if (key1.s.compare(key2.s) <= 0) return true;
            break;
    }
    return false;
}

//IXFileHandle =========================================================================================

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    fileHandle.collectCounterValues(ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter);
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

//IndexManager helper functions ========================================================================
bool IndexManager::areKeysEqual(AttrType attrType, IndexPage::key key1, IndexPage::key key2) const {
    switch (attrType) {
        case AttrType::TypeInt:
            if (key1.i == key2.i) return true;
            break;
        case AttrType::TypeReal:
            if (key1.r == key2.r) return true;
            break;
        case AttrType::TypeVarChar:
            if (key1.s == key2.s) return true;
            break;
    }
    return false;
}

void IndexManager::printKey(AttrType attrType, IndexPage::key k) const {
    switch (attrType) {
        case AttrType::TypeInt:
            cout << k.i;
            break;
        case AttrType::TypeReal:
            cout << k.r;
            break;
        case AttrType::TypeVarChar:
            cout << k.s;
            break;
    }
}

IndexManager::IndexPage::key IndexManager::createKey(AttrType attrType, void *key) {
    switch (attrType) {
        case AttrType::TypeInt: {
            signed i;
            memcpy(&i, key, INT_SIZE);
            return {.i = i};
        }
        case AttrType::TypeReal: {
            float r;
            memcpy(&r, key, REAL_SIZE);
            return {.r = r};
        }
        case AttrType::TypeVarChar: {
            unsigned len;
            memcpy(&len, key, VARCHAR_LENGTH_SIZE);
            char *str = static_cast<char *>(key) + VARCHAR_LENGTH_SIZE;
            return {.s = {str, len}};
        }
        default:
            return {.i = 0};
    }
}

vector<page_pointer_t> IndexManager::search(AttrType attrType, void *key, IXFileHandle &ixfileHandle) {
    //create indexPage object
    IndexPage temp(ixfileHandle.fileHandle, 0);
    vector<page_pointer_t> result;

    //parse the search key into a struct
    IndexPage::key k = createKey(attrType, key);

    //start at the root
    result.push_back(0);
    while (temp.getType() != LeafPage) {
        IndexPage::iterator it = temp.find(attrType, k);
        /*
            case 1: it = end, so no need to increment
            case 2: it != end, so no need to increment unless search keys are equal
        */
        if (it != temp.end(attrType)) {
            if (areKeysEqual(attrType, it.getKey(), k)) ++it;
        }

        result.push_back(it.getValue().pnum);
        temp.setData(ixfileHandle.fileHandle, it.getValue().pnum);
    }

    //return the leaf page
    return result;
}

ssize_t IndexManager::getRecordSize(IndexPage::key k, AttrType attrType, IndexPage::value v, PageType pageType) {
    ssize_t keySize = 0;
    switch (attrType) {
        case AttrType::TypeInt:
            keySize += INT_SIZE;
            break;
        case AttrType::TypeReal:
            keySize += REAL_SIZE;
            break;
        case AttrType::TypeVarChar:
            keySize += VARCHAR_LENGTH_SIZE;
            keySize += k.s.length();
            break;
    }

    size_t dataSize = (pageType == LeafPage) ? sizeof(RID) : sizeof(page_pointer_t);
    return keySize + dataSize;
}

void IndexManager::printHelper(int numSpaces, IXFileHandle &ixfileHandle, AttrType attrType, page_pointer_t currPageNum) const {
    //read the page into IndexPage
    IndexPage page(ixfileHandle.fileHandle, currPageNum);
    cout << string(numSpaces, ' ') << "{\"keys\":";

    //base case
    if (page.getType() == LeafPage) {
        auto it = page.begin(attrType);
        auto prevKey = it.getKey();

        cout << " [\"";
        printKey(attrType, prevKey);
        cout << ": [";

        string comma = "";
        while (it != page.end(attrType)) {
            auto key = it.getKey();
            auto val = it.getValue();

            if (areKeysEqual(attrType, prevKey, key)) {
                cout << comma << "(" << val.rid.pageNum << "," << val.rid.slotNum << ")";
                comma = ", ";
            } else {
                cout << "]\",\"";
                printKey(attrType, key);
                cout << ":["
                     << "(" << val.rid.pageNum << "," << val.rid.slotNum << ")";
            }
            prevKey = key;
            ++it;
        }

        cout << "]\"]}," << endl;
        return;
    }

    //recursive case
    auto it = page.begin(attrType);
    cout << "[";
    string comma = "";
    while (it != page.end(attrType)) {
        auto key = it.getKey();
        cout << comma << "\"";
        printKey(attrType, key);
        cout << "\"";
        comma = ",";
        ++it;
    }

    cout << "]," << endl;
    cout << string(numSpaces, ' ') << " \"children\": [" << endl;

    //depth first search through children
    it = page.begin(attrType);
    while (it != page.end(attrType)) {
        printHelper(numSpaces + 4, ixfileHandle, attrType, it.getValue().pnum);
        ++it;
    }

    //call once more for rightmost pageRef
    printHelper(numSpaces + 4, ixfileHandle, attrType, it.getValue().pnum);

    cout << string(numSpaces, ' ') << "]}," << endl;
}

//IndexManager::IndexPage ==============================================================================

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
        data_offset += sizeof(page_pointer_t) * 2;  //Leaf pages have extra page pointers
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

IndexManager::IndexPage::iterator IndexManager::IndexPage::begin(AttrType attr_type) const {
    PageType page_type = getType();

    //Iterator always points to start of key
    size_t it_pos = sizeof(page_metadata_t);  //Data for all page types
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
            break;
        case AttrType::TypeReal:
            entry_size += REAL_SIZE;
            memmove(it.where + entry_size, it.where, bytes_to_move);
            memcpy(insert_pos, &k.r, REAL_SIZE);
            insert_pos += REAL_SIZE;
            break;
        case AttrType::TypeVarChar:
            unsigned len = k.s.length();
            entry_size += VARCHAR_LENGTH_SIZE + len;
            memmove(it.where + entry_size, it.where, bytes_to_move);
            memcpy(insert_pos, &len, VARCHAR_LENGTH_SIZE);
            memcpy(insert_pos + VARCHAR_LENGTH_SIZE, k.s.c_str(), len);
            insert_pos += VARCHAR_LENGTH_SIZE + len;
            break;
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
                if (it.getKey().s.compare(search_key.s) >= 0) return it;
                break;
        }
        ++it;
    }
    return it;
};

//IndexManager::IndexPage::iterator ====================================================================

IndexManager::IndexPage::value IndexManager::IndexPage::iterator::getValue() const {
    if (page_type == LeafPage) {
        RID r;
        memcpy(&r, where + calcNextKeySize(), sizeof(RID));
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
            signed i;
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
