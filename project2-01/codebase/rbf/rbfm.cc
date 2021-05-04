#include "rbfm.h"

#include <math.h>
#include <string.h>

#include <bitset>

RBFM_ScanIterator::RBFM_ScanIterator(FileHandle &fileHandle,
                                     const vector<Attribute> &recordDescriptor,
                                     const string &conditionAttribute,
                                     const CompOp compOp,                     // comparision type such as "<" and "="
                                     const void *value,                       // used in the comparison
                                     const vector<string> &attributeNames) {  // a list of projected attributes
    this->fileHandle = fileHandle;
    this->recordDescriptor = recordDescriptor;
    this->conditionAttribute = conditionAttribute;
    this->compOp = compOp;
    this->value = value;
    this->attributeNames = attributeNames;
}

void RBFM_ScanIterator::getAttrInfo() {
    for(ssize_t index = 0; index < recordDescriptor.size(); index++) {
        if(recordDescriptor[index].name == conditionAttribute) {
            attrLength = recordDescriptor[index].length;
            attrType = recordDescriptor[index].type;
        }
    }
}

RC RBFM_ScanIterator::matchOperator(void *data, char *record, char *attribute) {
    //now that we have the record and attribute we need to check to see which operation matches as well as whether a record fits that operation
    switch(compOp) {
        case CompOp::EQ_OP:
            break;
        case CompOp::GE_OP:
            break;
        case CompOp::GT_OP:
            break;
        case CompOp::LE_OP:
            break;
        case CompOp::LT_OP:
            break;
        case CompOp::NE_OP:
            break;
        case CompOp::NO_OP:
            break;
        default:
            return -1;
    }
    return 0;
}

RC RBFM_ScanIterator::scanPage(RID &rid, char *page, char *dest) {
    // start checking from the rid
    MiniDirectory m{};
    memcpy(&m, &page[PAGE_SIZE - sizeof(MiniDirectory)], sizeof(MiniDirectory));

    Slot s{};
    while (tracker.slotNum < m.slotCount) {
        //read current slot
        memcpy(&s, &page[PAGE_SIZE - sizeof(MiniDirectory) - ((tracker.slotNum + 1) * sizeof(Slot))], sizeof(Slot));

        //if the slot is empty or a redirect continue
        if(s.offset < 0 || s.length == 0) {
            tracker.slotNum++;
            continue;
        }

        //otherwise first read record
        char record[PAGE_SIZE];
        if(RecordBasedFileManager::instance()->readRecordHelper(fileHandle, recordDescriptor, {(unsigned)s.offset, s.length}, record, 1, page) != 0) return -1;

        //read the attribute if name not NULL
        char attribute[attrLength + 1];
        if(value != NULL) {
            if(RecordBasedFileManager::instance()->readAttributeHelper(fileHandle, recordDescriptor, {(unsigned)s.offset, s.length}, conditionAttribute, attribute, 1, record) != 0) return -1;
        }

        //switch statement to check what kind of condition is used
        if(matchOperator(dest, record, attribute) == 0) {
            rid.pageNum = tracker.pageNum;
            rid.slotNum = tracker.slotNum;
            tracker.slotNum++;
            return 0;
        }

        tracker.slotNum++;
    }

    //couldn't find any matches on this page
    return -1;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    //read page containing the rid
    char *dest = static_cast<char *>(data);
    char page[PAGE_SIZE];
    if (fileHandle.readPage(tracker.pageNum, page) != 0) return -1;

    //populate the length and type of conditional variable
    getAttrInfo();

    //iterate through all pages to find a match
    unsigned numPages = fileHandle.getNumberOfPages();
    while(tracker.pageNum < numPages) {
        //if scanned page found a match return
        if(scanPage(rid, page, dest) == 0) {
            return 0;
        }

        //otherwise update the tracker rid 
        tracker.pageNum++;
        tracker.slotNum = 0;
    }

    //you've reached the end
    return -1;
}

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager *RecordBasedFileManager::instance() {
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
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
    char record[PAGE_SIZE];

    //create record and calculate size record takes up
    ssize_t recordSize;
    if ((recordSize = createRecord(recordDescriptor, data, record)) < 0) return -1;

    //write record into free page
    return writeRecord(fileHandle, record, rid, recordSize);
}

//have a function that calculates record size and creates the record to insert/update
ssize_t RecordBasedFileManager::createRecord(const vector<Attribute> &recordDescriptor, const void *data, void *record) {
    //find the size of recordBuff, create the record
    const char *dataBuff = static_cast<const char *>(data);
    char *recordBuff = static_cast<char *>(record);
    ssize_t numFields = recordDescriptor.size();
    size_t nullLength = ceil(static_cast<float>(numFields) / CHAR_BIT);
    ssize_t recordSize = nullLength + (sizeof(field_offset_t) * numFields);

    //copy null flags
    memWrite(recordBuff, dataBuff, nullLength);

    // copy dummy fieldOffset values to overwrite later
    field_offset_t fieldOffset = 0;
    for (ssize_t index = 0; index < numFields; index++) {
        memWrite(recordBuff, &fieldOffset, sizeof(field_offset_t));
    }

    //find the total size of record and store contents into recordBuff
    const char *fieldStart = &dataBuff[nullLength];
    fieldOffset = nullLength + (sizeof(field_offset_t) * numFields);
    char *fieldBuff = recordBuff - (sizeof(field_offset_t) * numFields);
    for (ssize_t index = 0; index < numFields; index++) {
        //check if null flag is set
        if ((dataBuff[index / CHAR_BIT] << (index % CHAR_BIT) & 0x80)) {
            memWrite(fieldBuff, &fieldOffset, sizeof(field_offset_t));
            continue;  //skip if null
        }

        //figure out the type of field
        switch (recordDescriptor[index].type) {
            case AttrType::TypeInt: {
                memRead(recordBuff, fieldStart, sizeof(int));
                recordBuff += sizeof(int);
                recordSize += sizeof(int);

                fieldOffset += sizeof(int);
                memWrite(fieldBuff, &fieldOffset, sizeof(field_offset_t));
                break;
            }
            case AttrType::TypeReal: {
                memRead(recordBuff, fieldStart, sizeof(float));
                recordBuff += sizeof(float);
                recordSize += sizeof(float);

                fieldOffset += sizeof(float);
                memWrite(fieldBuff, &fieldOffset, sizeof(field_offset_t));
                break;
            }
            case AttrType::TypeVarChar: {
                unsigned len;
                memRead(&len, fieldStart, sizeof(len));

                memRead(recordBuff, fieldStart, len);
                recordBuff += len;
                recordSize += len;
                fieldOffset += len;

                memWrite(fieldBuff, &fieldOffset, sizeof(field_offset_t));
                break;
            }
            default:
                return -1;
        }
    }

    return recordSize;
}

RC RecordBasedFileManager::writeRecord(FileHandle &fileHandle, void *data, RID &rid, ssize_t len) {
    //first get the number of pages
    unsigned numPages = fileHandle.getNumberOfPages();

    //if there are no pages
    if (numPages == 0) {
        return createRecordPage(fileHandle, data, rid, len, 0);
    }

    //if there are pages check the last one
    RC ret = isValidPage(fileHandle, numPages - 1, len, data, rid);
    if (ret == 1) {
        return 0;
    } else if (ret == -1) {
        return -1;
    }

    //if thats full iterate through all pages except last
    for (ssize_t index = 0; index < fileHandle.getNumberOfPages() - 1; index++) {
        RC ret = isValidPage(fileHandle, index, len, data, rid);
        if (ret == 1) {
            return 0;
        } else if (ret == -1) {
            return -1;
        }
    }

    //if you are outside the for loop then create a new page since all are not sufficient enough to store record
    return createRecordPage(fileHandle, data, rid, len, numPages);
}

RC RecordBasedFileManager::createRecordPage(FileHandle &fileHandle, void *data, RID &rid, ssize_t len, unsigned pageNum) {
    //create new mini directory
    MiniDirectory m = {
        static_cast<slot_offset_t>(PAGE_SIZE - (sizeof(MiniDirectory) + (2 * sizeof(Slot)))),
        1,
        static_cast<page_offset_t>(len)};

    //create new slot
    Slot s = {
        0,
        (unsigned)len};

    //create buffer
    char page[PAGE_SIZE];
    char *pageBuff = static_cast<char *>(page);

    //write record into page
    memcpy(pageBuff, data, len);

    //write slot and mini directory into page
    unsigned offset = PAGE_SIZE - sizeof(MiniDirectory) - sizeof(Slot);
    char *miniDirectory = &pageBuff[offset];
    memWrite(miniDirectory, &s, sizeof(Slot));
    memWrite(miniDirectory, &m, sizeof(MiniDirectory));

    //update rid
    rid.pageNum = pageNum;
    rid.slotNum = 0;

    //append page
    return fileHandle.appendPage(page);
}

RC RecordBasedFileManager::isValidPage(FileHandle &fileHandle, unsigned pageNum, ssize_t len, void *data, RID &rid) {
    //read page into buffer
    char page[PAGE_SIZE];
    if (fileHandle.readPage(pageNum, page) != 0) return -1;

    char *pageBuff = static_cast<char *>(page);
    const char *miniDirectory = &pageBuff[PAGE_SIZE - sizeof(MiniDirectory)];

    //calculate free space
    MiniDirectory m{};
    memRead(&m, miniDirectory, sizeof(MiniDirectory));

    ssize_t freeSpace = PAGE_SIZE - m.pageOffset - (sizeof(MiniDirectory) + (m.slotCount * sizeof(Slot)));

    //if there is enough space write record and write page
    if (freeSpace >= len + sizeof(Slot)) {
        //create slot and mini directory info
        slot_count_t slotCount_new = m.slotCount + 1;
        slot_offset_t slotOffset_new = m.slotOffset - sizeof(Slot);
        Slot emptySlot{};

        //check if there is a free slot
        if (m.slotOffset != (PAGE_SIZE - (sizeof(MiniDirectory) + (m.slotCount * sizeof(Slot)) + sizeof(Slot)))) {
            slotCount_new = m.slotCount;  //slotCount is unchanged

            //adjust pointers
            miniDirectory = &pageBuff[m.slotOffset];
            memRead(&emptySlot, miniDirectory, sizeof(Slot));
            slotOffset_new = emptySlot.offset;
        }

        MiniDirectory m_new{
            slotOffset_new,
            slotCount_new,
            static_cast<page_offset_t>(m.pageOffset + len)};

        Slot s = {
            m.pageOffset,
            (unsigned)len};

        //write record into page
        char *newRecord = &pageBuff[m.pageOffset];
        memcpy(newRecord, data, len);

        //update minidirectory
        newRecord = &pageBuff[m.slotOffset];
        memcpy(newRecord, &s, sizeof(Slot));

        newRecord = &pageBuff[PAGE_SIZE - sizeof(MiniDirectory)];
        memcpy(newRecord, &m_new, sizeof(MiniDirectory));

        //write page
        if (fileHandle.writePage(pageNum, page) != 0) return -1;

        //update rid
        rid.pageNum = pageNum;
        rid.slotNum = (((PAGE_SIZE - m.slotOffset) - sizeof(MiniDirectory)) / sizeof(Slot)) - 1;

        return 1;
    }

    //there was not enough space
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return readRecordHelper(fileHandle, recordDescriptor, rid, data, 0, nullptr);
}

RC RecordBasedFileManager::readRecordHelper(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data, int flag, char *page_) {
    char *dest = static_cast<char *>(data);
    char page[PAGE_SIZE];
    RC ret = -1;

    if(flag) {
        strcpy(page, page_);
    } else {
        ret = fileHandle.readPage(rid.pageNum, page);
    }

    if (ret != 0) return -1;  //Page number is invalid

    Slot s{};
    ret = parseSlot(page, rid.slotNum, s);
    if (ret != 0) return -1;                                                                                         //Slot number is invalid
    if (s.offset < 0) return readRecordHelper(fileHandle, recordDescriptor, {(unsigned)(s.offset * -1), s.length}, data, 0, page_);  //Slot was moved
    if (s.length == 0) return -1;                                                                                    //Slot was deleted

    char *record = &page[s.offset];
    size_t numFields = recordDescriptor.size();
    size_t nullLength = ceil(static_cast<float>(numFields) / CHAR_BIT);
    size_t dirLength = (numFields * sizeof(field_offset_t));
    char *dirStart = &record[nullLength];

    //Copy null bitmap
    memWrite(dest, record, nullLength);
    char *fieldStart = &record[nullLength + dirLength];
    for (size_t index = 0; index < numFields; index++) {
        if (record[index / CHAR_BIT] << (index % CHAR_BIT) & 0x80) continue;  //Skip if null

        //Get field offset
        field_offset_t offset;
        memcpy(&offset, &dirStart[index * sizeof(offset)], sizeof(offset));

        char *fieldEnd = &record[offset];
        unsigned fieldLength = fieldEnd - fieldStart;

        //Copy varchar length
        if (recordDescriptor[index].type == AttrType::TypeVarChar) {
            memWrite(dest, &fieldLength, sizeof(fieldLength));
        }

        //Copy field
        memWrite(dest, fieldStart, fieldLength);
        fieldStart = fieldEnd;
    }

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    const char *record = static_cast<const char *>(data);
    size_t numFields = recordDescriptor.size();
    size_t nullLength = ceil(static_cast<float>(numFields) / CHAR_BIT);

    const char *fieldStart = &record[nullLength];
    for (size_t index = 0; index < numFields; index++) {
        cout << recordDescriptor[index].name << ": ";

        if (record[index / CHAR_BIT] << (index % CHAR_BIT) & 0x80) {
            //Field is null
            cout << "NULL ";
        } else {
            //Determine field type
            switch (recordDescriptor[index].type) {
                case AttrType::TypeInt: {
                    int i;
                    memRead(&i, fieldStart, sizeof(i));
                    cout << i << " ";
                    break;
                }
                case AttrType::TypeReal: {
                    float f;
                    memRead(&f, fieldStart, sizeof(f));
                    cout << f << " ";
                    break;
                }
                case AttrType::TypeVarChar: {
                    //Get length
                    unsigned len;
                    memRead(&len, fieldStart, sizeof(len));

                    //Get string
                    char s[len + 1];
                    memRead(s, fieldStart, len);
                    s[len] = '\0';
                    cout << s << " ";
                    break;
                }
                default:
                    return -1;
            }
        }
    }
    cout << endl;
    return 0;
}

RC RecordBasedFileManager::parseSlot(char *page, unsigned slotNum, Slot &s) const {
    size_t slotCountOffset = PAGE_SIZE - sizeof(page_offset_t) - sizeof(slot_count_t);
    slot_count_t *slotCount = reinterpret_cast<slot_count_t *>(&page[slotCountOffset]);  //Assumed to be half-word aligned
    if (slotNum >= *slotCount) return -1;                                                //Invalid slot

    char *slot = &page[slotCountOffset - sizeof(slot_offset_t) - ((slotNum + 1) * sizeof(Slot))];
    memcpy(&s, slot, sizeof(Slot));  //Assumed to be stored without padding
    return 0;
}

void RecordBasedFileManager::memWrite(char *&dest, const void *src, size_t len) const {
    memcpy(dest, src, len);
    dest += len;
}

void RecordBasedFileManager::memRead(void *dest, const char *&src, size_t len) const {
    memcpy(dest, src, len);
    src += len;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    //read page
    char page[PAGE_SIZE];
    if (fileHandle.readPage(rid.pageNum, page) != 0) return -1;
    char *pageBuff = static_cast<char *>(page);

    //read mini directory
    const char *tempPageBuff = &pageBuff[PAGE_SIZE - sizeof(MiniDirectory)];
    MiniDirectory m{};
    memcpy(&m, tempPageBuff, sizeof(MiniDirectory));

    //read slot
    Slot s{};
    if (parseSlot(pageBuff, rid.slotNum, s) != 0) return -1;  //Slot number is invalid
    if (s.offset < 0) {                                       //Slot was moved
        /*
            case 2: RID redirected
            1) perform same steps as if RID not redirected
            2) once you return, reset slot that held redirect information and have it point to another freeSlot
        */
        if (deleteRecord(fileHandle, recordDescriptor, {(unsigned)(s.offset * -1), s.length}) != 0) {
            return -1;
        }

        Slot reset = {
            m.slotOffset,
            0};

        slot_offset_t slotOffset = PAGE_SIZE - sizeof(MiniDirectory) - ((rid.slotNum + 1) * sizeof(Slot));

        memcpy(&pageBuff[slotOffset], &reset, sizeof(Slot));
        memcpy(&pageBuff[PAGE_SIZE - sizeof(MiniDirectory)], &slotOffset, sizeof(slot_offset_t));

        //write page
        if (fileHandle.writePage(rid.pageNum, page) != 0) {
            return -1;
        }

        return 0;
    }
    if (s.length == 0) return -1;  //Slot was deleted

    /*
        case 1: RID not redirected
        1) create a dest buffer pointing to end of the deleted record
        2) save offset of the start of deleted record, memcpy the buffer to the start offset
        3) create new slot (slot offset, 0) and overwrite slot of the deleted record
        4) change the pageOffset, have freeSlot offset point to the start of the newly deleted slot
    */

    //copy records after deleted record to the deleted record offset
    tempPageBuff = &pageBuff[s.offset + s.length];
    memmove(&pageBuff[s.offset], tempPageBuff, m.pageOffset - (s.offset + s.length));

    //copy new slot and mini directory info
    Slot s_new = {
        m.slotOffset,
        0};

    MiniDirectory m_new = {
        static_cast<slot_offset_t>(PAGE_SIZE - sizeof(MiniDirectory) - ((rid.slotNum + 1) * sizeof(Slot))),
        m.slotCount,
        static_cast<page_offset_t>(m.pageOffset - s.length)};

    memcpy(&pageBuff[m_new.slotOffset], &s_new, sizeof(Slot));
    memcpy(&pageBuff[PAGE_SIZE - sizeof(MiniDirectory)], &m_new, sizeof(MiniDirectory));

    //write page
    return fileHandle.writePage(rid.pageNum, page);
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    //calculate record size and create the updated record to insert
    char record[PAGE_SIZE];
    RID copy1 = {rid.pageNum, rid.slotNum};
    RID copy2 = {rid.pageNum, rid.slotNum};

    //create record and calculate size record takes up
    ssize_t recordSize;
    if ((recordSize = createRecord(recordDescriptor, data, record)) < 0) return -1;

    return updateRecordHelper(fileHandle, recordDescriptor, record, copy1, copy2, 0, recordSize);
}

RC RecordBasedFileManager::updateRecordHelper(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                              void *record, RID &originalRid, RID &currentRid, int flag, ssize_t recordSize) {
    //read page
    char page[PAGE_SIZE];
    if (fileHandle.readPage(currentRid.pageNum, page) != 0) return -1;
    char *pageBuff = static_cast<char *>(page);

    //read mini directory
    const char *tempPageBuff = &pageBuff[PAGE_SIZE - sizeof(MiniDirectory)];
    MiniDirectory m{};
    memcpy(&m, tempPageBuff, sizeof(MiniDirectory));

    //read slot
    Slot s{};
    if (parseSlot(pageBuff, currentRid.slotNum, s) != 0) return -1;  //Slot number is invalid
    if (s.offset < 0) {                                              //Slot was moved
        //if the original page has enough space, insert updated record, delete the original record in the redirected page
        ssize_t freeSpace = PAGE_SIZE - m.pageOffset - (sizeof(MiniDirectory) + (m.slotCount * sizeof(Slot)));
        Slot s_new;

        if (freeSpace >= recordSize) {
            //insert the updated record
            memcpy(&pageBuff[m.pageOffset], record, recordSize);

            //create new slot and mini directory info
            s_new = {
                m.pageOffset,
                static_cast<unsigned>(recordSize)};
            page_offset_t newPageOffset = m.pageOffset + recordSize;

            //update page
            memcpy(&pageBuff[PAGE_SIZE - sizeof(MiniDirectory) - ((originalRid.slotNum + 1) * sizeof(Slot))], &s_new, sizeof(Slot));
            memcpy(&pageBuff[PAGE_SIZE - sizeof(page_offset_t)], &newPageOffset, sizeof(page_offset_t));

            //write page
            if (fileHandle.writePage(originalRid.pageNum, page) != 0) return -1;

            //delete record in the redirected page
            return deleteRecord(fileHandle, recordDescriptor, {(unsigned)(s.offset * -1), s.length});
        } else {
            //otherwise recur and update the redirected page
            currentRid.pageNum = (unsigned)(s.offset * -1);
            currentRid.slotNum = s.length;
            if (updateRecordHelper(fileHandle, recordDescriptor, record, originalRid, currentRid, 1, recordSize) != 0) return -1;

            //once you exit use the contents in currentRid to update the slot on the original page (gives info on the new pageNum, slotNum or it's unchanged)
            s_new = {
                (signed)currentRid.pageNum * -1,
                currentRid.slotNum};
            memcpy(&pageBuff[PAGE_SIZE - sizeof(MiniDirectory) - ((originalRid.slotNum + 1) * sizeof(Slot))], &s_new, sizeof(Slot));

            return fileHandle.writePage(originalRid.pageNum, page);
        }
    }
    if (s.length == 0) return -1;  //Slot was deleted

    /*
        1) if theres enough space
            a. move records after updated record accordingly
            b. copy in the updated record to the space created
            c. update page offset
        2) if there isnt enough space
            a. if the call wasn't from the redirected call
                i. create a temp RID, find an available page and insert record
                ii. remove record from the original page
                iii. update slot to contain temp RID info, update mini directory page offset
            b. if the call was from the redirected call
                i. if the original rid has enough space,
                    then insert the record, update original slot, update page offset,
                    remove record from redirected page and make slot empty + adjust page offset and pointers
                ii. if not, repeat steps in 2a except for step iii,
                    instead of iii, make slot empty slot + adjust page offset and pointers
                    change the original page's tombstone
    */

    //calulate free space
    ssize_t freeSpace = PAGE_SIZE - m.pageOffset - (sizeof(MiniDirectory) + (m.slotCount * sizeof(Slot))) + s.length;

    //if there's enough space
    if (freeSpace >= recordSize) {
        //shift records up or down
        tempPageBuff = &pageBuff[s.offset + s.length];
        memmove(&pageBuff[s.offset + recordSize], tempPageBuff, m.pageOffset - (s.offset + s.length));

        //insert the updated record
        memcpy(&pageBuff[s.offset], record, recordSize);

        //create new slot and mini directory info
        Slot s_new = {
            s.offset,
            static_cast<unsigned>(recordSize)};
        page_offset_t newPageOffset = m.pageOffset - s.length + recordSize;

        //update page
        memcpy(&pageBuff[PAGE_SIZE - sizeof(MiniDirectory) - ((currentRid.slotNum + 1) * sizeof(Slot))], &s_new, sizeof(Slot));
        memcpy(&pageBuff[PAGE_SIZE - sizeof(page_offset_t)], &newPageOffset, sizeof(page_offset_t));

        //write page
        return fileHandle.writePage(currentRid.pageNum, page);
    }

    //if there's not enough space and not a redirected call
    if (flag == 0) {
        //find a page to insert updated record
        RID temp{};
        if (writeRecord(fileHandle, record, temp, recordSize)) return -1;

        //remove original record from page
        tempPageBuff = &pageBuff[s.offset + s.length];
        memmove(&pageBuff[s.offset], tempPageBuff, m.pageOffset - (s.offset + s.length));

        //create new slot and mini directory info
        Slot s_new = {
            static_cast<signed>(temp.pageNum * -1),
            temp.slotNum};

        MiniDirectory m_new = {
            m.slotOffset,
            m.slotCount,
            static_cast<page_offset_t>(m.pageOffset - s.length)};

        //update page
        memcpy(&pageBuff[PAGE_SIZE - (sizeof(MiniDirectory) + ((originalRid.slotNum + 1) * sizeof(Slot)))], &s_new, sizeof(Slot));
        memcpy(&pageBuff[PAGE_SIZE - sizeof(MiniDirectory)], &m_new, sizeof(MiniDirectory));

        //write page
        return fileHandle.writePage(originalRid.pageNum, page);
    }

    //if there's not enough space and redirected call
    if (flag == 1) {
        //find a page to insert updated record
        RID temp{};
        if (writeRecord(fileHandle, record, temp, recordSize)) return -1;

        //delete record from page
        if (deleteRecord(fileHandle, recordDescriptor, {currentRid.pageNum, currentRid.slotNum}) != 0) return -1;

        currentRid = temp;
    }

    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
    return readAttributeHelper(fileHandle, recordDescriptor, rid, attributeName, data, 0, nullptr);
}

RC RecordBasedFileManager::readAttributeHelper(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data, int flag, char *record_) {
    //read record into buffer
    char record[PAGE_SIZE];
    char *recordBuff = static_cast<char *>(record);
    char *dest = static_cast<char *>(data);

    if(flag) {
        strcpy(record, record_);
    } else {
        if (readRecord(fileHandle, recordDescriptor, rid, record) != 0) return -1;
    }

    //find index corresponding to the attribute name
    ssize_t numFields = recordDescriptor.size();
    size_t nullLength = ceil(static_cast<float>(numFields) / CHAR_BIT);
    field_offset_t fieldStart = nullLength + (numFields * sizeof(Slot));
    field_offset_t fieldEnd;
    for (ssize_t index = 0; index < numFields; index++) {
        //get the field offset
        memcpy(&fieldEnd, &recordBuff[nullLength + (sizeof(field_offset_t) * index)], sizeof(field_offset_t));

        if (recordDescriptor[index].name == attributeName) {
            //write null bit to data
            uint8_t bitMap = (record[index / CHAR_BIT] << (index % CHAR_BIT) & 0x80);
            memWrite(dest, &bitMap, sizeof(uint8_t));

            //write attribute to dest
            memWrite(dest, &recordBuff[fieldStart], fieldEnd - fieldStart);

            return 0;
        }
        fieldStart = fieldEnd;
    }

    return -1;

}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,
                                const void *value,
                                const vector<string> &attributeNames,
                                RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator = RBFM_ScanIterator(fileHandle, recordDescriptor, conditionAttribute,
                                          compOp, value, attributeNames);
    return 0;
}