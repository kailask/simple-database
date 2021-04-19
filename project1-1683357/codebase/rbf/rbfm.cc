#include "rbfm.h"

#include <math.h>
#include <string.h>

#include <bitset>

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
    //find the size of recordBuff, store the nonNull attributes in order, create the record
    const char *dataBuff = static_cast<const char *>(data);
    ssize_t totalSize = 0;
    ssize_t numFields = recordDescriptor.size();
    ssize_t nullLength = ceil((double)recordDescriptor.size() / 8);
    totalSize += nullLength;
    totalSize += (sizeof(field_offset_t) * numFields);

    void *record = malloc(PAGE_SIZE);
    char *recordBuff = static_cast<char *>(record);

    //copy null flags
    memWrite(recordBuff, dataBuff, nullLength);

    // copy dummy fieldOffset values to overwrite later
    field_offset_t fieldOffset = 0;
    for(ssize_t index = 0; index < numFields; index++) {
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
                totalSize += sizeof(int);

                fieldOffset += sizeof(int);
                memWrite(fieldBuff, &fieldOffset, sizeof(field_offset_t));
                break;
            }
            case AttrType::TypeReal: {
                memRead(recordBuff, fieldStart, sizeof(float));
                recordBuff += sizeof(float);
                totalSize += sizeof(float);

                fieldOffset += sizeof(float);
                memWrite(fieldBuff, &fieldOffset, sizeof(field_offset_t));
                break;
            }
            case AttrType::TypeVarChar: {
                unsigned len;
                memRead(&len, fieldStart, sizeof(len));

                memRead(recordBuff, fieldStart, len);
                recordBuff += len;
                totalSize += len;
                fieldOffset += len;

                memWrite(fieldBuff, &fieldOffset, sizeof(field_offset_t));
                break;
            }
            default:
                return -1;
        }
    }

    cout << "total size is " << totalSize << endl;

    //write record into free page
    RC res = writeRecord(fileHandle, record, rid, totalSize);

    free(record);

    //write page back into file
    return (res == -1 ? -1 : 0);
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    char *dest = static_cast<char *>(data);
    char page[PAGE_SIZE];
    RC ret = fileHandle.readPage(rid.pageNum, page);
    if (ret != 0) return -1;  //Page number is invalid

    Slot s{};
    ret = parseSlot(page, rid.slotNum, s);
    if (ret != 0) return -1;                                                                      //Slot number is invalid
    if (s.redirect) return readRecord(fileHandle, recordDescriptor, {s.offset, s.length}, data);  //Slot was moved
    if (s.length == 0) return -1;                                                                 //Slot was deleted

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

    char *slot = &page[slotCountOffset - ((slotNum + 1) * sizeof(Slot))];
    memcpy(&s, slot, sizeof(Slot));  //Assumed to be stored without padding
    return 0;
}

RC RecordBasedFileManager::writeRecord(FileHandle &fileHandle, void *data, RID &rid, ssize_t len) {
    //first get the last page and check if that has enough space
    unsigned numPages = fileHandle.getNumberOfPages();

    //if there are no pages
    if (numPages == 0) {
        return createRecordPage(fileHandle, data, rid, len, 0);
    }

    //if there are pages check the last one
    


    //if thats full iterate through all pages
    // for (ssize_t index = 0; index < fileHandle.getNumberOfPages(); index++) {
    // }

    //if you are outside the for loop the create a new page since all are not sufficient enough to store record

    return 0;
}

RC RecordBasedFileManager::createRecordPage(FileHandle &fileHandle, void *data, RID &rid, ssize_t len, unsigned pageNum) {
    page_offset_t pageOffset = len;
    slot_count_t slotCount = 1;
    Slot s = {
        (unsigned) 0,
        (unsigned) pageOffset,
        0
    };

    //create buffer
    void *page = malloc(PAGE_SIZE);
    char *pageBuff = static_cast<char *>(page);

    //write record into page
    memcpy(pageBuff, data, len);

    //create mini directory
    unsigned offset = PAGE_SIZE - sizeof(page_offset_t) - sizeof(slot_count_t) - sizeof(Slot);

    cout << "the size of Slot is " << sizeof(Slot) << endl;

    cout << "offset is " << offset << endl;

    char *miniDirectory = &pageBuff[offset];
    memWrite(miniDirectory, &s, sizeof(Slot));
    memWrite(miniDirectory, &slotCount, sizeof(slot_count_t));
    memWrite(miniDirectory, &pageOffset, sizeof(page_offset_t));

    //append page
    if(fileHandle.appendPage(page) != 0) {
        return -1;
    }

    //update rid
    rid.pageNum = pageNum;
    rid.slotNum = 0;

    free(page);

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
