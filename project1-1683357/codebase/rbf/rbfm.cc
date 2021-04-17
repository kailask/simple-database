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
    return -1;
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
    memcpy(dest, record, nullLength);
    dest += nullLength;

    char *fieldStart = &record[nullLength + dirLength];
    for (size_t index = 0; index < numFields; index++) {
        if (record[index / CHAR_BIT] >> (index % CHAR_BIT) & 1) continue;  //Skip if null

        //Get field offset
        field_offset_t offset;
        memcpy(&offset, &dirStart[index * sizeof(offset)], sizeof(offset));

        char *fieldEnd = &record[offset];
        unsigned fieldLength = fieldEnd - fieldStart;

        //Copy varchar length
        if (recordDescriptor[index].type == AttrType::TypeVarChar) {
            memcpy(dest, &fieldLength, sizeof(fieldLength));
            dest += sizeof(fieldLength);
        }

        //Copy field
        memcpy(dest, fieldStart, fieldLength);
        dest += fieldLength;
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

        if (record[index / CHAR_BIT] >> (index % CHAR_BIT) & 1) {
            //Field is null
            cout << "NULL ";
        } else {
            //Determine field type
            switch (recordDescriptor[index].type) {
                case AttrType::TypeInt: {
                    int i;
                    memcpy(&i, fieldStart, sizeof(i));
                    fieldStart += sizeof(i);
                    cout << i << " ";
                    break;
                }
                case AttrType::TypeReal: {
                    float f;
                    memcpy(&f, fieldStart, sizeof(f));
                    fieldStart += sizeof(f);
                    cout << f << " ";
                    break;
                }
                case AttrType::TypeVarChar: {
                    //Get length
                    unsigned len;
                    memcpy(&len, fieldStart, sizeof(len));
                    fieldStart += sizeof(len);

                    //Get string
                    char s[len + 1];
                    memcpy(s, fieldStart, len);
                    fieldStart += len;
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
