#ifndef _ix_h_
#define _ix_h_

#include <cmath>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan

#define SUCCESS 0
#define FAILURE 1

#define NULL_PAGE -1

//Page attribute types
typedef uint32_t page_metadata_t;
typedef int32_t page_pointer_t;

//Index page types
enum PageType {
    LeafPage,
    InternalPage
};

class IX_ScanIterator;
class IXFileHandle;

class IXFileHandle {
   public:
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
};

class IndexManager {
   public:
    static IndexManager *instance();

    // Create an index file.
    RC createFile(const string &fileName);

    // Delete an index file.
    RC destroyFile(const string &fileName);

    // Open an index and return an ixfileHandle.
    RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

    // Close an ixfileHandle for an index.
    RC closeFile(IXFileHandle &ixfileHandle);

    // Insert an entry into the given index that is indicated by the given ixfileHandle.
    RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixfileHandle.
    RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixfileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    class IndexPage {
       public:
        //Page iterator
        class iterator;

        //Possible page values
        struct value {
            RID rid;
            page_pointer_t pnum;
        };

        //Possible page keys
        struct key {
            float r;
            signed i;
            string s;
        };

        //Bitmasks for metadata
        static const uint32_t offset_mask = 0x7FFFFFFF;
        static const uint32_t type_mask = 0x80000000;

        //Constructors
        IndexPage() {setupPointers();};
        IndexPage(FileHandle &file, page_pointer_t page_num);  //Read in page
        IndexPage(PageType type, void *initial_data, size_t data_size,
                  page_pointer_t next_ = NULL_PAGE, page_pointer_t prev_ = NULL_PAGE);
        ~IndexPage() { delete data; };

        //Iterator
        iterator begin(AttrType attr_type) const;
        iterator end(AttrType attr_type) const;
        iterator find(AttrType attr_type, key &search_key) const;
        void insert(iterator &it, key &k, value &v);
        RC erase(iterator &it);
        IndexPage split(iterator &it);  //Split data after iterator into new page

        //Commit file to disk
        RC write(FileHandle &file, ssize_t page_num = -1) const;

        PageType getType() const { return (*metadata & type_mask) ? LeafPage : InternalPage; }
        uint32_t getOffset() const { return *metadata & offset_mask; }
        page_pointer_t getNextPage() const { return *next; }
        page_pointer_t getPrevPage() const { return *prev; }

        RC setData(FileHandle &file, size_t page_num) { return file.readPage(page_num, data); };
        void setNextPage(page_pointer_t n) { *next = n; }
        void setPrevPage(page_pointer_t p) { *prev = p; }

       private:
        void setupPointers();
        void setOffset(uint32_t offset) { *metadata = (offset & offset_mask) | (*metadata & type_mask); }

        char *data;
        page_metadata_t *metadata;

        //Leaf pages only
        page_pointer_t *next = NULL;
        page_pointer_t *prev = NULL;
    };

    class IndexPage::iterator {
       public:
        value getValue() const;
        key getKey() const;
        size_t getOffset() const { return where - page; }

        bool operator==(const iterator &that) const { return this->where == that.where; }
        bool operator!=(const iterator &that) const { return this->where != that.where; }
        iterator &operator++() {
            where += calcNextEntrySize();
            return *this;
        };

        iterator() {};
        iterator(AttrType attr_type_, PageType page_type_, size_t offset, char *page_)
            : attr_type(attr_type_), page_type(page_type_), where(page_ + offset), page(page_){};

       private:
        friend class IndexPage;
        AttrType attr_type;
        PageType page_type;
        char *where;
        char *page;

        const size_t calcNextKeySize() const;
        const size_t calcNextEntrySize() const;
    };

   protected:
    IndexManager();
    ~IndexManager();

   private:
    friend class IX_ScanIterator;
    string fileName_;
    static IndexManager *_index_manager;
    static PagedFileManager *pfm;
    vector<page_pointer_t> search(AttrType attrType, void *key, IXFileHandle &ixfileHandle);
    IndexPage::key createKey(AttrType attrType, void *key);
    ssize_t getRecordSize(IndexPage::key k, AttrType attrType, IndexPage::value v, PageType pageType);
    void printHelper(int numSpaces, IXFileHandle &ixfileHandle, AttrType attrType, page_pointer_t currPageNum) const;
    bool areKeysEqual(AttrType attrType, IndexPage::key key1, IndexPage::key key2) const;
    void printKey(AttrType attrType, IndexPage::key k) const;
};

class IX_ScanIterator {
   public:
    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

   private:
    IndexManager *im;
    friend class IndexManager;
    IXFileHandle ix;
    AttrType attrType;
    const void* lowKey;
    const void* highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;
    IndexManager::IndexPage temp;
    page_pointer_t startPage;
    page_pointer_t endPage;
    IndexManager::IndexPage::iterator start;
    page_pointer_t getLeftPage();
    page_pointer_t getRightPage();
    RC scanInit(IXFileHandle &ixfileHandle_,
            const AttrType &attrType_,
            const void *lowKey_,
            const void *highKey_,
            bool lowKeyInclusive_,
            bool highKeyInclusive_);
    void formatKey(IndexManager::IndexPage::key k, void* dest);
    bool keyCmpLess(IndexManager::IndexPage::key key1, IndexManager::IndexPage::key key2);
    bool keyCmpLessEqual(IndexManager::IndexPage::key key1, IndexManager::IndexPage::key key2);
};

#endif
