#ifndef _ix_h_
#define _ix_h_

#include <cstring>
#include <string>
#include <vector>

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

        //Bitmasks for metadata
        static const uint32_t offset_mask = 0x7FFFFFFF;
        static const uint32_t type_mask = 0x80000000;

        //Constructors
        IndexPage(FileHandle &file, size_t page_num);                   //Read in page
        IndexPage(PageType type, void *initial_data, size_t data_size,  //Create new page
                  page_pointer_t next_ = NULL_PAGE, page_pointer_t prev_ = NULL_PAGE);
        ~IndexPage() { delete data; };

        //Iterator
        iterator begin(AttrType attr_type);
        iterator end(AttrType attr_type);
        void insert(iterator &it, char *entry, size_t entry_size);
        RC erase(iterator &it);

        //Commit file to disk
        RC write(FileHandle &file, ssize_t page_num = -1) const;
        //Split data after iterator into new page
        IndexPage *split(iterator &it);

        PageType getType() const { return (*metadata & type_mask) ? LeafPage : InternalPage; }
        uint32_t getOffset() const { return *metadata & offset_mask; }
        page_pointer_t getNextPage() const { return *next; }
        page_pointer_t getPrevPage() const { return *prev; }

        RC setData(FileHandle &file, size_t page_num) { return file.readPage(page_num, data); };
        void setNextPage(page_pointer_t n) { *next = n; }
        void setPrevPage(page_pointer_t p) { *prev = p; }

       private:
        void setupPointers();
        void setOffset(uint32_t offset);

        char *data;
        page_metadata_t *metadata;

        //Leaf pages only
        page_pointer_t *next = NULL;
        page_pointer_t *prev = NULL;
    };

    class IndexPage::iterator {
       public:
        iterator(AttrType attr_type_, PageType page_type_, char *where_, const char *page_)
            : attr_type(attr_type_), page_type(page_type_), where(where_), page(page_){};

        //Get Value
        const char *get() const { return (page_type == InternalPage) ? where : where + calcNextKeySize(); };
        //Get Key
        const char *operator*() const { return (page_type == LeafPage) ? where : where + sizeof(page_pointer_t); };

        bool operator==(const iterator &that) const { return this->where == that.where; }
        bool operator!=(const iterator &that) const { return this->where != that.where; }
        iterator &operator++() {
            where += calcNextEntrySize();
            return *this;
        };

        size_t getOffset() const { return where - page; }

       private:
        friend class IndexPage;
        const AttrType attr_type;
        const PageType page_type;
        char *where;
        const char *page;

        const size_t calcNextKeySize() const {
            switch (attr_type) {
                case TypeInt:
                    return INT_SIZE;
                case TypeReal:
                    return REAL_SIZE;
                case TypeVarChar:
                    void *varchar_length_start = (page_type == LeafPage) ? where : where + sizeof(page_pointer_t);
                    unsigned varchar_length = 0;
                    memcpy(&varchar_length, varchar_length_start, VARCHAR_LENGTH_SIZE);
                    return VARCHAR_LENGTH_SIZE + varchar_length;
            }
            return 0;
        };

        const size_t calcNextEntrySize() const {
            size_t data_size = (page_type == LeafPage) ? sizeof(RID) : sizeof(page_pointer_t);
            return data_size + calcNextKeySize();
        }
    };

   protected:
    IndexManager();
    ~IndexManager();

   private:
    static IndexManager *_index_manager;
    static PagedFileManager *pfm;
    IndexPage search(Attribute &attr, void *key, IXFileHandle &ixfileHandle);
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
};

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

#endif
