#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan

#define SUCCESS 0
#define FAILURE 1

#define INVALID_PAGE -1

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

   protected:
    IndexManager();
    ~IndexManager();

   private:
    static IndexManager *_index_manager;

   public:
    class IndexPage {
       public:
        //Page iterator
        class iterator;

        enum PAGE_TYPE {
            LEAF_PAGE,
            INTERNAL_PAGE
        };

        //Page attribute types
        typedef uint32_t page_metadata_t;
        typedef int32_t page_pointer_t;

        //Bitmasks for metadata
        static const uint32_t offset_mask = 0x7FFFFFFF;
        static const uint32_t type_mask = 0x80000000;

        //Constructors
        IndexPage(FileHandle &file, ssize_t page_num = -1);              //Read in page
        IndexPage(PAGE_TYPE type, void *initial_data, size_t data_size,  //Create new page
                  page_pointer_t next_ = INVALID_PAGE, page_pointer_t prev_ = INVALID_PAGE);
        ~IndexPage();

        //Iterator
        iterator begin(Attribute &attr);
        iterator end(Attribute &attr);
        // void insert(iterator &it);
        // void erase(iterator &it);

        //Commit file to disk
        RC write(FileHandle &file, ssize_t page_num = -1);

        PAGE_TYPE getType() const { return (*metadata & type_mask) ? LEAF_PAGE : INTERNAL_PAGE; }
        uint32_t getOffset() const { return *metadata & offset_mask; }
        page_pointer_t getNextPage() const { return *next; }
        page_pointer_t getPrevPage() const { return *prev; }

        RC setData(FileHandle &file, size_t page_num);
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
        iterator(Attribute &attr_, PAGE_TYPE type_, char *where_) : attr(attr_), type(type_), where(where_){};

        iterator &operator++();
        iterator &operator*();
        bool operator==(const iterator &that) const {
            return this->where == that.where;
        }
        bool operator!=(const iterator &that) const {
            return this->where != that.where;
        }

       private:
        const Attribute attr;
        const PAGE_TYPE type;
        char *where;
    };
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

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
};

#endif
