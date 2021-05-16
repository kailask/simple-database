#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan

#define SUCCESS 0
#define FAILURE 1

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

    class IndexPage {
       public:
        enum PAGE_TYPE {
            LEAF_PAGE,
            INTERNAL_PAGE
        };

        //Page attribute types
        typedef uint32_t page_metadata_t;
        typedef uint32_t page_pointer_t;

        IndexPage(FileHandle &file, ssize_t page_num = -1);               //Read in page
        IndexPage(PAGE_TYPE type, void *initial_data, size_t data_size);  //Create new page
        ~IndexPage();

        //Commit file to disk
        RC write(FileHandle &file, ssize_t page_num = -1);

        PAGE_TYPE getType() const { return (*metadata & 0x80000000) ? LEAF_PAGE : INTERNAL_PAGE; }
        uint32_t getOffset() const { return *metadata & 0x7FFFFFFF; }
        void setMetadata(PAGE_TYPE type, uint32_t offset);

       private:
        char *data;
        page_metadata_t *metadata;
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
