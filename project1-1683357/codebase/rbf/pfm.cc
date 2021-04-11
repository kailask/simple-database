#include "pfm.h"

PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance() {
    if (!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager() {
}

PagedFileManager::~PagedFileManager() {
}

RC PagedFileManager::createFile(const string &fileName) {
    int exists = access(fileName.c_str(), F_OK);

    if (exists == 0) {
        cout << "File " << fileName << " exists" << endl;
        return -1;
    }

    FILE *fileHandle = fopen(fileName.c_str(), "w");
    int res = fclose(fileHandle);

    if (res != 0) {
        cout << "File " << fileName << "didn't close properly" << endl;
        return -1;
    }

    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName) {
    int exists = access(fileName.c_str(), F_OK);

    if (exists != 0) {
        cout << "File " << fileName << "doesn't exist" << endl;
        return -1;
    }

    int res = remove(fileName.c_str());

    if (res != 0) {
        cout << "File " << fileName << " didn't delete properly" << endl;
        return -1;
    }

    return 0;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    int exists = access(fileName.c_str(), F_OK);

    if (exists != 0) {
        cout << "File " << fileName << "doesn't exist" << endl;
        return -1;
    }

    if (fileHandle.getfd() != NULL) {
        cout << "Filehandle for file " << fileName << " already exists" << endl;
        return -1;
    }

    FILE *fd = fopen(fileName.c_str(), "r+");
    fileHandle.setfd(fd);

    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if (fileHandle.getfd() == NULL) {
        cout << "Filehandle doesn't exist" << endl;
        return -1;
    }

    int res = fclose(fileHandle.getfd());

    if (res != 0) {
        cout << "File didn't close properly" << endl;
        return -1;
    }

    return 0;
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle() {
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    int file_no = fileno(fd);
    if (lseek(file_no, pageNum * PAGE_SIZE, SEEK_SET) == -1) {
        return -1;
    }

    if (read(file_no, data, PAGE_SIZE) != PAGE_SIZE) {
        return -1;
    }

    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    int file_no = fileno(fd);
    if (lseek(file_no, pageNum * PAGE_SIZE, SEEK_SET) == -1) {
        return -1;
    }

    if (write(file_no, data, PAGE_SIZE) != PAGE_SIZE) {
        return -1;
    }

    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    int file_no = fileno(fd);
    if (lseek(file_no, 0, SEEK_END) == -1) {
        return -1;
    }

    if (write(file_no, data, PAGE_SIZE) != PAGE_SIZE) {
        return -1;
    }

    appendPageCounter++;
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    return appendPageCounter;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

void FileHandle::setfd(FILE *fd_) {
    this->fd = fd_;
}

FILE *FileHandle::getfd() {
    return this->fd;
}
