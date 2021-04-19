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
    unsigned numPages = getNumberOfPages();
    if ((numPages == pageNum && pageNum != 0) ||
        numPages < pageNum ||
        fseek(fd, pageNum * PAGE_SIZE, SEEK_SET) == -1 ||
        fread(data, PAGE_SIZE, 1, fd) != 1) {
        perror("readPage");
        return -1;
    }

    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    unsigned numPages = getNumberOfPages();
    if ((numPages == pageNum && pageNum != 0) ||
        numPages < pageNum ||
        fseek(fd, pageNum * PAGE_SIZE, SEEK_SET) == -1 ||
        fwrite(data, PAGE_SIZE, 1, fd) != 1) {
        perror("writePage");
        return -1;
    }

    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    if (fseek(fd, 0, SEEK_END) == -1 ||
        fwrite(data, PAGE_SIZE, 1, fd) != 1) {
        perror("appendPage");
        return -1;
    }

    appendPageCounter++;
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    int len;
    if (fseek(fd, 0, SEEK_END) == -1 ||
        (len = ftell(fd)) == -1) {
        perror("getNumberOfPages");
        return -1; //this might need to change?
    }

    return len / PAGE_SIZE;
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
