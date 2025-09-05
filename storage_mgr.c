#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "storage_mgr.h"
#include "dberror.h"
/* Initial skeleton version */

/* manipulating page files */
void initStorageManager(void) {
    // 
}


/* create a file */
RC createPageFile(char *fileName) {
    // write a new file
    FILE *file = fopen(fileName, "wb+");

    // failed to open file
    if (file == NULL) { 
        //printf("fopen failed: %s\n", strerror(errno));
        return RC_FILE_NOT_FOUND;   
    }

    // allocate a page-sized (4096) buffer for the header page
    SM_PageHandle header = (SM_PageHandle) malloc(PAGE_SIZE);
    if (header == NULL) {
        fclose(file);
        return RC_WRITE_FAILED;
    }

    // initialize header with zero bytes
    for (int i = 0; i < PAGE_SIZE; i++) {
        header[i] = '\0';
    }

    // store total number of pages in header page
    int totalPages = 1;
    memcpy(header, &totalPages, sizeof(int)); 
    // header is like | 01 00 00 00 | 00 00 00 00 | ...all are 0... |

    // write header page to the file
    size_t headerWritten = fwrite(header, sizeof(char), PAGE_SIZE, file);
    free(header);
    if (headerWritten != PAGE_SIZE) {
        fclose(file);
        return RC_WRITE_FAILED;
    }
    
    
    // allocate one page-sized memory buffer for file operations
    // if malloc fails (out of memory), close the file and return an error 
    SM_PageHandle buffer = (SM_PageHandle) malloc(PAGE_SIZE);
    if (buffer == NULL) {
        fclose(file);
        return RC_WRITE_FAILED; 
    }

    for (int i = 0; i < PAGE_SIZE; i += 2) {
        buffer[i] = '\0';
        if (i + 1 < PAGE_SIZE) buffer[i + 1] = '\0';
    }

    // write PAGE_SIZE bytes to the file
    size_t written = fwrite(buffer, sizeof(char), PAGE_SIZE, file);
    free(buffer);
    if (written != PAGE_SIZE) {
        fclose(file);
        return RC_WRITE_FAILED;
    }
   
    // close the file
    fclose(file);
    return RC_OK;
}


RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    /* 
        Open an existing page file. 
        If the file does not exist, return "RC_FILE_NOT_FOUND".
        The second input parameter is the file handle to be filled.
        IF succeed, initialize all fields of the file handle with the file's info.   
    */
    FILE *file = fopen(fileName, "rb+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // allocate a page for reading header page
    SM_PageHandle header = (SM_PageHandle) malloc(PAGE_SIZE);
    if (header == NULL) {
        free(header);  
        fclose(file);
        return RC_WRITE_FAILED; 
    } 

    int totalPages;
    // when initializing, we stored total number of pages in the first int of header;
    // now we read it back into totalPages
    memcpy(&totalPages, header, sizeof(int)); 
    free(header);   

    // initial SM_FileHandle
    fHandle->fileName = fileName;
    fHandle->totalNumPages = totalPages;
    fHandle->curPagePos = 0;       //  Defualt is header page (number 0)
    fHandle->mgmtInfo = file;      //  FILE* pointer
    
    return RC_OK;
}


/* close a file */
RC closePageFile(SM_FileHandle *fHandle) {
    RC rc = RC_OK;

    // validate file handle: must not be NULL and must point to an open file
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        rc = RC_FILE_HANDLE_NOT_INIT;
        goto finally;
    }

    // close  the underlying FILE* 
    if (fclose((FILE *) fHandle->mgmtInfo) != 0) {
        rc = RC_WRITE_FAILED;
        goto finally;
    }

finally:
    // avoid dangling pointers
    if (fHandle != NULL) {
        fHandle->mgmtInfo = NULL;
    }

    return rc;
}

/* destroya file */
RC destroyPageFile(char *fileName) {

    if (fileName == NULL) {
        return RC_FILE_NOT_FOUND; 
    }
    // destroy the file
    int res = remove(fileName);
    if (res == 0) {
        return RC_OK;
    } else {
        return RC_FILE_NOT_FOUND; // destroy failure: file is not found, file is not removable
    }
}

/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // 
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle) {
    // 
    return 0;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // 
    return RC_OK;
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // 
    return RC_OK;
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // 
    return RC_OK;
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // 
    return RC_OK;
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // 
    return RC_OK;
}

/* writing blocks to a page file */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // 
    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // 
    return RC_OK;
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    // 
    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    // 
    return RC_OK;
}
