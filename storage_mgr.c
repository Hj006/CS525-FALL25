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

RC createPageFile(char *fileName) {
    // write a new file
    FILE *file = fopen(fileName, "wb+");
    // if failed on open a new file
    if (file == NULL) {
        printf("fopen failed: %s\n", strerror(errno));
        return RC_FILE_NOT_FOUND;   
    }

    // header
    SM_PageHandle header = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
    if (header == NULL) {
        fclose(file);
        return RC_WRITE_FAILED;
    }

    // write totalNumPages in header
    int totalPages = 1;
    memcpy(header, &totalPages, sizeof(int)); 
    // write header and free header
    size_t headerWritten = fwrite(header, sizeof(char), PAGE_SIZE, file);
    if (headerWritten != PAGE_SIZE) {
        fclose(file);
        return RC_WRITE_FAILED;
    }
    free(header);
    
    // malloc page size buffer 
    // if no enough page size buffer, the creating fail     
    SM_PageHandle buffer = (SM_PageHandle) malloc(PAGE_SIZE);
    if (buffer == NULL) {
        fclose(file);
        return RC_WRITE_FAILED; 
    }
    //TODO: write 0/ in file


    
    // write file and free buffer
    size_t written = fwrite(buffer, sizeof(char), PAGE_SIZE, file);
    if (written != PAGE_SIZE) {
        fclose(file);
        return RC_WRITE_FAILED;
    }
    free(buffer);

    // close the file
    fclose(file);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    //open file, if not found ,return RC_FILE_NOT_FOUND
    FILE *file = fopen(fileName, "rb+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    // malloc page size header
    SM_PageHandle header = (SM_PageHandle) malloc(PAGE_SIZE);
    if (header == NULL) {
        fclose(file);
        return RC_WRITE_FAILED; // malloc fail
    }    
    //get totalNumPages
    int totalPages;
    memcpy(&totalPages, header, sizeof(int)); // totalNumPages
    free(header);  
    // initial SM_FileHandle
    fHandle->fileName = fileName;
    fHandle->totalNumPages = totalPages;  // totalNumPages
    fHandle->curPagePos = 0;       // Page 0 = header
    fHandle->mgmtInfo = file;      //  FILE* pointer
    
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    // 
    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    // 
    return RC_OK;
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
