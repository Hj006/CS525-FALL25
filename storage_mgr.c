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

    size_t readBytes = fread(header, sizeof(char), PAGE_SIZE, file);  // read a whole page (4096 bytes) of data from the file and put it into the header buffer
    if (readBytes != PAGE_SIZE) {
        free(header);
        fclose(file);
        return RC_READ_NON_EXISTING_PAGE;
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
        return RC_FILE_NOT_FOUND;       // destroy failure: file is not found, file is not removable
    }
}


/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /*
        The method reads the block at position pageNum from a file and stores its content in the memory pointed to by the memPage page handle.
        If the file has less than pageNum pages, the method should return RC READ NON EXISTING PAGE
        The physical offset is pageNum + 1 because we have a header page.
    */

    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // move the file pointer to the right physical location (skip the header block)
    FILE *file = (FILE *) fHandle->mgmtInfo;
    long offset = (pageNum + 1) * PAGE_SIZE;                // +1 because page 0 is the header
    int successfulMove = fseek(file, offset, SEEK_SET);     // fseek() return 0 if the move worked

    if (successfulMove == 0) { 
        
        // successfully, read the page content into memory
        size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, file);
        if (bytesRead != PAGE_SIZE) {
            return RC_READ_NON_EXISTING_PAGE;
        }
        
        // we already moved the file pointer with fseek,
        // update the current page position in the file handle
        fHandle->curPagePos = pageNum;    

        return RC_OK;

    } else {
        return RC_READ_NON_EXISTING_PAGE;
    }

}

int getBlockPos(SM_FileHandle *fHandle) {
    // return the current page position in a file

    if (fHandle == NULL) {
        return -1;          // file handle not initialized
    }

    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /*
        Read the first page in a file
    */ 
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // read the first data page of the file
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {


    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int target = fHandle->curPagePos - 1;   // previous boclk number = current block number -1
    if (target < 0) {                       // there is no "previous" page to read
        return RC_READ_NON_EXISTING_PAGE;
    }

    return readBlock(target, fHandle, memPage);

}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int target = fHandle->curPagePos;
    return readBlock(target, fHandle, memPage);

}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int target = fHandle->curPagePos + 1;
    if (target >= fHandle->totalNumPages) {   // if the next page number goes beyond totalNumPages, it means we are already at the last page and there is no next page
        return RC_READ_NON_EXISTING_PAGE;
    }

    return readBlock(target, fHandle, memPage);

}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int target = fHandle->totalNumPages - 1;    // page numbers start at 0, therefore we -1
    return readBlock(target, fHandle, memPage);
}

/* writing blocks to a page file */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /*
        Write a page to disk using either the current position or an absolute position
        write memPage data to file[pageNum]
        On success, update curPagePos
    */

    // error checks: 
    // file handle must be valid (not NULL) 
    // pageNum must be within the range of existing pages
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE; 
    }

    FILE *file = (FILE *) fHandle->mgmtInfo;
    long offset = (pageNum + 1) * PAGE_SIZE; // +1 to skip header
    int successfulMove = fseek(file, offset, SEEK_SET);
    
    if ( successfulMove != 0) {

        return RC_WRITE_FAILED;

    } else{

        // write one full page in a single fwrite call
        size_t written = fwrite(memPage, sizeof(char), PAGE_SIZE, file);

        if (written != PAGE_SIZE) {
            return RC_WRITE_FAILED;
        }

        fflush(file);                        // flush buffer to disk
        fHandle->curPagePos = pageNum;      // update current page position

        return RC_OK;        
    }
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    
    // error checks
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int current = fHandle->curPagePos;     // get the current page number from the file handle
    RC result = writeBlock(current, fHandle, memPage);

    if (result != RC_OK) {
        return RC_WRITE_FAILED;
    }
    return RC_OK;
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {

    // error checks
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    FILE *file = (FILE *) fHandle->mgmtInfo;

    // allocate one page of memory initialized with zeros
    SM_PageHandle emptyPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
    if (emptyPage == NULL) {
        return RC_WRITE_FAILED;
    }

    // move file pointer to the end of the file
    int successfulMove = fseek(file, 0, SEEK_END);
    if (successfulMove != 0) {
        free(emptyPage);
        return RC_WRITE_FAILED;
    }

    // write the empty page to the end of the file
    size_t written = fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    free(emptyPage);

    if (written != PAGE_SIZE) {
        return RC_WRITE_FAILED;
    }

    fflush(file);                       // make sure data is flushed to disk
    fHandle->totalNumPages += 1;        // update the total number of pages in the file

    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {

    // error checks    
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    while (fHandle->totalNumPages < numberOfPages) {
        // keep appending empty pages until the file has at least numberOfPages pages
        RC rc = appendEmptyBlock(fHandle);
        if (rc != RC_OK) {
            return rc; // if appending fails, return the error immediately
        }
    }

    return RC_OK;
}

/*

    make
    ./test_assign1_1

    make clean

 */