#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


// Frame represents one page frame in the buffer pool
typedef struct Frame {
    PageNumber pageNum;   // current page number; if NO_PAGE, it's empty.
    char *data;           // pointer to the actual data
    bool dirty;           // dirty flag
    int fixCount;         // how many clients are using this page
    int ref;              // used for replacement strategies (e.g., LRU counter)
} Frame;

// PoolMgmtData stores various information required for the entire buffer pool to be maintained during runtime
typedef struct PoolMgmtData {
    Frame *frames;        // point to array of frames
    int numReadIO;        // number of pages read from disk
    int numWriteIO;       // number of pages written to disk
    int nextVictim;       // index used for FIFO replacement
    void *strategyData;   // store stratData, which will be used in LRU-K 
} PoolMgmtData;

/*Buffer Manager Interface - Pool Handling*/ 

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {
    /*
      Initialize a buffer pool for an existing page file.

      Allocate memory for PoolMgmtData and attach it to bm->mgmtData.
      Allocate an array of frames (numPages size).
      Initialize each frame:
          - pageNum = NO_PAGE (means empty)
          - allocate memory for data
          - dirty = false
          - fixCount = 0
          - ref = 0 (for replacement strategies later)
      Initialize counters: numReadIO = 0, numWriteIO = 0, nextVictim = 0.
      Set bm->pageFile, bm->numPages, bm->strategy.

     */

    // allocate memory for management data
    PoolMgmtData *mgmt = (PoolMgmtData *) malloc(sizeof(PoolMgmtData));
    if (mgmt == NULL) {
        return RC_WRITE_FAILED;     // failed
    }

    // allocate frames
    mgmt->frames = (Frame *) malloc(sizeof(Frame) * numPages);
    if (mgmt->frames == NULL) { // allocate failed, to aviod leaky, we release mgmt and return error 
        free(mgmt);
        return RC_WRITE_FAILED;
    }

    // initialize frames
    for (int i = 0; i < numPages; i++) {
        mgmt->frames[i].pageNum = NO_PAGE;
        mgmt->frames[i].data = (char *) malloc(PAGE_SIZE);
        mgmt->frames[i].dirty = false;
        mgmt->frames[i].fixCount = 0;
        mgmt->frames[i].ref = 0; // counter for LRU
    }

    // initialize counters
    mgmt->numReadIO = 0;
    mgmt->numWriteIO = 0;
    mgmt->nextVictim = 0; // FIFO pointer
    mgmt->strategyData = stratData;

    // attach the pointer
    bm->pageFile = (char *) pageFileName;   // file name
    bm->numPages = numPages;                // number of frames
    bm->strategy = strategy;                // replacement strategy
    bm->mgmtData = mgmt;                    // management data

    return RC_OK;
}

// Shut down a buffer pool and free all resources
RC shutdownBufferPool(BM_BufferPool *const bm) {
    return RC_OK;
}

// Force all dirty pages (with fix count = 0) to be written back to disk
RC forceFlushPool(BM_BufferPool *const bm) {
    return RC_OK;
}

/* Buffer Manager Interface - Page Access*/

// Mark a page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;
}

// Unpin a page (decrement its fix count)
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;
}

// Force a specific page to be written back to disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;
}

// Pin a page into the buffer pool
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
            const PageNumber pageNum) {
    return RC_OK;
}

/* Buffer Manager Interface - Statistics*/

// Return the page numbers stored in each frame
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    return NULL;
}

// Return the dirty flags for each frame
bool *getDirtyFlags (BM_BufferPool *const bm) {
    return NULL;
}

// Return the fix counts for each frame
int *getFixCounts (BM_BufferPool *const bm) {
    return NULL;
}

// Return the number of pages read from disk
int getNumReadIO (BM_BufferPool *const bm) {
    return 0;
}

// Return the number of pages written to disk
int getNumWriteIO (BM_BufferPool *const bm) {
    return 0;
}