#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>



/*Buffer Manager Interface - Pool Handling*/ 


// Initialize a buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {
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