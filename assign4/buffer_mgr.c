#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#define RC_PINNED_PAGES_IN_BUFFER 400
#define RC_BUFFER_POOL_NOT_INIT 5 

static long long globalLRUCounter = 0;

// Frame represents one page frame in the buffer pool
typedef struct Frame {
    PageNumber pageNum;   // current page number; if NO_PAGE, it's empty.
    char *data;           // pointer to the actual data
    bool dirty;           // dirty flag
    int fixCount;         // how many clients are using this page
    int ref;              // used for LRU counter
    int refBit;           // used for CLOCK
} Frame;

// PoolMgmtData stores various information required for the entire buffer pool to be maintained during runtime
typedef struct PoolMgmtData {
    Frame *frames;        // point to array of frames
    int numReadIO;        // number of pages read from disk
    int numWriteIO;       // number of pages written to disk
    int nextVictim;       // index used for FIFO replacement
    int clockHand;        // index used for CLOCK replacement
    void *strategyData;   // store stratData
} PoolMgmtData;

typedef struct LRUKData {
    int K;                     // K value
    long long **histories;     // 2D array [numPages][K], Each frame's access history
    int *historyCount;         // Number of valid accesses recorded for each frame
} LRUKData;

/*Pool Handling*/ 

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
      Initialize counters.
      Set bm->pageFile, bm->numPages, bm->strategy.
     */

    // test the input file if existing
    SM_FileHandle fh;
    RC rc = openPageFile((char *) pageFileName, &fh);
    if (rc != RC_OK) {
        return rc;  // return error
    }
    closePageFile(&fh);

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
        mgmt->frames[i].refBit = 0;  
    }

    // initialize counters
    mgmt->numReadIO = 0;
    mgmt->numWriteIO = 0;
    mgmt->nextVictim = 0; // FIFO pointer
    mgmt->clockHand = 0; // CLOCK pointer
    mgmt->strategyData = stratData;
    if (strategy == RS_LRU_K) {
        LRUKData *data = malloc(sizeof(LRUKData));
        data->K = 2; // set K = 2

        // initialize and allocate memory
        data->histories = malloc(sizeof(long long*) * numPages);
        data->historyCount = malloc(sizeof(int) * numPages);

        for (int i = 0; i < numPages; i++) {
            data->histories[i] = malloc(sizeof(long long) * data->K);
            for (int j = 0; j < data->K; j++) {
                data->histories[i][j] = -1;  
            }
            data->historyCount[i] = 0;
        }

        mgmt->strategyData = data;
    }

    // attach the pointer
    bm->pageFile = (char *) pageFileName;   // file name
    bm->numPages = numPages;                // number of frames
    bm->strategy = strategy;                // replacement strategy
    bm->mgmtData = mgmt;                    // management data

    return RC_OK;
}

// Shut down a buffer pool and free all resources
RC shutdownBufferPool(BM_BufferPool *const bm) {
    /*
      Safely close unused buffer pools
      Write all dirty pages back to disk
      Release all memory
      Clean up BM_BufferPool
    */

    // get the management data structure 
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;

    //  flush all dirty pages back to disk 
    SM_FileHandle fh;
    RC rc = openPageFile(bm->pageFile, &fh); 
    if (rc != RC_OK) {
        return rc;
    }

    for (int i = 0; i < bm->numPages; i++) {
        if (mgmt->frames[i].dirty == true) {
            // write the dirty page back to the page file
            writeBlock(mgmt->frames[i].pageNum, &fh, mgmt->frames[i].data);
            mgmt->numWriteIO++;
            mgmt->frames[i].dirty = false; 
        }
    }

    closePageFile(&fh);
    
    // if the strategy is LRU_K and pointer strategyData isn't null, free the struct LRUKData
    if (bm->strategy == RS_LRU_K && mgmt->strategyData != NULL) {
        LRUKData *data = (LRUKData *)mgmt->strategyData;
        for (int i = 0; i < bm->numPages; i++) {
            free(data->histories[i]);
        }
        free(data->histories);
        free(data->historyCount);
        free(data);
    }

    //  free all allocated memory
    for (int i = 0; i < bm->numPages; i++) {
        free(mgmt->frames[i].data);  // relese the data in the  frame
    }
    free(mgmt->frames);   // release frames 
    free(mgmt);           // release the management data

    //  clean up the buffer pool struct 
    bm->mgmtData = NULL;
    bm->pageFile = NULL;
    bm->numPages = 0;
    bm->strategy = 0;

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    /*
      look through all frames in the buffer pool
      for every frame that is dirty and not pinned (fixCount == 0),
      writes the page back to disk, increments the write I/O counter and resets the dirty flag
    */

    // check whether it is initialized
    if (bm == NULL || bm->mgmtData == NULL) {
        return RC_BUFFER_POOL_NOT_INIT; 
    }
    // get the management structure
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;

    // for loop all the frame
    for (int i = 0; i < bm->numPages; i++) {
        Frame *frame = &mgmt->frames[i];

        if (frame->pageNum != NO_PAGE){
            // for the dirty and unpinned frame
            if ( frame->dirty && frame->fixCount == 0) {
                
                // wirte back using writeBlock
                //RC rc = writeBlock(frame->pageNum, (SM_FileHandle *) bm->mgmtData, frame->data);
                SM_FileHandle fh;
                RC rc = openPageFile(bm->pageFile, &fh);
                if (rc != RC_OK) {
                    return rc;
                }                
                rc = writeBlock(frame->pageNum, &fh, frame->data);
                closePageFile(&fh);

                if (rc != RC_OK) {
                    return RC_WRITE_FAILED; 
                }

                // update I/O counter 
                mgmt->numWriteIO++;

                // clear the dirty flag
                frame->dirty = false;
            }
        }

    }

    return RC_OK;
}

/*Page Access*/

// Mark a page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {

    // get the management structure
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;

    // total number of frame
    int i = bm->numPages - 1;

    while (i >= 0) {
        if (mgmt->frames[i].pageNum == page->pageNum) {
            mgmt->frames[i].dirty = true;   // found the target page then mark it as dirty
            return RC_OK;
        }
        i--;  
    }

    // did not find return error
    return RC_READ_NON_EXISTING_PAGE;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    /*
      each time a page is unpinned, the "fixCount" of the page is reduced by 1, 
      indicating that a user/process is no longer using it.
    */

    // check whether it is initialized
    if (bm == NULL || bm->mgmtData == NULL) {
        return RC_BUFFER_POOL_NOT_INIT;
    }

    // get the management structure
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;
    
    // total number of frame
    int i = bm->numPages - 1;
    
    while (i >= 0) {
        if (mgmt->frames[i].pageNum == page->pageNum) {
            if (mgmt->frames[i].fixCount > 0) {
                //printf("Unpin page %d at frame %d, fixCount before: %d\n", page->pageNum, i, mgmt->frames[i].fixCount);
                mgmt->frames[i].fixCount--;
                //printf("Unpin page %d at frame %d, fixCount after: %d\n", page->pageNum, i, mgmt->frames[i].fixCount);
                return RC_OK;
            }
        }
        i--;
    }

    return RC_READ_NON_EXISTING_PAGE;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    /*
      forces a specific page to be written back to disk
      updates the numWriteIO
    */

    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;
    SM_FileHandle fh;
    RC rc = openPageFile(bm->pageFile, &fh);
    if (rc != RC_OK) return rc;

    int i = bm->numPages - 1;
    while (i >= 0) {
        if (mgmt->frames[i].pageNum == page->pageNum) {
            // write back to the disk
            writeBlock(page->pageNum, &fh, mgmt->frames[i].data);
            mgmt->numWriteIO++;
            mgmt->frames[i].dirty = false;
            closePageFile(&fh);
            return RC_OK;
        }
        i--;
    }

    closePageFile(&fh);
    return RC_READ_NON_EXISTING_PAGE; 
}


// Debug function: print K access history of all frames
/* 
static void printHistories(BM_BufferPool *const bm) {
    PoolMgmtData *mgmt = (PoolMgmtData *)bm->mgmtData;
    LRUKData *data = (LRUKData *)mgmt->strategyData;
    int K = data->K;

    printf("\n=== Histories Snapshot ===\n");
    for (int i = 0; i < bm->numPages; i++) {
        printf("Frame %d (page %d): ", i, mgmt->frames[i].pageNum);
        for (int j = 0; j < K; j++) {
            printf("%lld ", data->histories[i][j]);
        }
        printf("(count=%d)\n", data->historyCount[i]);
    }
    printf("==========================\n\n");
}
*/


// Pin a page into the buffer pool
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
            const PageNumber pageNum) {
                
    // check whether it is initialized                
    if (bm == NULL || bm->mgmtData == NULL) {
        return RC_BUFFER_POOL_NOT_INIT;
    }

    if (pageNum < 0) { // check pageNum
        return RC_READ_NON_EXISTING_PAGE; 
    }
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;

    //if page is already in buffer
    int i = bm->numPages - 1;   
    while (i >= 0) {
        if (mgmt->frames[i].pageNum == pageNum) {
            mgmt->frames[i].fixCount++;
            page->pageNum = pageNum;
            page->data = mgmt->frames[i].data;
            
            if (bm->strategy == RS_LRU) { // LRU strategy, count and see the time node of the call
                mgmt->frames[i].ref = globalLRUCounter++;
            }
            else if (bm->strategy == RS_CLOCK) { // CLOCK strategy, when a page is pinned, the ref should always be 1
                mgmt->frames[i].refBit = 1; // Set reference bit
            }
            else if (bm->strategy == RS_LFU) { // Least frequently used strategy
                mgmt->frames[i].ref++; // each time a page is found in the buffer, its frequency count is incremented by 1.
            }                
            else if (bm->strategy == RS_LRU_K) {
                LRUKData *data = (LRUKData *)mgmt->strategyData;
                int K = data->K;
                int idx = i; 
                globalLRUCounter++;

                if (data->historyCount[idx] < K) {
                    // not filled yet, add more directly
                    data->histories[idx][data->historyCount[idx]] = globalLRUCounter;
                    data->historyCount[idx]++;
                } else {
                    // alreday filled, left shift
                    for (int j = 0; j < K - 1; j++) {
                        data->histories[idx][j] = data->histories[idx][j + 1];
                    }
                    data->histories[idx][K - 1] = globalLRUCounter;
                }

                //printHistories(bm);
            }
            return RC_OK;
        }
        i--; 
    }

    // if page not in buffer, choose a victim frame
    int victim = -1;

    // prioritize finding empty frames
    for (int j = 0; j < bm->numPages; j++) {
        if (mgmt->frames[j].pageNum == NO_PAGE) {
            victim = j;
            break;
        }
    }

    if (victim == -1) {
        switch (bm->strategy) {
            /*
                RS_FIFO = 0,
                RS_LRU = 1,
                RS_CLOCK = 2,
                RS_LFU = 3,
                RS_LRU_K = 4  
            */
            case RS_FIFO: {
                int start = mgmt->nextVictim;
                int found = 0;

                // for loop all frame，find a victim whose fixCount == 0 
                for (int j = 0; j < bm->numPages; j++) {
                    int idx = (start + j) % bm->numPages;

                    if (mgmt->frames[idx].fixCount == 0) {
                        victim = idx;
                        mgmt->nextVictim = (idx + 1) % bm->numPages; // update next victim pointer
                        found = 1;
                        break;
                    }
                }

                if (!found) {
                    return RC_PINNED_PAGES_IN_BUFFER; // all frame is pinned
                }
                break;
            }


            case RS_LRU:{

                int victimIndex = -1;
                int minRef = __INT_MAX__;

                int i = 0;
                while (i < bm->numPages) {
                    if (mgmt->frames[i].fixCount == 0) { // if the page is pinned, we can't repalce it 
                        if (mgmt->frames[i].ref < minRef) { // find the least resntly used
                            minRef = mgmt->frames[i].ref;
                            victimIndex = i;
                        }
                    }
                    i++;
                }

                if (victimIndex == -1) {
                    return RC_PINNED_PAGES_IN_BUFFER;  // failed to find 
                }
                victim = victimIndex;
                break;
            }

            case RS_CLOCK: {
                while (true) {
                    /*
                    If the page is pinned, skip it.
                    If refBit == 1, clear it to zero which means the second chance.
                    If refBit == 0, select it as the victim.
                    */
                    Frame *candidate = &mgmt->frames[mgmt->clockHand]; // is the frame pointed to by the current pointer

                    if (candidate->fixCount == 0) {
                        if (candidate->refBit == 0) {
                            victim = mgmt->clockHand;
                            mgmt->clockHand = (mgmt->clockHand + 1) % bm->numPages;
                            break;
                        } else {
                            candidate->refBit = 0; // give second chance
                        }
                    }

                    mgmt->clockHand = (mgmt->clockHand + 1) % bm->numPages;
                }
                break;
            }

            case RS_LFU: {
                int victimIndex = -1; // this will be the index of the victim when finish
                int minRef = __INT_MAX__; // Initialize the frequency count of the least frequently used frame with INT_MAX
            
                // to select a victim that has least frequent accessing
                for (int i = 0; i < bm->numPages; i++) {
                    if (mgmt->frames[i].fixCount == 0) {  // victim must be unpinned frames
                        if (mgmt->frames[i].ref < minRef) {
                            minRef = mgmt->frames[i].ref;
                            victimIndex = i; // i is the index of the chosen victim frame
                        }
                    }
                }
            
                if (victimIndex == -1) {  // if victimIndex is -1, all the frames pinned. Victim actually not found.
                    return RC_PINNED_PAGES_IN_BUFFER;
                }
                
                victim = victimIndex;
                break;
            }

            case RS_LRU_K: {
                LRUKData *data = (LRUKData *)mgmt->strategyData;
                int K = data->K;

                int victimIndex = -1;
                long long oldestKth = LLONG_MAX;

                // to select a victim that has at least K access histories
                for (int i = 0; i < bm->numPages; i++) {
                    if (mgmt->frames[i].fixCount == 0) { // only consider unpinned frames
                        if (data->historyCount[i] >= K) {
                            // use the oldest one among last K accesses
                            long long kth = data->histories[i][K - 1];
                            if (kth < oldestKth) {
                                oldestKth = kth;
                                victimIndex = i;
                            }
                        }
                    }
                }

                // if no frame has >= K accesses, use the most recent access
                if (victimIndex == -1) {
                    for (int i = 0; i < bm->numPages; i++) {
                        if (mgmt->frames[i].fixCount == 0 && data->historyCount[i] > 0) {
                            long long last = data->histories[i][data->historyCount[i] - 1];
                            if (last < oldestKth) {
                                oldestKth = last;
                                victimIndex = i;
                            }
                        }
                    }
                }

                // if still no victim found, it means all frames are pinned
                if (victimIndex == -1) {
                    return RC_PINNED_PAGES_IN_BUFFER;
                }

                victim = victimIndex;
                break;
            }
            
            default:
                return RC_WRITE_FAILED;  // the strategy is not involved
        }
    }
    // dirty pages modified by users need to be written back to disk
    if (mgmt->frames[victim].dirty == true) {
        SM_FileHandle fh;
        openPageFile(bm->pageFile, &fh);
        writeBlock(mgmt->frames[victim].pageNum, &fh, mgmt->frames[victim].data);
        mgmt->numWriteIO++;
        closePageFile(&fh);
    }

    //  ensure file has enough pages before reading 
    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);
    if (pageNum >= fh.totalNumPages) {
        RC rc = ensureCapacity(pageNum + 1, &fh);
        if (rc != RC_OK) {
            closePageFile(&fh);
            return rc;
        }
    }
    
    // read new page into victim frame
    RC rc = readBlock(pageNum, &fh, mgmt->frames[victim].data);
    closePageFile(&fh);
    if (rc != RC_OK) return rc;

    mgmt->numReadIO++;
    mgmt->frames[victim].pageNum = pageNum;
    mgmt->frames[victim].dirty = false;
    mgmt->frames[victim].fixCount = 1;
    if (bm->strategy == RS_LRU) { // LRU strategy, count and see the time node of the call
        mgmt->frames[victim].ref = globalLRUCounter++;
    }
    else if (bm->strategy == RS_CLOCK) { // CLOCK strategy
        mgmt->frames[victim].refBit = 0;
    }  
    else if (bm->strategy == RS_LFU) { // least frequently used strategy
        mgmt->frames[victim].ref = 1; // New page starts with reference bit 1
    }  
    else if (bm->strategy == RS_LRU_K) {
        LRUKData *data = (LRUKData *)mgmt->strategyData;
        int K = data->K;
        int idx = victim;

        //  clear History
        for (int j = 0; j < K; j++) {
            data->histories[idx][j] = -1;
        }
        data->historyCount[idx] = 0;

        globalLRUCounter++;
        // record the first visit to a new page
        data->histories[idx][0] = globalLRUCounter;
        data->historyCount[idx] = 1;
    }

  
    // update PageHandle
    page->pageNum = pageNum;
    page->data = mgmt->frames[victim].data;

    return RC_OK;
}

/* Buffer Manager Interface - Statistics*/


PageNumber *getFrameContents (BM_BufferPool *const bm) {
    /*
      Iterate through all frames and store the pageNum in an array.
      If a frame is empty, the pageNum will be NO_PAGE.
    */
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;

    // an arry to store the page number of the frame
    PageNumber *contents = (PageNumber *) malloc(sizeof(PageNumber) * bm->numPages);

    // copy the pageNum of each frame
    for (int i = 0; i < bm->numPages; i++) {
        contents[i] = mgmt->frames[i].pageNum; 
    }

    return contents;
}


bool *getDirtyFlags (BM_BufferPool *const bm) {
    /*
      For loop the frame and put the dirty flag (true/false) into the array.
    */
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;

    bool *flags = (bool *) malloc(sizeof(bool) * bm->numPages);

    for (int i = 0; i < bm->numPages; i++) {
        flags[i] = mgmt->frames[i].dirty;  // true/false
    }

    return flags;
}


int *getFixCounts (BM_BufferPool *const bm) {
    /*
      get the fix counts for each frame
    */ 
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;

    int *fixCounts = (int *) malloc(sizeof(int) * bm->numPages);

    for (int i = 0; i < bm->numPages; i++) {
        fixCounts[i] = mgmt->frames[i].fixCount; 
    }

    return fixCounts;
}

// Return the number of pages read from disk
int getNumReadIO (BM_BufferPool *const bm) {
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;
    return mgmt->numReadIO;
}

// Return the number of pages written to disk
int getNumWriteIO (BM_BufferPool *const bm) {
    PoolMgmtData *mgmt = (PoolMgmtData *) bm->mgmtData;
    return mgmt->numWriteIO;
}
