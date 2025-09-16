#include "storage_mgr.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "test_helper.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// var to store the current test's name
char *testName;

// check whether buffer pool content is the same as expected
#define ASSERT_EQUALS_POOL(expected,bm,message)                    \
do {                                    \
char *real;                                \
char *_exp = (char *) (expected);                                   \
real = sprintPoolContent(bm);                    \
if (strcmp((_exp),real) != 0)                    \
{                                    \
printf("[%s-%s-L%i-%s] FAILED: expected <%s> but was <%s>: %s\n",TEST_INFO, _exp, real, message); \
free(real);                            \
exit(1);                            \
}                                    \
printf("[%s-%s-L%i-%s] OK: expected <%s> and was <%s>: %s\n",TEST_INFO, _exp, real, message); \
free(real);                                \
} while(0)

// helper
static void createDummyPages(BM_BufferPool *bm, int num);

// test methods
static void testCLOCK (void);
static void testLFU (void);

int
main (void)
{
    initStorageManager();
    testName = "";

    testCLOCK();
    testLFU();

    return 0;
}

void
createDummyPages(BM_BufferPool *bm, int num)
{
    int i;
    BM_PageHandle *h = MAKE_PAGE_HANDLE();

    CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

    for (i = 0; i < num; i++)
    {
        CHECK(pinPage(bm, h, i));
        sprintf(h->data, "%s-%i", "Page", h->pageNum);
        CHECK(markDirty(bm, h));
        CHECK(unpinPage(bm,h));
    }

    CHECK(shutdownBufferPool(bm));
    free(h);
}

// ====================================================
// Test the CLOCK page replacement strategy
// ====================================================
static void
testCLOCK (void)
{
    const char *poolContents[] = {
        "[0 0],[-1 0],[-1 0]",
        "[0 0],[1 0],[-1 0]",
        "[0 0],[1 0],[2 0]",
        "[3 0],[1 0],[2 0]",
        "[3 0],[4 0],[2 0]",
        "[3 0],[4 0],[5 0]"
    };

    int snapshot = 0;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    testName = "Testing CLOCK page replacement";

    CHECK(createPageFile("testbuffer.bin"));
    createDummyPages(bm, 100);
    CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_CLOCK, NULL));

    for (int i = 0; i < 3; i++) {
        pinPage(bm, h, i);
        unpinPage(bm, h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "CLOCK: initial load");
    }

    for (int i = 3; i < 6; i++) {
        pinPage(bm, h, i);
        unpinPage(bm, h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "CLOCK: replacement");
    }

    ASSERT_EQUALS_INT(6, getNumReadIO(bm), "CLOCK: number of read I/Os");
    ASSERT_EQUALS_INT(0, getNumWriteIO(bm), "CLOCK: number of write I/Os");

    CHECK(shutdownBufferPool(bm));
    CHECK(destroyPageFile("testbuffer.bin"));
    free(bm);
    free(h);
    TEST_DONE();
}

// ====================================================
// Test the LFU page replacement strategy
// ====================================================
static void
testLFU (void)
{
    const char *poolContents[] = {
        "[0 0],[-1 0],[-1 0]",
        "[0 0],[1 0],[-1 0]",
        "[0 0],[1 0],[2 0]",
        "[0 0],[1 0],[2 0]",
        "[0 0],[1 0],[2 0]",
        "[0 0],[1 0],[3 0]",
        "[0 0],[1 0],[4 0]"
    };

    int snapshot = 0;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    testName = "Testing LFU page replacement";

    CHECK(createPageFile("testbuffer.bin"));
    createDummyPages(bm, 100);
    CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_LFU, NULL));

    for (int i = 0; i < 3; i++) {
        pinPage(bm, h, i);
        unpinPage(bm, h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "LFU: initial load");
    }

    // increase frequency of page 0 and 1
    for (int i = 0; i < 2; i++) {
        pinPage(bm, h, 0);
        unpinPage(bm, h);
        pinPage(bm, h, 1);
        unpinPage(bm, h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "LFU: increase frequency");
    }

    for (int i = 3; i < 5; i++) {
        pinPage(bm, h, i);
        unpinPage(bm, h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "LFU: replacement");
    }

    ASSERT_EQUALS_INT(5, getNumReadIO(bm), "LFU: number of read I/Os");
    ASSERT_EQUALS_INT(0, getNumWriteIO(bm), "LFU: number of write I/Os");

    CHECK(shutdownBufferPool(bm));
    CHECK(destroyPageFile("testbuffer.bin"));
    free(bm);
    free(h);
    TEST_DONE();
}
