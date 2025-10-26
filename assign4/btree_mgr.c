#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "tables.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>



// define the type of node
typedef enum NodeType {
    NODE_LEAF,
    NODE_INTERNAL
} NodeType;

// information shared by both nodes
typedef struct BTreeNodeBase {
    NodeType type;       
    int numKeys;         // the number of keys currently stored
    int pageNum;         // the page number on disk
} BTreeNodeBase;

// leaf node structure
typedef struct BTreeLeafNode {
    BTreeNodeBase base;  // shared part
    Value *keys;         // key array
    RID *rids;           // the record pointer corresponding to each key
    int nextLeaf;        // the page number of the next leaf node 
} BTreeLeafNode;

// inner node structure
typedef struct BTreeInternalNode {
    BTreeNodeBase base;  // shared part
    Value *keys;         // key array
    int *children;       // child node page number array
} BTreeInternalNode;

typedef struct BTreeMgmtData {
    BM_BufferPool *bm;   // buffer manager
    BM_PageHandle *page; // current page handle
    int rootPage;        // root node page number
    int numNodes;        // total num of node
    int numEntries;      // total num of key
    int order;           // order
} BTreeMgmtData;
typedef struct ScanMgmtData {
    int currentPage;  
    int keyIndex;     //  key index in the current leaf page
    bool end;          // whether the scan has ended
} ScanMgmtData;

BTreeLeafNode *createLeafNode(int pageNum, int order) {
    BTreeLeafNode *leaf = (BTreeLeafNode *)malloc(sizeof(BTreeLeafNode));
    leaf->base.type = NODE_LEAF;
    leaf->base.numKeys = 0;
    leaf->base.pageNum = pageNum;
    leaf->keys = calloc(order + 1, sizeof(Value));  //  +1 preventing out-of-bounds splitting
    leaf->rids = calloc(order + 1, sizeof(RID));    // 
    leaf->nextLeaf = -1; // -1 means no sub node
    return leaf;
}

BTreeInternalNode *createInternalNode(int pageNum, int order) {
    BTreeInternalNode *node = (BTreeInternalNode *)malloc(sizeof(BTreeInternalNode));
    node->base.type = NODE_INTERNAL;
    node->base.numKeys = 0;
    node->base.pageNum = pageNum;
    node->keys = calloc(order + 1, sizeof(Value));     //  +1
    node->children = calloc(order + 2, sizeof(int));   //  +2 (the number of children is 1 more than the key)
    return node;
}

/* 
   Index Manager Initialization / Shutdown
*/
RC initIndexManager (void *mgmtData) {
    // initialize the storage manager
    (void)mgmtData;
    initStorageManager();

    return RC_OK;
}

RC shutdownIndexManager () {

    return RC_OK;
}

/* 

Page Number Content
0 Metadata page
1 Root node page 
2 Subsequent node pages

   Create / Open / Close / Delete B+ Tree

*/
RC createBtree (char *idxId, DataType keyType, int n) {
    // TODO: create page file, initialize root node, store metadata
    SM_FileHandle fh;
    BM_BufferPool *bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
    BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
    RC rc;

    // create new page file
    rc = createPageFile(idxId);
    if (rc != RC_OK) return rc;

    rc = openPageFile(idxId, &fh);
    if (rc != RC_OK) return rc;

    // initialize buffer pool
    rc = initBufferPool(bm, idxId, 10, RS_LRU, NULL);
    if (rc != RC_OK) return rc;

    // initialize meta data page
    rc = pinPage(bm, page, 0);
    if (rc != RC_OK) return rc;

    // write info
    char metadata[PAGE_SIZE];
    memset(metadata, 0, PAGE_SIZE);
    sprintf(metadata, "%d %d %d %d %d", 
            keyType,   
            n,         // order
            1,         // rootPage 
            1,         // numNodes 
            0);        // numEntries 
    memcpy(page->data, metadata, strlen(metadata) + 1);

    markDirty(bm, page);
    unpinPage(bm, page);
    forceFlushPool(bm);

    // empty root leaf page）
    rc = pinPage(bm, page, 1);
    if (rc != RC_OK) return rc;

    //BTreeLeafNode *root = createLeafNode(1, n);
    //memcpy(page->data, root, sizeof(BTreeLeafNode));

    memset(page->data, 0, PAGE_SIZE);  // clear 
    printf("createBtree: bm=%p page=%p rootPage=%d\n", bm, page, 1);

    markDirty(bm, page);
    unpinPage(bm, page);
    forceFlushPool(bm);

    // close
    shutdownBufferPool(bm);
    closePageFile(&fh);

    free(bm);
    free(page);
    //free(root);

    printf("B+ tree '%s' created successfully.\n", idxId);
    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId) {
    BM_BufferPool *bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
    BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
    RC rc;

    // initialize buffer pool
    rc = initBufferPool(bm, idxId, 10, RS_LRU, NULL);
    if (rc != RC_OK) return rc;

    // read metadata page
    rc = pinPage(bm, page, 0);
    if (rc != RC_OK) return rc;

    int keyType, order, rootPage, numNodes, numEntries;
    sscanf(page->data, "%d %d %d %d %d", &keyType, &order, &rootPage, &numNodes, &numEntries);
    
    printf("openBtree: bm=%p page=%p rootPage(from file)=%d\n", bm, page, rootPage);

    // constructing BTreeHandle and Management Structure
    BTreeHandle *newTree = (BTreeHandle *) malloc(sizeof(BTreeHandle));
    BTreeMgmtData *mgmtData = (BTreeMgmtData *) malloc(sizeof(BTreeMgmtData));

    mgmtData->bm = bm;
    mgmtData->page = page;
    mgmtData->rootPage = rootPage;
    mgmtData->numNodes = numNodes;
    mgmtData->numEntries = numEntries;
    mgmtData->order = order;

    newTree->keyType = keyType;
    newTree->idxId = idxId;
    newTree->mgmtData = mgmtData;
    printf("metadata content: '%s'\n", (char*)page->data);

    *tree = newTree;

    unpinPage(bm, page);
    printf("B+ tree '%s' opened successfully.\n", idxId);
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree) {
    BTreeMgmtData *mgmt = (BTreeMgmtData *) tree->mgmtData;

    forceFlushPool(mgmt->bm);
    shutdownBufferPool(mgmt->bm);

    free(mgmt->page);
    free(mgmt->bm);
    free(mgmt);
    free(tree);

    printf("B+ tree closed successfully.\n");
    return RC_OK;
}

RC deleteBtree (char *idxId) {
    RC rc = destroyPageFile(idxId);
    if (rc != RC_OK)
        return rc;

    printf("B+ tree '%s' deleted successfully.\n", idxId);
    return RC_OK;
}

/* 
   3. Access Information About a B+ Tree
*/
RC getNumNodes (BTreeHandle *tree, int *result) {
    //  return number of nodes in the tree
    BTreeMgmtData *mgmt = (BTreeMgmtData *) tree->mgmtData;
    *result = mgmt->numNodes;
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result) {
    //  return number of key entries in the tree
    BTreeMgmtData *mgmt = (BTreeMgmtData *) tree->mgmtData;
    *result = mgmt->numEntries;
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result) {
    //  return key data type
    *result = tree->keyType;
    return RC_OK;
}

/* 
   4. Index Operations
*/
RC findKey (BTreeHandle *tree, Value *key, RID *result) {
    // TODO: search for a key in the B+ tree
    return RC_IM_KEY_NOT_FOUND;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid) {
    // TODO: insert key/RID into the B+ tree
    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key) {
    // TODO: delete key/RID from the B+ tree
    return RC_OK;
}

/* 
   5. Scanning (iterate over sorted entries)
*/
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {
    // TODO: initialize scan structure
    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result) {
    // TODO: return next RID in sorted order
    return RC_IM_NO_MORE_ENTRIES;
}

RC closeTreeScan (BT_ScanHandle *handle) {
    // TODO: close and clean up scan structure
    return RC_OK;
}