#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "tables.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>



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

BTreeLeafNode *createLeafNode(int pageNum, int order) {
    BTreeLeafNode *leaf = (BTreeLeafNode *)malloc(sizeof(BTreeLeafNode));
    leaf->base.type = NODE_LEAF;
    leaf->base.numKeys = 0;
    leaf->base.pageNum = pageNum;
    leaf->keys = (Value *)calloc(order, sizeof(Value));
    leaf->rids = (RID *)calloc(order, sizeof(RID));
    leaf->nextLeaf = -1; // -1 means no sub node
    return leaf;
}

BTreeInternalNode *createInternalNode(int pageNum, int order) {
    BTreeInternalNode *node = (BTreeInternalNode *)malloc(sizeof(BTreeInternalNode));
    node->base.type = NODE_INTERNAL;
    node->base.numKeys = 0;
    node->base.pageNum = pageNum;
    node->keys = (Value *)calloc(order, sizeof(Value));
    node->children = (int *)calloc(order + 1, sizeof(int));
    return node;
}

/* 
   Index Manager Initialization / Shutdown
*/
RC initIndexManager (void *mgmtData) {
    // TODO: initialize any global structures if needed
    return RC_OK;
}

RC shutdownIndexManager () {
    // TODO: free any global resources if needed
    return RC_OK;
}

/* 
   Create / Open / Close / Delete B+ Tree
*/
RC createBtree (char *idxId, DataType keyType, int n) {
    // TODO: create page file, initialize root node, store metadata
    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId) {
    // TODO: open existing B+ tree file and load metadata
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree) {
    // TODO: flush dirty pages and close buffer pool
    return RC_OK;
}

RC deleteBtree (char *idxId) {
    // TODO: destroy the page file corresponding to idxId
    return RC_OK;
}

/* 
   3. Access Information About a B+ Tree
*/
RC getNumNodes (BTreeHandle *tree, int *result) {
    // TODO: return number of nodes in the tree
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result) {
    // TODO: return number of key entries in the tree
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result) {
    // TODO: return key data type
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
