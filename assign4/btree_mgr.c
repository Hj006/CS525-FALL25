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
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle page;
    int currentPage = mgmt->rootPage;

    while (true) {
        pinPage(bm, &page, currentPage);

        int offset = 0;
        NodeType type;
        memcpy(&type, page.data + offset, sizeof(NodeType));
        offset += sizeof(NodeType);

        if (type == NODE_LEAF) {
            int numKeys, nextLeaf;
            memcpy(&numKeys, page.data + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(&nextLeaf, page.data + offset, sizeof(int));
            offset += sizeof(int);

            for (int i = 0; i < numKeys; i++) {
                int keyVal;
                RID rid;
                memcpy(&keyVal, page.data + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&rid.page, page.data + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&rid.slot, page.data + offset, sizeof(int));
                offset += sizeof(int);

                if (keyVal == key->v.intV) {
                    *result = rid;
                    unpinPage(bm, &page);
                    return RC_OK;
                }
            }

            unpinPage(bm, &page);
            return RC_IM_KEY_NOT_FOUND;
        } else {
            int numKeys;
            memcpy(&numKeys, page.data + offset, sizeof(int));
            offset += sizeof(int);

            int *keys = calloc(numKeys, sizeof(int));
            for (int i = 0; i < numKeys; i++) {
                memcpy(&keys[i], page.data + offset, sizeof(int));
                offset += sizeof(int);
            }

            int *children = calloc(numKeys + 1, sizeof(int));
            for (int i = 0; i <= numKeys; i++) {
                memcpy(&children[i], page.data + offset, sizeof(int));
                offset += sizeof(int);
            }

            int i;
            for (i = 0; i < numKeys; i++) {
                if (key->v.intV < keys[i])
                    break;
            }
            currentPage = children[i];

            free(keys);
            free(children);
            unpinPage(bm, &page);
        }
    }
}

static RC splitLeaf(BTreeHandle *tree, BTreeLeafNode *leaf, Value *key, RID rid,
                    Value *promoteKey, int *newLeafPageNum)
{
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    int order = mgmt->order;

    printf("[splitLeaf] page=%d, numKeys(before)=%d\n", leaf->base.pageNum, leaf->base.numKeys);

    //  collect all key/rid
    int totalKeys = leaf->base.numKeys + 1;
    Value *tempKeys = calloc(totalKeys, sizeof(Value));
    RID *tempRids = calloc(totalKeys, sizeof(RID));

    //  initialize all Value types to prevent undefined behavior when comparing
    for (int i = 0; i < totalKeys; i++) {
        tempKeys[i].dt = DT_INT;
    }

    for (int i = 0; i < leaf->base.numKeys; i++) {
        tempKeys[i].v.intV = leaf->keys[i].v.intV;
        tempRids[i] = leaf->rids[i];
    }

    //  insert new key (keep ascending order) 
    int inserted = 0;
    int j = 0;
    for (int i = 0; i < leaf->base.numKeys; i++) {
        if (!inserted && key->v.intV < leaf->keys[i].v.intV) {
            tempKeys[j].dt = DT_INT;
            tempKeys[j].v.intV = key->v.intV;
            tempRids[j] = rid;
            inserted = 1;
            j++;
        }
        tempKeys[j].dt = DT_INT;
        tempKeys[j].v.intV = leaf->keys[i].v.intV;
        tempRids[j] = leaf->rids[i];
        j++;
    }
    // if the new key is the largest, append it to the end
    if (!inserted) {
        tempKeys[j].dt = DT_INT;
        tempKeys[j].v.intV = key->v.intV;
        tempRids[j] = rid;
    }




    //  split
    int split = (totalKeys + 1) / 2;

    //  create new leaf 
    int newPage = ++mgmt->numNodes;
    *newLeafPageNum = newPage;
    BTreeLeafNode *newLeaf = createLeafNode(newPage, order);

    // assign left and right keys and kids
    leaf->base.numKeys = split;
    newLeaf->base.numKeys = totalKeys - split;
    for (int i = 0; i < split; i++) {
        leaf->keys[i] = tempKeys[i];
        leaf->rids[i] = tempRids[i];
    }
    for (int i = 0; i < newLeaf->base.numKeys; i++) {
        newLeaf->keys[i] = tempKeys[split + i];
        newLeaf->rids[i] = tempRids[split + i];
    }

    int oldNext = leaf->nextLeaf;

    // determine the minimum key of the next leaf (if it exists)
    int nextMinKey = INT_MAX;
    if (oldNext != -1) {
        BM_PageHandle ph_next;
        pinPage(bm, &ph_next, oldNext);
        NodeType type;
        memcpy(&type, ph_next.data, sizeof(NodeType));
        if (type == NODE_LEAF) {
            int numKeys_next;
            memcpy(&numKeys_next, ph_next.data + sizeof(NodeType), sizeof(int));
            if (numKeys_next > 0) {
                int offset_next = sizeof(NodeType) + sizeof(int) + sizeof(int);
                memcpy(&nextMinKey, ph_next.data + offset_next, sizeof(int));
            }
        }
        unpinPage(bm, &ph_next);
    }

    // determine whether to insert before or after oldNext
    if (newLeaf->keys[0].v.intV < nextMinKey) {
        // insert between leaf and oldNext
        newLeaf->nextLeaf = oldNext;
        leaf->nextLeaf = newPage;
    } else {
        // insert after oldNext
        newLeaf->nextLeaf = -1;
        leaf->nextLeaf = oldNext;
    }

    // promote key
    promoteKey->dt = DT_INT;
    promoteKey->v.intV = newLeaf->keys[0].v.intV;

    printf("[splitLeaf] left=%d keys=", leaf->base.pageNum);
    for (int i = 0; i < leaf->base.numKeys; i++) printf("%d ", leaf->keys[i].v.intV);
    printf(" | next=%d\n", leaf->nextLeaf);
    printf("[splitLeaf] right=%d keys=", newLeaf->base.pageNum);
    for (int i = 0; i < newLeaf->base.numKeys; i++) printf("%d ", newLeaf->keys[i].v.intV);
    printf(" | next=%d\n", newLeaf->nextLeaf);

    //  serializly write back to disk (order: NodeType → numKeys → nextLeaf)
    BM_PageHandle ph;
    NodeType type = NODE_LEAF;
    int offset;

    // wirte leaf
    pinPage(bm, &ph, leaf->base.pageNum);
    memset(ph.data, 0, PAGE_SIZE);
    offset = 0;
    memcpy(ph.data + offset, &type, sizeof(NodeType)); offset += sizeof(NodeType);
    memcpy(ph.data + offset, &leaf->base.numKeys, sizeof(int)); offset += sizeof(int);
    memcpy(ph.data + offset, &leaf->nextLeaf, sizeof(int)); offset += sizeof(int);
    for (int i = 0; i < leaf->base.numKeys; i++) {
        int keyVal = leaf->keys[i].v.intV;
        memcpy(ph.data + offset, &keyVal, sizeof(int)); offset += sizeof(int);
        memcpy(ph.data + offset, &leaf->rids[i].page, sizeof(int)); offset += sizeof(int);
        memcpy(ph.data + offset, &leaf->rids[i].slot, sizeof(int)); offset += sizeof(int);
    }
    markDirty(bm, &ph);
    unpinPage(bm, &ph);

    // write right
    pinPage(bm, &ph, newLeaf->base.pageNum);
    memset(ph.data, 0, PAGE_SIZE);
    offset = 0;
    memcpy(ph.data + offset, &type, sizeof(NodeType)); offset += sizeof(NodeType);
    memcpy(ph.data + offset, &newLeaf->base.numKeys, sizeof(int)); offset += sizeof(int);
    memcpy(ph.data + offset, &newLeaf->nextLeaf, sizeof(int)); offset += sizeof(int);
    for (int i = 0; i < newLeaf->base.numKeys; i++) {
        int keyVal = newLeaf->keys[i].v.intV;
        memcpy(ph.data + offset, &keyVal, sizeof(int)); offset += sizeof(int);
        memcpy(ph.data + offset, &newLeaf->rids[i].page, sizeof(int)); offset += sizeof(int);
        memcpy(ph.data + offset, &newLeaf->rids[i].slot, sizeof(int)); offset += sizeof(int);
    }
    markDirty(bm, &ph);
    unpinPage(bm, &ph);

//  fix isolated tail node: if newLeaf is the last
if (newLeaf->nextLeaf == -1) {
    BM_PageHandle ph_scan;
    int curPage = mgmt->rootPage;

    //  find the leftmost leaf node
    while (true) {
        pinPage(bm, &ph_scan, curPage);
        NodeType t;
        memcpy(&t, ph_scan.data, sizeof(NodeType));

        if (t == NODE_LEAF) {
            unpinPage(bm, &ph_scan);
            break;
        }

        int numKeys;
        memcpy(&numKeys, ph_scan.data + sizeof(NodeType), sizeof(int));

        // get the first child node
        int firstChild;
        memcpy(&firstChild, ph_scan.data + sizeof(NodeType) + sizeof(int) + numKeys * sizeof(int), sizeof(int));
        unpinPage(bm, &ph_scan);
        curPage = firstChild;
    }

    //  start from the leftmost leaf and follow nextLeaf to find the last leaf
    int lastLeaf = curPage;
    while (true) {
        pinPage(bm, &ph_scan, lastLeaf);
        NodeType t;
        memcpy(&t, ph_scan.data, sizeof(NodeType));

        if (t != NODE_LEAF) {
            unpinPage(bm, &ph_scan);
            break;
        }

        int nextLeaf;
        memcpy(&nextLeaf, ph_scan.data + sizeof(NodeType) + sizeof(int), sizeof(int));

        if (nextLeaf == -1) {
            //  found the last leaf
            if (lastLeaf != newPage) {
                memcpy(ph_scan.data + sizeof(NodeType) + sizeof(int), &newPage, sizeof(int));
                markDirty(bm, &ph_scan);
                printf("[splitLeaf-fix] Updated last leaf %d → next=%d\n", lastLeaf, newPage);
            }
            unpinPage(bm, &ph_scan);
            break;
        }

        unpinPage(bm, &ph_scan);
        lastLeaf = nextLeaf;
    }
}
    printf("[splitLeaf-debug] leaf %d nextLeaf=%d\n", leaf->base.pageNum, leaf->nextLeaf);
    printf("[splitLeaf-debug] newLeaf %d nextLeaf=%d\n", newLeaf->base.pageNum, newLeaf->nextLeaf);

    free(tempKeys);
    free(tempRids);
    free(newLeaf);
    return RC_OK;
}




static RC splitInternalNode(BTreeHandle *tree, BTreeInternalNode *node, 
                            Value promoteKeyFromChild, int rightChildPage,
                            Value *promoteKey, int *newRightPage) {
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    int order = mgmt->order;

    Value *tempKeys = calloc(order + 1, sizeof(Value));
    int *tempChildren = calloc(order + 2, sizeof(int));

    for (int i = 0; i < node->base.numKeys; i++)
        tempKeys[i] = node->keys[i];
    for (int i = 0; i <= node->base.numKeys; i++)
        tempChildren[i] = node->children[i];

    // insert new key/child
    int i = node->base.numKeys - 1;
    while (i >= 0 && tempKeys[i].v.intV > promoteKeyFromChild.v.intV) {
        tempKeys[i + 1] = tempKeys[i];
        tempChildren[i + 2] = tempChildren[i + 1];
        i--;
    }
    tempKeys[i + 1] = promoteKeyFromChild;
    tempChildren[i + 2] = rightChildPage;

    int split = (order + 1) / 2;

    // create a new internal node
    int newPage = ++mgmt->numNodes;
    *newRightPage = newPage;

    BTreeInternalNode *newInternal = createInternalNode(newPage, order);
    node->base.numKeys = split;

    for (int j = 0; j < split; j++) {
        node->keys[j] = tempKeys[j];
        node->children[j] = tempChildren[j];
    }
    node->children[split] = tempChildren[split];

    *promoteKey = tempKeys[split];

    int j = 0;
    for (int k = split + 1; k <= order; k++) {
        newInternal->keys[j] = tempKeys[k];
        newInternal->children[j] = tempChildren[k];
        j++;
    }
    newInternal->children[j] = tempChildren[order + 1];
    newInternal->base.numKeys = j;

    // write back to disk
    BM_PageHandle page;
    pinPage(bm, &page, node->base.pageNum);
    memset(page.data, 0, PAGE_SIZE);
    memcpy(page.data, &node->base.type, sizeof(NodeType));
    memcpy(page.data + sizeof(NodeType), &node->base.numKeys, sizeof(int));
    int offset = sizeof(NodeType) + sizeof(int);
    for (int j = 0; j < node->base.numKeys; j++) {
        memcpy(page.data + offset, &node->keys[j].v.intV, sizeof(int));
        offset += sizeof(int);
    }
    for (int j = 0; j <= node->base.numKeys; j++) {
        memcpy(page.data + offset, &node->children[j], sizeof(int));
        offset += sizeof(int);
    }
    markDirty(bm, &page);
    unpinPage(bm, &page);

    pinPage(bm, &page, newInternal->base.pageNum);
    memset(page.data, 0, PAGE_SIZE);
    memcpy(page.data, &newInternal->base.type, sizeof(NodeType));
    memcpy(page.data + sizeof(NodeType), &newInternal->base.numKeys, sizeof(int));
    offset = sizeof(NodeType) + sizeof(int);
    for (int j = 0; j < newInternal->base.numKeys; j++) {
        memcpy(page.data + offset, &newInternal->keys[j].v.intV, sizeof(int));
        offset += sizeof(int);
    }
    for (int j = 0; j <= newInternal->base.numKeys; j++) {
        memcpy(page.data + offset, &newInternal->children[j], sizeof(int));
        offset += sizeof(int);
    }
    markDirty(bm, &page);
    unpinPage(bm, &page);


    free(tempKeys);
    free(tempChildren);
    free(newInternal);
    return RC_OK;
}

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle *page = malloc(sizeof(BM_PageHandle));
    printf("insertKey: bm=%p page=%p rootPage=%d numEntries=%d\n",
        mgmt->bm, mgmt->page, mgmt->rootPage, mgmt->numEntries);


    //  when the root is empty, create the first leaf
    if (mgmt->numEntries == 0) {
        printf("DEBUG: rootPage=%d (creating first leaf)\n", mgmt->rootPage);

        pinPage(bm, page, mgmt->rootPage);

        // write empty leaves serially
        NodeType type = NODE_LEAF;
        int numKeys = 1;
        int nextLeaf = -1;
        memset(page->data, 0, PAGE_SIZE);

        int offset = 0;
        memcpy(page->data + offset, &type, sizeof(NodeType)); offset += sizeof(NodeType);
        memcpy(page->data + offset, &numKeys, sizeof(int)); offset += sizeof(int);
        memcpy(page->data + offset, &nextLeaf, sizeof(int)); offset += sizeof(int);
        memcpy(page->data + offset, &key->v.intV, sizeof(int)); offset += sizeof(int);
        memcpy(page->data + offset, &rid, sizeof(RID));

        markDirty(bm, page);
        unpinPage(bm, page);
        forceFlushPool(bm);

        mgmt->numEntries = 1;
        mgmt->numNodes = 1;

        printf("[insertKey] Created first leaf key=%d\n", key->v.intV);

        free(page);
        return RC_OK;
    }

    //free(page);

    pinPage(bm, page, mgmt->rootPage);
    NodeType rootType;
    memcpy(&rootType, page->data, sizeof(NodeType));
    BTreeLeafNode leaf;

    if (rootType == NODE_LEAF) {
        printf("Root is leaf\n");

        //  deserialize leaf node header
        int numKeys, nextLeaf;
        int offset = sizeof(NodeType);
        memcpy(&numKeys, page->data + offset, sizeof(int)); offset += sizeof(int);
        memcpy(&nextLeaf, page->data + offset, sizeof(int)); offset += sizeof(int);

        printf("numKeys=%d, nextLeaf=%d\n", numKeys, nextLeaf);

        //  Mmnually construct the leaf structure
        BTreeLeafNode leaf;
        leaf.base.type = NODE_LEAF;
        leaf.base.pageNum = mgmt->rootPage;
        leaf.base.numKeys = numKeys;
        leaf.nextLeaf = nextLeaf;
        int order = mgmt->order;

        leaf.keys = calloc(order, sizeof(Value));
        leaf.rids = calloc(order, sizeof(RID));

        // deserialize all key/rid
        for (int i = 0; i < numKeys; i++) {
            memcpy(&leaf.keys[i].v.intV, page->data + offset, sizeof(int)); offset += sizeof(int);
            leaf.keys[i].dt = DT_INT;
            memcpy(&leaf.rids[i], page->data + offset, sizeof(RID)); offset += sizeof(RID);
        }

        // （B）determine whether it is full
        if (leaf.base.numKeys < order) {
            //  insert by asending order
            int inserted = 0;
            Value *tempKeys = calloc(order + 1, sizeof(Value));
            RID *tempRids = calloc(order + 1, sizeof(RID));

            for (int i = 0, j = 0; i < leaf.base.numKeys; i++, j++) {
                if (!inserted && key->v.intV < leaf.keys[i].v.intV) {
                    tempKeys[j].dt = DT_INT;
                    tempKeys[j].v.intV = key->v.intV;
                    tempRids[j] = rid;
                    inserted = 1;
                    j++;
                }
                tempKeys[j] = leaf.keys[i];
                tempRids[j] = leaf.rids[i];
            }
            if (!inserted) {
                tempKeys[leaf.base.numKeys].dt = DT_INT;
                tempKeys[leaf.base.numKeys].v.intV = key->v.intV;
                tempRids[leaf.base.numKeys] = rid;
            }

            leaf.base.numKeys++;

            //  write back to disk
            memset(page->data, 0, PAGE_SIZE);
            int offset = 0;
            memcpy(page->data + offset, &leaf.base.type, sizeof(NodeType)); offset += sizeof(NodeType);
            memcpy(page->data + offset, &leaf.base.numKeys, sizeof(int)); offset += sizeof(int);
            memcpy(page->data + offset, &leaf.nextLeaf, sizeof(int)); offset += sizeof(int);
            for (int i = 0; i < leaf.base.numKeys; i++) {
                memcpy(page->data + offset, &tempKeys[i].v.intV, sizeof(int)); offset += sizeof(int);
                memcpy(page->data + offset, &tempRids[i], sizeof(RID)); offset += sizeof(RID);
            }

            markDirty(bm, page);
            unpinPage(bm, page);
            forceFlushPool(bm);
            mgmt->numEntries++;

            free(tempKeys);
            free(tempRids);
            free(page);

            printf("[insertKey] Inserted into leaf key=%d (sorted)\n", key->v.intV);
            return RC_OK;
        } else {
            // leaf node is full -> split
            Value promoteKey;
            int newLeafPage;
            leaf.base.pageNum = mgmt->rootPage; 
            splitLeaf(tree, &leaf, key, rid, &promoteKey, &newLeafPage);

            // create new root
            int rootPage = ++mgmt->numNodes;
            BTreeInternalNode *root = createInternalNode(rootPage, mgmt->order);
            root->base.numKeys = 1;
            root->keys[0] = promoteKey;
            root->children[0] = mgmt->rootPage;
            root->children[1] = newLeafPage;

            //  write serially to disk
            BM_PageHandle rootPg;
            pinPage(bm, &rootPg, rootPage);

            int offset = 0;
            memset(rootPg.data, 0, PAGE_SIZE);
            memcpy(rootPg.data + offset, &root->base.type, sizeof(NodeType)); offset += sizeof(NodeType);
            memcpy(rootPg.data + offset, &root->base.numKeys, sizeof(int)); offset += sizeof(int);

            for (int i = 0; i < root->base.numKeys; i++) {
                memcpy(rootPg.data + offset, &root->keys[i].v.intV, sizeof(int));
                offset += sizeof(int);
            }
            for (int i = 0; i <= root->base.numKeys; i++) {
                memcpy(rootPg.data + offset, &root->children[i], sizeof(int));
                offset += sizeof(int);
            }

            markDirty(bm, &rootPg);
            unpinPage(bm, &rootPg);

            mgmt->rootPage = rootPage;
            mgmt->numEntries++;

            printf("[insertKey] Created new root page=%d (promoteKey=%d)\n",
                rootPage, promoteKey.v.intV);
            return RC_OK;
        }
    } else {

        // rootis an internal node
     

        //  deserialize internal nodes
        BTreeInternalNode node;
        int offset = 0;
        NodeType type;
        memcpy(&type, page->data + offset, sizeof(NodeType)); offset += sizeof(NodeType);
        memcpy(&node.base.numKeys, page->data + offset, sizeof(int)); offset += sizeof(int);

        node.base.type = type;
        node.base.pageNum = mgmt->rootPage;
        node.keys = calloc(mgmt->order + 1, sizeof(Value));
        node.children = calloc(mgmt->order + 2, sizeof(int));

        for (int i = 0; i < node.base.numKeys; i++) {
            memcpy(&node.keys[i].v.intV, page->data + offset, sizeof(int));
            node.keys[i].dt = DT_INT;
            offset += sizeof(int);
        }
        for (int i = 0; i <= node.base.numKeys; i++) {
            memcpy(&node.children[i], page->data + offset, sizeof(int));
            offset += sizeof(int);
        }

        //  find the child node to insert
        int i;
        for (i = 0; i < node.base.numKeys; i++) {
            if (key->v.intV < node.keys[i].v.intV)
                break;
        }
        int targetPage = node.children[i];

        //  open the target leaf page
        BM_PageHandle leafPg;
        pinPage(bm, &leafPg, targetPage);

        //  deserialize leaf nodes
        BTreeLeafNode leaf;
        offset = 0;
        memcpy(&leaf.base.type, leafPg.data + offset, sizeof(NodeType)); offset += sizeof(NodeType);
        memcpy(&leaf.base.numKeys, leafPg.data + offset, sizeof(int)); offset += sizeof(int);
        memcpy(&leaf.nextLeaf, leafPg.data + offset, sizeof(int)); offset += sizeof(int);

        leaf.base.pageNum = targetPage;
        leaf.keys = calloc(mgmt->order, sizeof(Value));
        leaf.rids = calloc(mgmt->order, sizeof(RID));

        for (int j = 0; j < leaf.base.numKeys; j++) {
            memcpy(&leaf.keys[j].v.intV, leafPg.data + offset, sizeof(int));
            leaf.keys[j].dt = DT_INT;
            offset += sizeof(int);
            memcpy(&leaf.rids[j], leafPg.data + offset, sizeof(RID));
            offset += sizeof(RID);
        }

        //  determine whether it is full
        if (leaf.base.numKeys < mgmt->order) {
            //  insert  leaf directly 
            int j = leaf.base.numKeys - 1;
            while (j >= 0 && leaf.keys[j].v.intV > key->v.intV) {
                leaf.keys[j + 1] = leaf.keys[j];
                leaf.rids[j + 1] = leaf.rids[j];
                j--;
            }
            leaf.keys[j + 1] = *key;
            leaf.rids[j + 1] = rid;
            leaf.base.numKeys++;

            // write back to the disk
            memset(leafPg.data, 0, PAGE_SIZE);
            offset = 0;
            memcpy(leafPg.data + offset, &leaf.base.type, sizeof(NodeType)); offset += sizeof(NodeType);
            memcpy(leafPg.data + offset, &leaf.base.numKeys, sizeof(int)); offset += sizeof(int);
            memcpy(leafPg.data + offset, &leaf.nextLeaf, sizeof(int)); offset += sizeof(int);
            for (int j = 0; j < leaf.base.numKeys; j++) {
                memcpy(leafPg.data + offset, &leaf.keys[j].v.intV, sizeof(int)); offset += sizeof(int);
                memcpy(leafPg.data + offset, &leaf.rids[j], sizeof(RID)); offset += sizeof(RID);
            }
            markDirty(bm, &leafPg);
            unpinPage(bm, &leafPg);
            mgmt->numEntries++;
            return RC_OK;
        }

        //  ortherwise split leaf
        Value promoteKey;
        int newLeafPage;
        splitLeaf(tree, &leaf, key, rid, &promoteKey, &newLeafPage);

        //  insert promoteKey into the internal node (insert in order)
        int pos = node.base.numKeys;
        while (pos > 0 && node.keys[pos - 1].v.intV > promoteKey.v.intV) {
            node.keys[pos] = node.keys[pos - 1];
            node.children[pos + 1] = node.children[pos];
            pos--;
        }
        node.keys[pos] = promoteKey;
        node.children[pos + 1] = newLeafPage;
        node.base.numKeys++;

        //  write back to internal nodes
        pinPage(bm, page, node.base.pageNum);
        memset(page->data, 0, PAGE_SIZE);
        offset = 0;
        memcpy(page->data + offset, &node.base.type, sizeof(NodeType)); offset += sizeof(NodeType);
        memcpy(page->data + offset, &node.base.numKeys, sizeof(int)); offset += sizeof(int);
        for (int i = 0; i < node.base.numKeys; i++) {
            memcpy(page->data + offset, &node.keys[i].v.intV, sizeof(int)); offset += sizeof(int);
        }
        for (int i = 0; i <= node.base.numKeys; i++) {
            memcpy(page->data + offset, &node.children[i], sizeof(int)); offset += sizeof(int);
        }
        markDirty(bm, page);
        unpinPage(bm, page);

        mgmt->numEntries++;
        return RC_OK;
            
    }
}


RC deleteKey(BTreeHandle *tree, Value *key) {
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle page;
    int currentPage = mgmt->rootPage;

    // traverse to the leaf node
    while (true) {
        pinPage(bm, &page, currentPage);

        int offset = 0;
        NodeType type;
        memcpy(&type, page.data + offset, sizeof(NodeType));
        offset += sizeof(NodeType);

        if (type == NODE_LEAF) {
            int numKeys, nextLeaf;
            memcpy(&numKeys, page.data + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(&nextLeaf, page.data + offset, sizeof(int));
            offset += sizeof(int);

            // find target key
            int found = -1;
            for (int i = 0; i < numKeys; i++) {
                int keyVal;
                memcpy(&keyVal, page.data + offset, sizeof(int));
                if (keyVal == key->v.intV) {
                    found = i;
                }
                // skip key + RID(both are int)
                offset += sizeof(int) + 2 * sizeof(int);
            }

            if (found == -1) {
                unpinPage(bm, &page);
                return RC_IM_KEY_NOT_FOUND;
            }

            // delete: Re-copy all key-value pairs except found
            offset = sizeof(NodeType) + 2 * sizeof(int);
            char temp[PAGE_SIZE];
            memset(temp, 0, PAGE_SIZE);
            NodeType nodeType = NODE_LEAF;
            memcpy(temp, &nodeType, sizeof(NodeType));
            int newNumKeys = numKeys - 1;
            memcpy(temp + sizeof(NodeType), &newNumKeys, sizeof(int));
            memcpy(temp + sizeof(NodeType) + sizeof(int), &nextLeaf, sizeof(int));

            int writeOffset = sizeof(NodeType) + 2 * sizeof(int);
            int readOffset = sizeof(NodeType) + 2 * sizeof(int);
            for (int i = 0; i < numKeys; i++) {
                int keyVal;
                RID rid;
                memcpy(&keyVal, page.data + readOffset, sizeof(int));
                readOffset += sizeof(int);
                memcpy(&rid.page, page.data + readOffset, sizeof(int));
                readOffset += sizeof(int);
                memcpy(&rid.slot, page.data + readOffset, sizeof(int));
                readOffset += sizeof(int);

                if (keyVal == key->v.intV)
                    continue; // skip deleted

                memcpy(temp + writeOffset, &keyVal, sizeof(int));
                writeOffset += sizeof(int);
                memcpy(temp + writeOffset, &rid.page, sizeof(int));
                writeOffset += sizeof(int);
                memcpy(temp + writeOffset, &rid.slot, sizeof(int));
                writeOffset += sizeof(int);
            }

            // write back
            memcpy(page.data, temp, PAGE_SIZE);
            markDirty(bm, &page);
            unpinPage(bm, &page);
            forceFlushPool(bm);

            mgmt->numEntries--;
            printf("[deleteKey] Deleted key=%d from leaf page=%d\n",
                   key->v.intV, currentPage);
            return RC_OK;
        } else {
            // internal node
            int numKeys;
            memcpy(&numKeys, page.data + offset, sizeof(int));
            offset += sizeof(int);

            int *keys = calloc(numKeys, sizeof(int));
            for (int i = 0; i < numKeys; i++) {
                memcpy(&keys[i], page.data + offset, sizeof(int));
                offset += sizeof(int);
            }

            int *children = calloc(numKeys + 1, sizeof(int));
            for (int i = 0; i <= numKeys; i++) {
                memcpy(&children[i], page.data + offset, sizeof(int));
                offset += sizeof(int);
            }

            int i;
            for (i = 0; i < numKeys; i++) {
                if (key->v.intV < keys[i])
                    break;
            }
            currentPage = children[i];

            free(keys);
            free(children);
            unpinPage(bm, &page);
        }
    }
}


/* 
   5. Scanning (iterate over sorted entries)
*/
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    BTreeMgmtData *mgmt = (BTreeMgmtData *) tree->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle page;
    int currentPage = mgmt->rootPage;

    // go straight down to the leftmost leaf
    while (true) {
        pinPage(bm, &page, currentPage);
        NodeType type;
        memcpy(&type, page.data, sizeof(NodeType));

        if (type == NODE_LEAF)
            break;

        int numKeys;
        memcpy(&numKeys, page.data + sizeof(NodeType), sizeof(int));

        // calculate the offset of the first child node
        int offset = sizeof(NodeType) + sizeof(int) + numKeys * sizeof(int);
        int firstChild;
        memcpy(&firstChild, page.data + offset, sizeof(int));

        unpinPage(bm, &page);
        currentPage = firstChild;
    }

    // initialize the scan data structure
    BT_ScanHandle *sc = malloc(sizeof(BT_ScanHandle));
    ScanMgmtData *scan = malloc(sizeof(ScanMgmtData));

    sc->tree = tree;
    scan->currentPage = currentPage;
    scan->keyIndex = -1;  //   must -1 
    scan->end = false;
    sc->mgmtData = scan;

    unpinPage(bm, &page);

    printf("Opened tree scan (start leaf=%d).\n", currentPage);

    *handle = sc;
    return RC_OK;
}


RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    BTreeHandle *tree = handle->tree;
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    ScanMgmtData *scan = (ScanMgmtData *)handle->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle page;

    // if the scan is complete
    if (scan->end)
        return RC_IM_NO_MORE_ENTRIES;

    pinPage(bm, &page, scan->currentPage);

    int offset = 0;
    NodeType type;
    memcpy(&type, page.data + offset, sizeof(NodeType)); offset += sizeof(NodeType);
    if (type != NODE_LEAF) {
        printf("[nextEntry ERROR] page %d is not leaf\n", scan->currentPage);
        unpinPage(bm, &page);
        return -1;
    }

    int numKeys;
    memcpy(&numKeys, page.data + offset, sizeof(int)); offset += sizeof(int);

    int nextLeaf;
    memcpy(&nextLeaf, page.data + offset, sizeof(int)); offset += sizeof(int);

    //  ++ before each entry, so that the 0th key can be read when keyIndex = -1
    scan->keyIndex++;

    //  if the current page is finished
    if (scan->keyIndex >= numKeys) {
        if (nextLeaf == -1) {
            // there is no next page, scanning is completed
            scan->end = true;
            unpinPage(bm, &page);
            return RC_IM_NO_MORE_ENTRIES;
        }

        // otherwise jump to the next page
        unpinPage(bm, &page);
        scan->currentPage = nextLeaf;
        scan->keyIndex = -1;  //  the next page starts from the beginning (0 after the next ++)
        return nextEntry(handle, result);
    }

    // each entry = key(4B) + page(4B) + slot(4B)
    int entryOffset = offset + scan->keyIndex * (sizeof(int) + sizeof(int) + sizeof(int));
    int keyVal;
    memcpy(&keyVal, page.data + entryOffset, sizeof(int)); entryOffset += sizeof(int);
    memcpy(&result->page, page.data + entryOffset, sizeof(int)); entryOffset += sizeof(int);
    memcpy(&result->slot, page.data + entryOffset, sizeof(int));

    printf("[SCAN] page=%d keyIndex=%d key=%d rid=(%d,%d)\n",
           scan->currentPage, scan->keyIndex, keyVal, result->page, result->slot);

    unpinPage(bm, &page);
    return RC_OK;
}



RC closeTreeScan(BT_ScanHandle *handle)
{
    if (!handle)
        return -1;
    free(handle->mgmtData);
    free(handle);
    printf("Closed tree scan.\n");
    return RC_OK;
}

#define INDENT_STEP 4

//  auxiliary recursive function,
static void printNode(BTreeHandle *tree, int pageNum, int depth, char *result) {
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle ph;
    int offset = 0;

    pinPage(bm, &ph, pageNum);

    NodeType type;
    memcpy(&type, ph.data + offset, sizeof(NodeType));
    offset += sizeof(NodeType);

    // print indent
    for (int i = 0; i < depth * INDENT_STEP; i++)
        strcat(result, " ");

    char buf[256];
    if (type == NODE_LEAF) {
        int numKeys, nextLeaf;
        memcpy(&numKeys, ph.data + offset, sizeof(int)); offset += sizeof(int);
        memcpy(&nextLeaf, ph.data + offset, sizeof(int)); offset += sizeof(int);

        sprintf(buf, "LEAF(page=%d,next=%d): [", pageNum, nextLeaf);
        strcat(result, buf);

        for (int i = 0; i < numKeys; i++) {
            int key;
            RID rid;
            memcpy(&key, ph.data + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(&rid.page, ph.data + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(&rid.slot, ph.data + offset, sizeof(int));
            offset += sizeof(int);

            sprintf(buf, "%d(rid=%d,%d)", key, rid.page, rid.slot);
            strcat(result, buf);
            if (i != numKeys - 1) strcat(result, ",");
        }


        strcat(result, "]\n");
    } else {
        int numKeys;
        memcpy(&numKeys, ph.data + offset, sizeof(int)); offset += sizeof(int);
        sprintf(buf, "INTERNAL(page=%d): [", pageNum);
        strcat(result, buf);

        int *keys = calloc(numKeys, sizeof(int));
        for (int i = 0; i < numKeys; i++) {
            memcpy(&keys[i], ph.data + offset, sizeof(int));
            offset += sizeof(int);
            sprintf(buf, "%d", keys[i]);
            strcat(result, buf);
            if (i != numKeys - 1) strcat(result, ",");
        }
        strcat(result, "]\n");

        int *children = calloc(numKeys + 1, sizeof(int));
        for (int i = 0; i <= numKeys; i++) {
            memcpy(&children[i], ph.data + offset, sizeof(int));
            offset += sizeof(int);
        }

        //  recursively print child nodes
        for (int i = 0; i <= numKeys; i++) {
            printNode(tree, children[i], depth + 1, result);
        }

        free(keys);
        free(children);
    }

    unpinPage(bm, &ph);
}

// 
char *printTree(BTreeHandle *tree) {
    char *result = calloc(5000, sizeof(char));
    strcpy(result, "=== B+ Tree Structure ===\n");
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;

    printNode(tree, mgmt->rootPage, 0, result);
    strcat(result, "==========================\n");

    return result;
}
