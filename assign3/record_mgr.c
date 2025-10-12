#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "tables.h"
#include "expr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// store table management info
typedef struct TableMgmtData {
    BM_BufferPool *bm;
    int numTuples;
} TableMgmtData;

typedef struct RM_PageInfo {
    int freeSlots;   // number of free record slots in this page
} RM_PageInfo;

typedef struct ScanMgmtData {
    int currentPage;      // current page being scanned
    int currentSlot;      // current slot within that page
    Expr *cond;           // condition expression
    BM_PageHandle ph;     // current page handle
} ScanMgmtData;
// ------------------------------------------------------------
// Helper: parse schema string from page 0 metadata
// ------------------------------------------------------------
static Schema *parseSchemaString(char *schemaStr) {
    if (schemaStr == NULL) return NULL;

    Schema *schema = (Schema *) malloc(sizeof(Schema));
    char *token = strtok(schemaStr, " ");
    if (token == NULL) return NULL;

    // Parse number of attributes
    schema->numAttr = atoi(token);

    // Allocate arrays
    schema->attrNames = (char **) malloc(sizeof(char *) * schema->numAttr);
    schema->dataTypes = (DataType *) malloc(sizeof(DataType) * schema->numAttr);
    schema->typeLength = (int *) malloc(sizeof(int) * schema->numAttr);

    // Parse attribute info: name type len
    for (int i = 0; i < schema->numAttr; i++) {
        token = strtok(NULL, " ");
        schema->attrNames[i] = strdup(token);

        token = strtok(NULL, " ");
        schema->dataTypes[i] = (DataType) atoi(token);

        token = strtok(NULL, " ");
        schema->typeLength[i] = atoi(token);
    }

    // Parse key info
    token = strtok(NULL, " ");
    schema->keySize = atoi(token);
    schema->keyAttrs = (int *) malloc(sizeof(int) * schema->keySize);

    for (int i = 0; i < schema->keySize; i++) {
        token = strtok(NULL, " ");
        schema->keyAttrs[i] = atoi(token);
    }

    return schema;
}

// Table & Record Manager


RC initRecordManager (void *mgmtData) {
    // just return RC_OK, to represent initialized successfully
    // no other requirements in hw
    return RC_OK;
}

RC shutdownRecordManager () {
    // Close all open tables 
    // Release globally allocated management structures 
    // 
    // Return success
    return RC_OK;
}

RC createTable (char *name, Schema *schema) {
    // create a new table and write schema information
    // mapping to CREATE TABLE
    // page 0: stores metadata, the schema, total number of records, and the first free page number
    // pages 1-N: store the actual record data.

    RC rc;

    // create pagefile
    rc = createPageFile(name);
    if (rc != RC_OK) return rc;

    // initialize Buffer Pool
    BM_BufferPool bm;
    rc = initBufferPool(&bm, name, 3, RS_LRU, NULL);
    if (rc != RC_OK) return rc;

    // this is page 0
    BM_PageHandle ph;
    rc = pinPage(&bm, &ph, 0);
    if (rc != RC_OK) {
        shutdownBufferPool(&bm);
        return rc;
    }

    // information in page 0
    int numTuples = 0;          // total number of records in the table
    int firstFreePage = -1;     // first free page number, -1 means none
    char *serializedSchema = serializeSchema(schema);

    // clear the page
    memset(ph.data, 0, PAGE_SIZE);

    // write table metadata into page 0
    sprintf(ph.data, "%d\n%d\n%s\n", numTuples, firstFreePage, serializedSchema); 
    // line 1: number of tuples
    // line 2: first free page
    // line 3+: schema info
    // Debug print 
    printf("[createTable] Writing metadata:\n%s\n", ph.data);
    free(serializedSchema);

    // mark page dirty, write back to disk, unpin
    markDirty(&bm, &ph);
    forcePage(&bm, &ph);
    unpinPage(&bm, &ph);

    // flush all pages and close buffer pool
    forceFlushPool(&bm);
    shutdownBufferPool(&bm);

    return RC_OK;
}


RC openTable (RM_TableData *rel, char *name) {
    BM_BufferPool *bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
    BM_PageHandle *ph = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
    RC rc;

    // initialize buffer pool
    rc = initBufferPool(bm, name, 3, RS_LRU, NULL);
    if (rc != RC_OK) {
        free(bm);
        free(ph);
        return rc;
    }

    // pin the first page
    rc = pinPage(bm, ph, 0);
    if (rc != RC_OK) {
        shutdownBufferPool(bm);
        free(bm);
        free(ph);
        return rc;
    }

    // read information from schema
    char *pageData = ph->data;
    int numTuples = atoi(pageData);

    // skip line 1
    char *pos = strchr(pageData, '\n');
    if (pos == NULL) return RC_FILE_NOT_FOUND;
    pos++;

    // skip line 2
    pos = strchr(pos, '\n');
    if (pos == NULL) return RC_FILE_NOT_FOUND;
    pos++;

    Schema *schema = parseSchemaString(pos);


    // initialize the table structure
    rel->name = strdup(name);
    rel->schema = schema;



    TableMgmtData *mgmt = (TableMgmtData *) malloc(sizeof(TableMgmtData));
    mgmt->bm = bm;
    mgmt->numTuples = numTuples;
    rel->mgmtData = mgmt;

    unpinPage(bm, ph);
    free(ph);

    return RC_OK;
}

RC closeTable (RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL)
        return RC_FILE_NOT_FOUND;

    typedef struct TableMgmtData {
        BM_BufferPool *bm;
        int numTuples;
    } TableMgmtData;

    TableMgmtData *mgmt = (TableMgmtData *) rel->mgmtData;
    BM_BufferPool *bm = mgmt->bm;

    // written the dirty pages
    forceFlushPool(bm);

    // force buffer pool to close
    shutdownBufferPool(bm);

    free(bm);
    free(mgmt);
    rel->mgmtData = NULL;

    return RC_OK;
}

RC deleteTable (char *name) {
    return destroyPageFile(name);
}

int getNumTuples (RM_TableData *rel) {
    return 0;
}


// Record 


RC insertRecord (RM_TableData *rel, Record *record) {
    // insert one record into the table
    // mapping to INSERT INTO table VALUES (...)

    // get table management info
    TableMgmtData *mgmt = (TableMgmtData *) rel->mgmtData;
    BM_BufferPool *bm = mgmt->bm; // pointer to this table’s buffer pool
    BM_PageHandle ph;

    int recordSize = getRecordSize(rel->schema);
    int slotSize = recordSize + 1; // one more for tag
    int recordsPerPage = PAGE_SIZE / slotSize; //  records/page
   
    // try from page 1, page 0 is metadata
    int pageNum = 1;
    RC rc;

    while (1) {
        if (pageNum % 500 == 0)
        printf("[insertRecord] currently on page=%d\n", pageNum);

        rc = pinPage(bm, &ph, pageNum);
        if (rc != RC_OK) { // Not enough, expand
            // if page not exists, create new empty page
            SM_FileHandle fh;
            openPageFile(rel->name, &fh);
            ensureCapacity(pageNum + 1, &fh);
            closePageFile(&fh);
            rc = pinPage(bm, &ph, pageNum);
            
            //  prevent garbage data
            memset(ph.data, '0', PAGE_SIZE);
            markDirty(bm, &ph);
        }

        // search for free slot in this page
        bool found = false;
        for (int i = 0; i < recordsPerPage; i++) {
            int offset = i * slotSize;

            //printf("[insertRecord] page=%d slot=%d size=%d ", pageNum, i, recordSize);
            //printf("data bytes: ");
            //for (int k = 0; k < recordSize; k++) {
            //    printf("%02X ", (unsigned char)record->data[k]);
            //}
            //printf("\n");

            if (ph.data[offset] == '0' || ph.data[offset] == '\0') { // empty slot
                ph.data[offset] = '1'; // marked as used
                memcpy(ph.data + offset+1, record->data, recordSize);

                // set record ID
                record->id.page = pageNum;
                record->id.slot = i;

                // mark dirty and unpin
                markDirty(bm, &ph);
                unpinPage(bm, &ph);
                mgmt->numTuples++;
                found = true;
                break;
            }
        }

        if (found) break; // inserted successfully
        unpinPage(bm, &ph);
        pageNum++; // move to next page
    }

    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id) {
    // delete record by marking slot empty

    TableMgmtData *mgmt = (TableMgmtData *) rel->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle ph;

    RC rc = pinPage(bm, &ph, id.page);
    if (rc != RC_OK) return rc;

    int recordSize = getRecordSize(rel->schema);
    int slotSize = recordSize + 1;
    int offset = id.slot * slotSize;

    ph.data[offset] = '0'; // mark as empty
    memset(ph.data + offset + 1, 0, recordSize); // clear record data

    markDirty(bm, &ph);
    unpinPage(bm, &ph);

    mgmt->numTuples--;

    return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {
    // update record content by RID

    TableMgmtData *mgmt = (TableMgmtData *) rel->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle ph;

    RC rc = pinPage(bm, &ph, record->id.page);
    if (rc != RC_OK) return rc;

    int recordSize = getRecordSize(rel->schema);
    int slotSize = recordSize + 1;  // containing tag
    int offset = record->id.slot * slotSize;

    // overwrite record content, skipping tag byte
    ph.data[offset] = '1';  // remark
    memcpy(ph.data + offset + 1, record->data, recordSize);

    markDirty(bm, &ph);
    unpinPage(bm, &ph);

    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record) {
    // get record data by RID
    // mapping to SELECT * FROM table WHERE RID = id

    TableMgmtData *mgmt = (TableMgmtData *) rel->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle ph;

    RC rc = pinPage(bm, &ph, id.page);
    if (rc != RC_OK) return rc;

    int recordSize = getRecordSize(rel->schema);
    int slotSize = recordSize + 1;
    int offset = id.slot * slotSize;


    //printf("[getRecord] page=%d slot=%d size=%d ", id.page, id.slot, recordSize);
    //printf("data bytes: ");
    //for (int k = 0; k < recordSize; k++) {
    //   printf("%02X ", (unsigned char)record->data[k]);
    //}
    //printf("\n");


    // copy data into record, skip the 1-byte slot tag
    memcpy(record->data, ph.data + offset+1, recordSize);
    record->id = id;

    unpinPage(bm, &ph);
    return RC_OK;
}


//  Scan 
// mapping to SELECT * FROM Students WHERE age > 20;


RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    // start a new scan on a table with a given condition
    // similar to "SELECT * FROM table WHERE cond"

    if (rel == NULL || rel->mgmtData == NULL)
        return RC_RM_UNKOWN_DATATYPE;

    // initialize scan structure
    ScanMgmtData *scanData = (ScanMgmtData *) malloc(sizeof(ScanMgmtData));
    scanData->currentPage = 1; // start from page 1 (page 0 is metadata)
    scanData->currentSlot = 0; // start from first slot
    scanData->cond = cond;

    scan->rel = rel;
    scan->mgmtData = scanData;

    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record) {
    // fetch next record matching the scan condition
/*
Loop through all pages;

Scan all slots in each page;

If the slot is not empty:

Read the data as a record

If cond == NULL → Return immediately;

Otherwise, use evalExpr() to check if the condition is satisfied;

If satisfied, return;

Return RC_RM_NO_MORE_TUPLES after scanning all the slots.

*/
    RM_TableData *rel = scan->rel;
    TableMgmtData *tableMgmt = (TableMgmtData *) rel->mgmtData;
    BM_BufferPool *bm = tableMgmt->bm;
    Schema *schema = rel->schema;

    ScanMgmtData *scanData = (ScanMgmtData *) scan->mgmtData;

    int recordSize = getRecordSize(schema);
    int recordsPerPage = PAGE_SIZE / recordSize;

    Value *result = NULL;

    while (scanData->currentPage < (1 + (tableMgmt->numTuples / recordsPerPage) + 1)) {

        RC rc = pinPage(bm, &scanData->ph, scanData->currentPage);
        if (rc != RC_OK) return rc;

        char *data = scanData->ph.data;

        // scan all slots on current page
        for (; scanData->currentSlot < recordsPerPage; scanData->currentSlot++) {
            int offset = scanData->currentSlot * recordSize;
            if (data[offset] == '\0') continue; // empty slot, skip

            // load record
            record->id.page = scanData->currentPage;
            record->id.slot = scanData->currentSlot;
            memcpy(record->data, data + offset, recordSize);

            // evaluate condition (if any)
            if (scanData->cond != NULL) {
                evalExpr(record, schema, scanData->cond, &result);

                if (result->v.boolV == TRUE) {
                    scanData->currentSlot++;
                    unpinPage(bm, &scanData->ph);
                    freeVal(result);
                    return RC_OK; // found matching record
                }
                freeVal(result);
            } else {
                // if no condition, return all records
                scanData->currentSlot++;
                unpinPage(bm, &scanData->ph);
                return RC_OK;
            }
        }

        // done scanning this page
        unpinPage(bm, &scanData->ph);
        scanData->currentPage++;
        scanData->currentSlot = 0;
    }

    return RC_RM_NO_MORE_TUPLES; // reached end
}

RC closeScan (RM_ScanHandle *scan) {
    // close scan and free scan management data
    if (scan->mgmtData != NULL)
        free(scan->mgmtData);
    scan->mgmtData = NULL;
    return RC_OK;
}


//  Schema 


int getRecordSize (Schema *schema) {
    // calculate total number of bytes needed to store one record
    int size = 0;

    for (int i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:
                size += sizeof(int);
                break;
            case DT_FLOAT:
                size += sizeof(float);
                break;
            case DT_BOOL:
                size += sizeof(bool);
                break;
            case DT_STRING:
                size += schema->typeLength[i];
                break;
        }
    }

    return size;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes,
                      int *typeLength, int keySize, int *keys) {
    // allocate memory for Schema
    Schema *schema = (Schema *) malloc(sizeof(Schema));

    schema->numAttr = numAttr;

    // copy attribute names
    schema->attrNames = (char **) malloc(sizeof(char *) * numAttr);
    for (int i = 0; i < numAttr; i++) {
        schema->attrNames[i] = (char *) malloc(strlen(attrNames[i]) + 1);
        strcpy(schema->attrNames[i], attrNames[i]);
    }

    // copy data types
    schema->dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
    memcpy(schema->dataTypes, dataTypes, sizeof(DataType) * numAttr);

    // copy type lengths
    schema->typeLength = (int *) malloc(sizeof(int) * numAttr);
    memcpy(schema->typeLength, typeLength, sizeof(int) * numAttr);

    // copy key info
    schema->keySize = keySize;
    schema->keyAttrs = (int *) malloc(sizeof(int) * keySize);
    memcpy(schema->keyAttrs, keys, sizeof(int) * keySize);

    return schema;
}

RC freeSchema (Schema *schema) {
    // free each attribute name
    for (int i = 0; i < schema->numAttr; i++) {
        free(schema->attrNames[i]);
    }

    // free arrays
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);

    // free schema itself
    free(schema);

    return RC_OK;
}


// Record & Attribute 
/*
record's sttucture looks like :
typedef struct Record {
    RID id;
    char *data;
} Record;


*/

RC createRecord (Record **record, Schema *schema) {
    // create a new, empty record
    *record = (Record *) malloc(sizeof(Record));
    int size = getRecordSize(schema) + 1;  // +1 for slot flag
    (*record)->data = (char *) malloc(size);
    
    // initialize record content
    memset((*record)->data, 0, size);
    (*record)->id.page = -1;
    (*record)->id.slot = -1;

    return RC_OK;
}

RC freeRecord (Record *record) {
    if (record == NULL)
        return RC_OK;
    free(record->data);
    free(record);
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
    // check if attrNum is valid
    if (attrNum >= schema->numAttr)
        return RC_RM_UNKOWN_DATATYPE;

    int offset = 0;
    char *data = record->data + 1;
    // compute offset for this attribute
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT: offset += sizeof(int); break;
            case DT_FLOAT: offset += sizeof(float); break;
            case DT_BOOL: offset += sizeof(bool); break;
            case DT_STRING: offset += schema->typeLength[i]; break;
        }
    }

    data += offset;

    // allocate Value and copy
    switch (schema->dataTypes[attrNum]) {
        case DT_INT: {
            int val;
            memcpy(&val, data, sizeof(int));
            MAKE_VALUE(*value, DT_INT, val);
            break;
        }
        case DT_FLOAT: {
            float val;
            memcpy(&val, data, sizeof(float));
            MAKE_VALUE(*value, DT_FLOAT, val);
            break;
        }
        case DT_BOOL: {
            bool val;
            memcpy(&val, data, sizeof(bool));
            MAKE_VALUE(*value, DT_BOOL, val);
            break;
        }
        case DT_STRING: {
            char *buf = (char *) malloc(schema->typeLength[attrNum] + 1);
            strncpy(buf, data, schema->typeLength[attrNum]);
            buf[schema->typeLength[attrNum]] = '\0';
            MAKE_STRING_VALUE(*value, buf);
            free(buf);
            break;
        }
    }

    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
    // check if attrNum is valid
    if (attrNum >= schema->numAttr)
        return RC_RM_UNKOWN_DATATYPE;

    int offset = 0;
    char *data = record->data + 1; // skip the flag byte
    // compute offset for this attribute
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT: offset += sizeof(int); break;
            case DT_FLOAT: offset += sizeof(float); break;
            case DT_BOOL: offset += sizeof(bool); break;
            case DT_STRING: offset += schema->typeLength[i]; break;
        }
    }

    data += offset;

    // write value into data
    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            memcpy(data, &(value->v.intV), sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(data, &(value->v.floatV), sizeof(float));
            break;
        case DT_BOOL:
            memcpy(data, &(value->v.boolV), sizeof(bool));
            break;
        case DT_STRING:
            memset(data, 0, schema->typeLength[attrNum]); // clear old content
            strncpy(data, value->v.stringV, schema->typeLength[attrNum] - 1);
            data[schema->typeLength[attrNum] - 1] = '\0'; // ensure null-terminated
            break;

    }

    return RC_OK;
}
