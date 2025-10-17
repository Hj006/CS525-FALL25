#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "tables.h"
#include "expr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
Achieve Record Manager in this file 
What can this file do?
Create and open a table
Insert, delete, update, and read records
Scan a table, supporting filtering by conditions like WHERE clauses
Define and manage schemas, which are information about the structure of a table
*/


// store table management info
typedef struct TableMgmtData {
    BM_BufferPool *bm;
    int numTuples;
} TableMgmtData;


// record the number of free record slots in a page 
typedef struct RM_PageInfo {
    int freeSlots;   
} RM_PageInfo;


// used to record the scanned location
typedef struct ScanMgmtData {
    int currentPage;      // current page being scanned
    int currentSlot;      // current slot within that page
    Expr *cond;           // condition expression
    BM_PageHandle ph;     // current page handle
} ScanMgmtData;


// parseSchemaString: parse text created by serializeSchema()
// Example:
//   Schema with <3> attributes (id: INT, name: STRING[4], age: INT) with keys: (id)
static Schema *parseSchemaString(char *str) {
    // allocate the memory of schema
    Schema *schema = malloc(sizeof(Schema));
    memset(schema, 0, sizeof(Schema));
    
    // allocate memory for each field array of the Schema;
    schema->numAttr = 0;
    schema->attrNames = malloc(sizeof(char *) * 20);
    schema->dataTypes = malloc(sizeof(DataType) * 20);
    schema->typeLength = malloc(sizeof(int) * 20);
    schema->keyAttrs = malloc(sizeof(int) * 20);

    char *p = strstr(str, "with <"); // based on example, we find : 'with <', then we find: 3
    if (!p) return NULL;
    p += 6;
    schema->numAttr = atoi(p);

    p = strchr(p, '('); // find the first '('
    if (!p) return NULL;
    p++;

    for (int i = 0; i < schema->numAttr; i++) {
        char attrName[100], typeStr[100];
        int len = 0;
        memset(attrName, 0, sizeof(attrName));
        memset(typeStr, 0, sizeof(typeStr));

        // Parse "id: TYPE" or "name: STRING[4]"
        if (sscanf(p, "%[^:]: %[^,)]", attrName, typeStr) != 2)
            break;

        schema->attrNames[i] = strdup(attrName);  // copy
        
        // Parse and store in schema->typeLength[i]
        if (strncmp(typeStr, "INT", 3) == 0)
            schema->dataTypes[i] = DT_INT;
        else if (strncmp(typeStr, "FLOAT", 5) == 0)
            schema->dataTypes[i] = DT_FLOAT;
        else if (strncmp(typeStr, "BOOL", 4) == 0)
            schema->dataTypes[i] = DT_BOOL;
        else if (strncmp(typeStr, "STRING", 6) == 0) {
            sscanf(typeStr, "STRING[%d]", &len);
            schema->dataTypes[i] = DT_STRING;
        }
        schema->typeLength[i] = len;

        // jump to the next
        p = strchr(p, ',');
        if (!p) break;
        p++;
    }

    // set key
    schema->keySize = 1;
    schema->keyAttrs[0] = 0;

    return schema;
}


// in rm_serializer.c, MAKE_VARSTRING() calls calloc(100, 0),
// which allocates zero bytes. this causes a segmentation fault on most systems (glibc >= 2.30).
// This replacement ensures calloc() always allocates at least 1 byte,
void *calloc(size_t n, size_t s) {
    if (s == 0) s = 1;          // ensure at least 1 byte per element
    return malloc(n * s);       // allocate a contiguous block manually
}



// Table & Record Manager


RC initRecordManager (void *mgmtData) {
    // just return RC_OK, to represent initialized successfully
    // no other requirements in hw
    return RC_OK;
}


RC shutdownRecordManager () {
    // close all open tables 
    // release globally allocated management structures 
    return RC_OK;
}


RC createTable(char *name, Schema *schema) {
    /*
    debug code
    printf("[DEBUG createTable] schema=%p numAttr=%d\n", schema, schema->numAttr);
    for (int i = 0; i < schema->numAttr; i++) {
        printf("  attr[%d] name=%p (%s) type=%d len=%d\n",
            i,
            schema->attrNames ? schema->attrNames[i] : NULL,
            schema->attrNames && schema->attrNames[i] ? schema->attrNames[i] : "(null)",
            schema->dataTypes ? schema->dataTypes[i] : -1,
            schema->typeLength ? schema->typeLength[i] : -1);
    }
    */
    RC rc;
    rc = createPageFile(name);
    if (rc != RC_OK) return rc;

    // initialize the buffer pool
    BM_BufferPool bm;
    rc = initBufferPool(&bm, name, 3, RS_LRU, NULL);
    if (rc != RC_OK) return rc;
    
    // page 0 was used to store metadata
    BM_PageHandle ph;
    rc = pinPage(&bm, &ph, 0);
    if (rc != RC_OK) {
        shutdownBufferPool(&bm);
        return rc;
    }

    int numTuples = 0;
    int firstFreePage = -1;

    // force allocation and freeing of memory several times to make the heap pointer cross the bad area
    for (int i = 0; i < 10; i++) {
        void *dummy = malloc(128);
        memset(dummy, 0, 128);
        free(dummy);
    }
    /*
    printf("[DEBUG before serializeSchema] keySize=%d keyAttrs=%p\n",
           schema->keySize, schema->keyAttrs);
    for (int i = 0; i < schema->keySize; i++) {
        printf("  keyAttrs[%d]=%d\n", i, schema->keyAttrs[i]);
    }
    */
    fflush(stdout);
    


    // serialization Schema
    char *tmp = serializeSchema(schema);
    // copy one
    char *serializedSchema = tmp ? strdup(tmp) : strdup("(null)");
    // printf("[DEBUG serializedSchema ptr=%p]\n", serializedSchema);
    
    // writing metadata
    memset(ph.data, 0, PAGE_SIZE);
    sprintf(ph.data, "%d\n%d\n%s\n", numTuples, firstFreePage, serializedSchema);

    markDirty(&bm, &ph);
    forcePage(&bm, &ph);
    unpinPage(&bm, &ph);

    forceFlushPool(&bm);
    shutdownBufferPool(&bm);

    free(serializedSchema);
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

    /*
    page 0 looks like:
    <numTuples>\n
    <firstFreePage>\n
    <serializedSchema>\n
    
    The table structure is defined in the third and subsequent lines, so we skip the first two lines below.
    */

    // skip line 1
    char *pos = strchr(pageData, '\n');
    if (pos == NULL) return RC_FILE_NOT_FOUND;
    pos++;

    // skip line 2
    pos = strchr(pos, '\n');
    if (pos == NULL) return RC_FILE_NOT_FOUND;
    pos++;

    //Schema *schema = parseSchemaString(pos);
    char *schemaCopy = strdup(pos);
    Schema *schema = parseSchemaString(schemaCopy);
    //free(schemaCopy);

    if (schema == NULL)
        return RC_RM_UNKOWN_DATATYPE;

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
    TableMgmtData *mgmt = (TableMgmtData *) rel->mgmtData;
    return mgmt->numTuples;
}


// Record 
/*
Page's structure:
| Slot 0 | Slot 1 | Slot 2 | Slot 3 | ...          |
| [tag][record_data][tag][record_data]...          |
*/

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
        //if (pageNum % 500 == 0)
        //printf("[insertRecord] currently on page=%d\n", pageNum);

        rc = pinPage(bm, &ph, pageNum);
        if (rc != RC_OK) { // if page not exists, create new empty page
            
            SM_FileHandle fh;
            openPageFile(rel->name, &fh);
            ensureCapacity(pageNum + 1, &fh);
            closePageFile(&fh);
            rc = pinPage(bm, &ph, pageNum);
            if (rc != RC_OK) { // check rc to prevent invalid ph.data access
                return rc;
            }  
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

    if (rel == NULL || rel->mgmtData == NULL)
        return RC_FILE_NOT_FOUND;
    
    TableMgmtData *mgmt = (TableMgmtData *) rel->mgmtData;
    BM_BufferPool *bm = mgmt->bm;
    BM_PageHandle ph;

    RC rc = pinPage(bm, &ph, id.page);
    if (rc != RC_OK) return rc;

    // calculate the offset
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
    
    // load the target page and calculate the position
    RC rc = pinPage(bm, &ph, record->id.page);
    if (rc != RC_OK) return rc;

    int recordSize = getRecordSize(rel->schema);
    int slotSize = recordSize + 1;  // containing tag
    int offset = record->id.slot * slotSize;

    if (offset + slotSize > PAGE_SIZE) {
        unpinPage(bm, &ph);
        return RC_READ_NON_EXISTING_PAGE;
    }
    
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

    if (record == NULL || record->data == NULL)
        return RC_RM_UNKOWN_DATATYPE;
    // check table handle validity
    if (rel == NULL || rel->mgmtData == NULL)
        return RC_FILE_NOT_FOUND;
    
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

    // boundary check
    if (offset + slotSize > PAGE_SIZE) { // avoid out-of-range access
        unpinPage(bm, &ph); 
        return RC_READ_NON_EXISTING_PAGE;
    }

    // check if this slot is occupied or not
    if (ph.data[offset] != '1') { //'1' means valid record
        unpinPage(bm, &ph);
        return RC_READ_NON_EXISTING_PAGE;
    }
    
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
    scanData->ph.pageNum = -1;
    scanData->ph.data = NULL;  
    scan->rel = rel;
    scan->mgmtData = scanData;

    return RC_OK;
}


RC next (RM_ScanHandle *scan, Record *record) {

    // fetch next record matching the scan condition
    /*
    progress:
    Loop through all pages;
    Scan all slots in each page;
    If the slot is not empty:
    Read the data as a record
    If cond == NULL → Return immediately;
    Otherwise, use evalExpr() to check if the condition is satisfied;
    If satisfied, return;
    Return RC_RM_NO_MORE_TUPLES after scanning all the slots.
    */

    if (scan == NULL || scan->rel == NULL || scan->mgmtData == NULL)
        return RC_FILE_NOT_FOUND;
    
    RM_TableData *rel = scan->rel;
    TableMgmtData *tableMgmt = (TableMgmtData *) rel->mgmtData;
    BM_BufferPool *bm = tableMgmt->bm;
    Schema *schema = rel->schema;
    ScanMgmtData *scanData = (ScanMgmtData *) scan->mgmtData;
    
    // calculate the maximum number of records that can be placed on each page
    int recordSize = getRecordSize(schema);
    int slotSize = recordSize + 1;
    int recordsPerPage = PAGE_SIZE / slotSize;
    Value *result = NULL;
    RC rc;
    
    // preventing infinite loops
    int totalPages = (tableMgmt->numTuples + recordsPerPage - 1) / recordsPerPage;
    if (totalPages == 0) totalPages = 1;

    while (scanData->currentPage <= totalPages) { // page layer
        rc = pinPage(bm, &scanData->ph, scanData->currentPage);
        if (rc != RC_OK) return rc;

        char *data = scanData->ph.data;

        for (; scanData->currentSlot < recordsPerPage; scanData->currentSlot++) { // slot layer
            int offset = scanData->currentSlot * slotSize;
            if (data[offset] != '1') continue; // skip empty slot

            record->id.page = scanData->currentPage;
            record->id.slot = scanData->currentSlot;
            memcpy(record->data, data + offset + 1, recordSize);

            if (scanData->cond == NULL) { // no conditions
                scanData->currentSlot++;
                unpinPage(bm, &scanData->ph);
                return RC_OK;
            }

            rc = evalExpr(record, schema, scanData->cond, &result); // evalExpr() evaluates a conditional expression
            if (rc == RC_OK && result != NULL && result->v.boolV == TRUE) {
                freeVal(result);
                scanData->currentSlot++;
                unpinPage(bm, &scanData->ph);
                return RC_OK;
            }
            if (result != NULL) freeVal(result);
        }

        unpinPage(bm, &scanData->ph);
        scanData->currentPage++;
        scanData->currentSlot = 0;
    }

    return RC_RM_NO_MORE_TUPLES;
}


RC closeScan (RM_ScanHandle *scan) {
    if (scan == NULL || scan->mgmtData == NULL)
        return RC_OK;

    ScanMgmtData *scanData = (ScanMgmtData *) scan->mgmtData;
    TableMgmtData *tableMgmt = (TableMgmtData *) scan->rel->mgmtData;

    if (scanData->ph.data != NULL) // preventing null pointers
        unpinPage(tableMgmt->bm, &scanData->ph);

    free(scanData);
    scan->mgmtData = NULL;
    return RC_OK;
}



//  Schema 


int getRecordSize (Schema *schema) {
    // return 0 if schema is invalid
    if (schema == NULL || schema->dataTypes == NULL || schema->typeLength == NULL)
        return 0;
    
    // calculate total number of bytes needed to store one record
    int size = 0;
    
    // add the corresponding number of bytes according to the type
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
    // return NULL if input parameters are invalid
    if (numAttr <= 0 || attrNames == NULL || dataTypes == NULL || typeLength == NULL)
        return NULL;    //schema creation failed
    
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
    if (keySize > 0) {    // allocate key attribute array only when keySize > 0 
        schema->keyAttrs = (int *) malloc(sizeof(int) * keySize);
        memcpy(schema->keyAttrs, keys, sizeof(int) * keySize);
    } else {
        schema->keyAttrs = NULL; // avoid undefined malloc(0) 
    }
    return schema;
}

RC freeSchema (Schema *schema) {
    // Return RC_OK for a NULL schema
    if (schema == NULL) return RC_OK;
    
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
    //int size = getRecordSize(schema) + 1;  // +1 for slot flag
    int size = getRecordSize(schema);
    (*record)->data = calloc(1, size);
    
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
    // validate input
    if (record == NULL || record->data == NULL || schema == NULL)
        return RC_FILE_NOT_FOUND;
    
    // check if attrNum is valid
    if (attrNum < 0 ||attrNum >= schema->numAttr)
        return RC_RM_UNKOWN_DATATYPE;

    int offset = 0;
    //char *data = record->data + 1;
    char *data = record->data;
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
            char *buf = malloc(schema->typeLength[attrNum] + 1);
            memset(buf, 0, schema->typeLength[attrNum] + 1);
            strncpy(buf, data, schema->typeLength[attrNum]);
            buf[schema->typeLength[attrNum]] = '\0';
            MAKE_STRING_VALUE(*value, buf);
            free(buf);
            //printf("[DEBUG getAttr] attrNum=%d typeLength=%d data='%s'\n",attrNum, schema->typeLength[attrNum], (*value)->v.stringV);
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
    // char *data = record->data + 1; // skip the flag byte
    char *data = record->data; 
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
        case DT_STRING: {
            memset(data, 0, schema->typeLength[attrNum] + 1); // Clear old data
            if (value->v.stringV != NULL) {
                // Allows writing of typeLength characters, and then fills in '\0'
                strncpy(data, value->v.stringV, schema->typeLength[attrNum]);
                data[schema->typeLength[attrNum]] = '\0';
            } else {
                data[0] = '\0';
            }
            //printf("[DEBUG setAttr] attrNum=%d write='%s'\n", attrNum, data);
            break;
        }

    }

    return RC_OK;
}
