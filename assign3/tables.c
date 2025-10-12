#include "tables.h"
#include "record_mgr.h"  // for getAttr()
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// ------------------------------------------------------------
// Convert string like "i:10", "f:3.14", "b:true", "s:hello"
// into a Value structure
// ------------------------------------------------------------
Value *stringToValue(char *value) {
    Value *v = (Value *) malloc(sizeof(Value));

    if (value == NULL || strlen(value) == 0) {
        printf("[stringToValue] ERROR: empty or null string!\n");
        MAKE_VALUE(v, DT_INT, 0);
        return v;
    }

    switch (value[0]) {
        case 'i': { // integer
            char *num = value + 1;
            if (num == NULL || *num == '\0') {
                printf("[stringToValue] WARNING: missing int after 'i'\n");
                MAKE_VALUE(v, DT_INT, 0);
            } else {
                MAKE_VALUE(v, DT_INT, atoi(num));
            }
            break;
        }

        case 'f': { // float
            char *num = value + 1;
            if (num == NULL || *num == '\0') {
                printf("[stringToValue] WARNING: missing float after 'f'\n");
                MAKE_VALUE(v, DT_FLOAT, 0.0);
            } else {
                MAKE_VALUE(v, DT_FLOAT, atof(num));
            }
            break;
        }

        case 's': { // string
            char *str = value + 1;
            MAKE_STRING_VALUE(v, str);
            break;
        }

        case 'b': { // bool
            char *boolStr = value + 1;
            bool b = (strcmp(boolStr, "true") == 0) ? true : false;
            MAKE_VALUE(v, DT_BOOL, b);
            break;
        }

        default:
            printf("[stringToValue] ERROR: unknown type prefix '%c'\n", value[0]);
            MAKE_VALUE(v, DT_INT, 0);
            break;
    }

    return v;
}


// ------------------------------------------------------------
// Convert Value structure into printable string
// ------------------------------------------------------------
char *serializeValue(Value *val) {
    char *result = (char *) malloc(100);
    memset(result, 0, 100);

    switch (val->dt) {
        case DT_INT:
            sprintf(result, "%d", val->v.intV);
            break;
        case DT_FLOAT:
            sprintf(result, "%.2f", val->v.floatV);
            break;
        case DT_BOOL:
            sprintf(result, "%s", val->v.boolV ? "true" : "false");
            break;
        case DT_STRING:
            sprintf(result, "%s", val->v.stringV);
            break;
    }

    return result;
}

// ------------------------------------------------------------
// Serialize Schema: convert to string "numAttr name type len ..."
// ------------------------------------------------------------
char *serializeSchema(Schema *schema) {
    char *result = (char *) malloc(2048);
    memset(result, 0, 2048);

    sprintf(result, "%d ", schema->numAttr);
    for (int i = 0; i < schema->numAttr; i++) {
        char buf[128];
        sprintf(buf, "%s %d %d ", schema->attrNames[i], schema->dataTypes[i], schema->typeLength[i]);
        strcat(result, buf);
    }

    char buf2[64];
    sprintf(buf2, "%d ", schema->keySize);
    strcat(result, buf2);
    for (int i = 0; i < schema->keySize; i++) {
        char buf3[32];
        sprintf(buf3, "%d ", schema->keyAttrs[i]);
        strcat(result, buf3);
    }

    return result;
}

// ------------------------------------------------------------
// Serialize Record: show all attribute values
// ------------------------------------------------------------
char *serializeRecord(Record *record, Schema *schema) {
    char *result = (char *) malloc(2048);
    memset(result, 0, 2048);

    strcat(result, "(");
    for (int i = 0; i < schema->numAttr; i++) {
        Value *val;
        getAttr(record, schema, i, &val);
        char *valStr = serializeValue(val);
        strcat(result, valStr);
        if (i < schema->numAttr - 1)
            strcat(result, ",");
        free(valStr);
        free(val);
    }
    strcat(result, ")");

    return result;
}

// ------------------------------------------------------------
// Serialize one attribute only
// ------------------------------------------------------------
char *serializeAttr(Record *record, Schema *schema, int attrNum) {
    Value *val;
    getAttr(record, schema, attrNum, &val);
    char *valStr = serializeValue(val);
    free(val);
    return valStr;
}

// ------------------------------------------------------------
// Dummy placeholders for completeness

// ------------------------------------------------------------
char *serializeTableInfo(RM_TableData *rel) {
    char *info = (char *) malloc(64);
    sprintf(info, "Table: %s, #attrs=%d", rel->name, rel->schema->numAttr);
    return info;
}

char *serializeTableContent(RM_TableData *rel) {
    // could be implemented later if test case needs it
    char *content = strdup("<table content not implemented>");
    return content;
}
