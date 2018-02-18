package com.ubs.risk.arisk;

import java.util.List;

public class SchemaObject {

    private String schemaName;
    private List<AriskField> fields;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<AriskField> getFields() {
        return fields;
    }

    public void setFields(List<AriskField> fields) {
        this.fields = fields;
    }
}

class AriskField {

    private String fieldName;
    private String fieldType;
    private String fieldOptional;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getFieldOptional() {
        return fieldOptional;
    }

    public void setFieldOptional(String fieldOptional) {
        this.fieldOptional = fieldOptional;
    }
}
