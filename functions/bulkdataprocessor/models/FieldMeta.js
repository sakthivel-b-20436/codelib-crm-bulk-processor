class FieldMeta {
  constructor() {
    this.fields = [];
    this.fieldDataTypeMap = {};
    this.readOnlyFields = [];
  }

  getFields() {
    return this.fields;
  }

  setFields(fields) {
    this.fields = fields;
  }

  getFieldDataTypeMap() {
    return this.fieldDataTypeMap;
  }

  setFieldDataTypeMap(map) {
    this.fieldDataTypeMap = map;
  }

  addField(fieldName, dataType, isReadOnly = false) {
    this.fields.push(fieldName);
    this.fieldDataTypeMap[fieldName] = dataType;
    if (isReadOnly) {
      this.readOnlyFields.push(fieldName);
    }
  }
}

module.exports = FieldMeta;
