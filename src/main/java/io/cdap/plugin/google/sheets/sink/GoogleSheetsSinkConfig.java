/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.google.sheets.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleInputSchemaFieldsUsageConfig;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configurations for Google Sheets Batch Sink plugin.
 */
public class GoogleSheetsSinkConfig extends GoogleInputSchemaFieldsUsageConfig {
  public static final String SHEET_NAME_FIELD_NAME = "sheetName";
  public static final String SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME = "schemaSpreadSheetNameFieldName";
  public static final String SCHEMA_SHEET_NAME_FIELD_NAME = "schemaSheetNameFieldName";
  public static final String WRITE_SCHEMA_FIELD_NAME = "writeSchema";
  public static final String MERGE_DATA_CELLS_FIELD_NAME = "mergeDataCells";

  public static final List<Schema.LogicalType> ALLOWED_LOGICAL_TYPES = Arrays.asList(Schema.LogicalType.DATE,
      Schema.LogicalType.TIME_MILLIS,
      Schema.LogicalType.TIME_MICROS,
      Schema.LogicalType.TIMESTAMP_MILLIS,
      Schema.LogicalType.TIMESTAMP_MICROS);

  public static final List<Schema.Type> ALLOWED_TYPES = Arrays.asList(Schema.Type.STRING,
      Schema.Type.LONG,
      Schema.Type.INT,
      Schema.Type.DOUBLE,
      Schema.Type.FLOAT,
      Schema.Type.BYTES,
      Schema.Type.BOOLEAN,
      Schema.Type.NULL,
      Schema.Type.ARRAY,
      Schema.Type.RECORD);

  public static final List<Schema.Type> ALLOWED_NESTED_TYPES = Arrays.asList(Schema.Type.STRING,
      Schema.Type.LONG,
      Schema.Type.INT,
      Schema.Type.DOUBLE,
      Schema.Type.FLOAT,
      Schema.Type.BYTES,
      Schema.Type.BOOLEAN,
      Schema.Type.NULL);

  @Name(SHEET_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be BYTES type) which will be used as body of file.\n" +
    "The minimal input schema should contain only this field.")
  @Macro
  protected String sheetName;

  @Nullable
  @Name(SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as name of file. \n" +
    "Is optional. In the case it is not set files have randomly generated 16-symbols names.")
  @Macro
  protected String schemaSpreadSheetNameFieldName;

  @Nullable
  @Name(SCHEMA_SHEET_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as MIME type of file. \n" +
    "All MIME types are supported except Google Drive types: https://developers.google.com/drive/api/v3/mime-types.\n" +
    "Is optional. In the case it is not set Google API will try to recognize file's MIME type automatically.")
  @Macro
  protected String schemaSheetNameFieldName;

  @Name(WRITE_SCHEMA_FIELD_NAME)
  @Description("")
  @Macro
  private boolean writeSchema;

  @Name(MERGE_DATA_CELLS_FIELD_NAME)
  @Description("")
  @Macro
  private boolean mergeDataCells;

  public void validate(FailureCollector collector, Schema schema) {
    super.validate(collector);

    // validate name field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME, schemaSpreadSheetNameFieldName,
                        "File name field", Schema.Type.STRING);

    // validate mime field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_SHEET_NAME_FIELD_NAME, schemaSpreadSheetNameFieldName,
                        "File mime field", Schema.Type.STRING);

    // validate schema
    validateSchema(collector, schema);
  }

  private void validateSchema(FailureCollector collector, Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      if (!ALLOWED_LOGICAL_TYPES.contains(fieldSchema.getLogicalType())
          && !ALLOWED_TYPES.contains(fieldSchema.getType())) {
        collector.addFailure(
            String.format("Field '%s' has unsupported schema type '%s' and logical type '%s'",
                field.getName(), fieldSchema.getType(), fieldSchema.getLogicalType()),
            String.format(String.format("Allowed top level logical types: [%s] and types: [%s]",
                ALLOWED_LOGICAL_TYPES.toString(),
                ALLOWED_TYPES.toString())));
      }
      // for array and record check that they don't have nested complex structures
      if (Schema.Type.ARRAY.equals(fieldSchema.getType())) {
        Schema componentSchema = fieldSchema.getComponentSchema();
        if (!ALLOWED_LOGICAL_TYPES.contains(componentSchema.getLogicalType())
            && !ALLOWED_NESTED_TYPES.contains(componentSchema.getType())) {
          collector.addFailure(
              String.format("Array field '%s' has unsupported schema type '%s' and logical type '%s'",
                  field.getName(), componentSchema.getType(), componentSchema.getLogicalType()),
              String.format(String.format("Allowed array logical types: [%s] and types: [%s]",
                  ALLOWED_LOGICAL_TYPES.toString(),
                  ALLOWED_NESTED_TYPES.toString())));
        }
      }
      if (Schema.Type.RECORD.equals(fieldSchema.getType())) {
        for (Schema.Field nestedField : fieldSchema.getFields()) {
          Schema nestedComponentSchema = nestedField.getSchema().getComponentSchema();
          if (!ALLOWED_LOGICAL_TYPES.contains(nestedComponentSchema.getLogicalType())
              && !ALLOWED_NESTED_TYPES.contains(nestedComponentSchema.getType())) {
            collector.addFailure(
                String.format("Record '%s' has field '%s' with unsupported schema type '%s' and logical type '%s'",
                    field.getName(), nestedField.getName(), nestedComponentSchema.getType(),
                    nestedComponentSchema.getLogicalType()),
                String.format(String.format("Allowed record nested logical types: [%s] and types: [%s]",
                    ALLOWED_LOGICAL_TYPES.toString(),
                    ALLOWED_NESTED_TYPES.toString())));
          }
        }
      }
    }
  }

  public String getSheetName() {
    return sheetName;
  }

  @Nullable
  public String getSchemaSpreadSheetNameFieldName() {
    return schemaSpreadSheetNameFieldName;
  }

  @Nullable
  public String getSchemaSheetNameFieldName() {
    return schemaSheetNameFieldName;
  }

  public boolean isWriteSchema() {
    return writeSchema;
  }

  public boolean isMergeDataCells() {
    return mergeDataCells;
  }
}
