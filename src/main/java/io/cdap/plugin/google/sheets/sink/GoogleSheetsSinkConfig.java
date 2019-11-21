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
  public static final String SPREADSHEET_NAME_FIELD_NAME = "spreadSheetName";
  public static final String SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME = "schemaSpreadSheetNameFieldName";
  public static final String SCHEMA_SHEET_NAME_FIELD_NAME = "schemaSheetNameFieldName";
  public static final String WRITE_SCHEMA_FIELD_NAME = "writeSchema";
  public static final String MERGE_DATA_CELLS_FIELD_NAME = "mergeDataCells";
  public static final String MIN_PAGE_EXTENSION_PAGE_FIELD_NAME = "minPageExtensionSize";
  public static final String THREADS_NUMBER_FIELD_NAME = "threadsNumber";
  public static final String MAX_BUFFER_SIZE_FIELD_NAME = "maxBufferSize";
  public static final String RECORDS_QUEUE_LENGTH_FIELD_NAME = "recordsQueueLength";
  public static final String MAX_FLUSH_INTERVAL_FIELD_NAME = "maxFlushInterval";
  public static final String FLUSH_EXECUTION_TIMEOUT_FIELD_NAME = "flushExecutionTimeout";

  public static final String TOP_LEVEL_SCHEMA_MESSAGE =
    "Field '%s' has unsupported schema type '%s' and logical type '%s'.";
  public static final String TOP_LEVEL_SCHEMA_CORRECTIVE_MESSAGE =
    "Allowed top level logical types: [%s] and types: [%s].";
  public static final String ARRAY_NESTED_SCHEMA_MESSAGE =
    "Array field '%s' has unsupported components' schema type '%s' and logical type '%s'.";
  public static final String ARRAY_NESTED_SCHEMA_CORRECTIVE_MESSAGE =
    "Allowed array nested logical types: [%s] and types: [%s].";
  public static final String RECORD_NESTED_SCHEMA_MESSAGE =
    "Record '%s' has field '%s' with unsupported schema type '%s' and logical type '%s'";
  public static final String RECORD_NESTED_SCHEMA_CORRECTIVE_MESSAGE =
    "Allowed record nested logical types: [%s] and types: [%s]";

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
  @Description("Default sheet title. Is used when user don't specify schema field as sheet title.")
  @Macro
  private String sheetName;

  @Name(SPREADSHEET_NAME_FIELD_NAME)
  @Description("Default spreadsheet file name. Is used when user don't specify schema field as spreadsheet name.")
  @Macro
  private String spreadSheetName;

  @Nullable
  @Name(SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as name of file. \n" +
    "Is optional. In the case it is not set Google API will use the value of **Default Spreadsheet name** property.")
  @Macro
  private String schemaSpreadSheetNameFieldName;

  @Nullable
  @Name(SCHEMA_SHEET_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as sheet title. \n" +
    "Is optional. In the case it is not set Google API will use the value of **Default sheet name** property.")
  @Macro
  private String schemaSheetNameFieldName;

  @Name(WRITE_SCHEMA_FIELD_NAME)
  @Description("Toggle that defines should the sink write out the input schema as first row of an out sheet.")
  @Macro
  private boolean writeSchema;

  @Name(MERGE_DATA_CELLS_FIELD_NAME)
  @Description("Toggle that defines should the sink merge data cells created as result of input arrays flatterning.")
  @Macro
  private boolean mergeDataCells;

  @Name(MIN_PAGE_EXTENSION_PAGE_FIELD_NAME)
  @Description("Minimal size of sheet extension when default sheet size (1000) was exceeded.")
  @Macro
  private int minPageExtensionSize;

  @Name(THREADS_NUMBER_FIELD_NAME)
  @Description("Number of threads which send batched API requests. " +
    "The greater value allows to process records quickly, but requires extended Google Sheets API quota.")
  @Macro
  private int threadsNumber;

  @Name(MAX_BUFFER_SIZE_FIELD_NAME)
  @Description("Maximal size in records of the batch API request. " +
    "The greater value allows to reduce the number of API requests, but causes growing of their size.")
  @Macro
  private int maxBufferSize;

  @Name(RECORDS_QUEUE_LENGTH_FIELD_NAME)
  @Description("Size of the queue used to receive records and for onwards grouping of them to batched API requests. " +
    "For the greater value there is more likely that the sink will group arrived records in the butches of " +
    "maximal size. Also greater value means more memory consumption.")
  @Macro
  private int recordsQueueLength;

  @Name(MAX_FLUSH_INTERVAL_FIELD_NAME)
  @Description("Interval with what the sink will try to get batched requests from the records queue and " +
    "send them to threads for sending to Sheets API.")
  @Macro
  private int maxFlushInterval;

  @Name(FLUSH_EXECUTION_TIMEOUT_FIELD_NAME)
  @Description("Maximal time the single thread should process the batched API request. " +
    "Be careful, the number of retries and maximal retry time also should be taken into account.")
  @Macro
  private int flushExecutionTimeout;

  public void validate(FailureCollector collector, Schema schema) {
    super.validate(collector);

    // validate name field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME, schemaSpreadSheetNameFieldName,
                        "Spreadsheet name field", Schema.Type.STRING);

    // validate mime field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_SHEET_NAME_FIELD_NAME, schemaSpreadSheetNameFieldName,
                        "Sheet name field", Schema.Type.STRING);

    // validate schema
    validateSchema(collector, schema);
  }

  private void validateSchema(FailureCollector collector, Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      checkSchemas(collector, fieldSchema, ALLOWED_LOGICAL_TYPES, ALLOWED_TYPES,
        String.format(TOP_LEVEL_SCHEMA_MESSAGE, field.getName(), fieldSchema.getType(), fieldSchema.getLogicalType()),
        String.format(TOP_LEVEL_SCHEMA_CORRECTIVE_MESSAGE, ALLOWED_LOGICAL_TYPES.toString(), ALLOWED_TYPES.toString()));
      // for array and record check that they don't have nested complex structures
      if (Schema.Type.ARRAY.equals(fieldSchema.getType())) {
        Schema componentSchema = fieldSchema.getComponentSchema();
        checkSchemas(collector, componentSchema, ALLOWED_LOGICAL_TYPES, ALLOWED_NESTED_TYPES,
          String.format(ARRAY_NESTED_SCHEMA_MESSAGE, field.getName(),
            componentSchema.getType(), componentSchema.getLogicalType()),
          String.format(ARRAY_NESTED_SCHEMA_CORRECTIVE_MESSAGE, ALLOWED_LOGICAL_TYPES.toString(),
            ALLOWED_NESTED_TYPES.toString()));
      }
      if (Schema.Type.RECORD.equals(fieldSchema.getType())) {
        for (Schema.Field nestedField : fieldSchema.getFields()) {
          Schema nestedComponentSchema = nestedField.getSchema();
          checkSchemas(collector, nestedComponentSchema, ALLOWED_LOGICAL_TYPES, ALLOWED_NESTED_TYPES,
            String.format(RECORD_NESTED_SCHEMA_MESSAGE, field.getName(), nestedField.getName(),
              nestedComponentSchema.getType(), nestedComponentSchema.getLogicalType()),
            String.format(String.format(RECORD_NESTED_SCHEMA_CORRECTIVE_MESSAGE,
              ALLOWED_LOGICAL_TYPES.toString(), ALLOWED_NESTED_TYPES.toString())));
        }
      }
    }
  }

  private void checkSchemas(FailureCollector collector, Schema fieldSchema,
                            List<Schema.LogicalType> allowedLogicalTypes, List<Schema.Type> allowedTypes,
                            String message, String correctiveAction) {
    Schema.LogicalType nonNullableLogicalType = fieldSchema.isNullable() ?
      fieldSchema.getNonNullable().getLogicalType() :
      fieldSchema.getLogicalType();
    Schema.Type nonNullableType = fieldSchema.isNullable() ?
      fieldSchema.getNonNullable().getType() :
      fieldSchema.getType();
    if (!allowedLogicalTypes.contains(nonNullableLogicalType) && !allowedTypes.contains(nonNullableType)) {
      collector.addFailure(message, correctiveAction);
    }
  }

  public String getSheetName() {
    return sheetName;
  }

  public String getSpreadSheetName() {
    return spreadSheetName;
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

  public int getMinPageExtensionSize() {
    return minPageExtensionSize;
  }

  public int getThreadsNumber() {
    return threadsNumber;
  }

  public int getMaxBufferSize() {
    return maxBufferSize;
  }

  public int getRecordsQueueLength() {
    return recordsQueueLength;
  }

  public int getMaxFlushInterval() {
    return maxFlushInterval;
  }

  public int getFlushExecutionTimeout() {
    return flushExecutionTimeout;
  }
}
