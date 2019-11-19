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

package io.cdap.plugin.google.sheets.source;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.source.utils.ColumnComplexSchemaInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Util class for building pipeline schema.
 */
public class SchemaBuilder {
  public static final String SCHEMA_ROOT_RECORD_NAME = "RowRecord";
  public static final String SPREADSHEET_NAME_FIELD_NAME = "spreadSheetName";
  public static final String SHEET_TITLE_FIELD_NAME = "sheetTitle";

  public static Schema buildSchema(GoogleSheetsSourceConfig config,
                                   LinkedHashMap<Integer, ColumnComplexSchemaInfo> dataSchemaInfo) {
    List<Schema.Field> generalFields = new ArrayList<>();
    generalFields.add(Schema.Field.of(SPREADSHEET_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    generalFields.add(Schema.Field.of(SHEET_TITLE_FIELD_NAME, Schema.of(Schema.Type.STRING)));

    for (ColumnComplexSchemaInfo schemaInfo : dataSchemaInfo.values()) {
      if (schemaInfo.getSubColumns().isEmpty()) {
        generalFields.add(Schema.Field.of(schemaInfo.getHeaderTitle(), Schema.nullableOf(schemaInfo.getDataSchema())));
      } else {
        List<Schema.Field> recordFields = schemaInfo.getSubColumns().stream()
          .map(c -> Schema.Field.of(c.getHeaderTitle(), Schema.nullableOf(c.getDataSchema())))
          .collect(Collectors.toList());
        generalFields.add(Schema.Field.of(schemaInfo.getHeaderTitle(),
          Schema.nullableOf(Schema.recordOf(schemaInfo.getHeaderTitle(), recordFields))));
      }
    }

    if (config.isExtractMetadata()) {
      generalFields.add(Schema.Field.of(config.getMetadataRecordName(),
          Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
    }

    return Schema.recordOf(SCHEMA_ROOT_RECORD_NAME, generalFields);
  }
}
