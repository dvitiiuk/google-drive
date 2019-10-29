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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Util class for building pipeline schema.
 */
public class SchemaBuilder {
  public static final String SCHEMA_ROOT_RECORD_NAME = "Sheet";
  public static final String SPREADSHEET_NAME_FIELD_NAME = "spreadSheetName";
  public static final String SHEET_TITLE_FIELD_NAME = "sheetTitle";
  public static final String METADATA_FIELD_NAME = "metadata";

  public static Schema buildSchema(GoogleSheetsSourceConfig config, LinkedHashMap<Integer, String> headerTitles) {
    List<Schema.Field> generalFields = new ArrayList<>();
    generalFields.add(Schema.Field.of(SPREADSHEET_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    generalFields.add(Schema.Field.of(SHEET_TITLE_FIELD_NAME, Schema.of(Schema.Type.STRING)));

    for (String headerTitle : headerTitles.values()) {
      generalFields.add(Schema.Field.of(headerTitle, Schema.of(Schema.Type.STRING)));
    }

    if (config.isExtractMetadata()) {
      generalFields.add(Schema.Field.of(METADATA_FIELD_NAME,
          Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
    }

    return Schema.recordOf(SCHEMA_ROOT_RECORD_NAME, generalFields);
  }
}
