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

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.common.Sheet;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms {@link Sheet} wrapper to {@link StructuredRecord} instance.
 */
public class SheetTransformer {
  private static Gson gson = new Gson();

  public static StructuredRecord transform(Sheet sheet, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      switch (name) {
        case SchemaBuilder.SHEET_TITLE_FIELD_NAME:
          builder.set(SchemaBuilder.SHEET_TITLE_FIELD_NAME, sheet.getSheetTitle());
          break;
        case SchemaBuilder.SPREADSHEET_NAME_FIELD_NAME:
          builder.set(SchemaBuilder.SPREADSHEET_NAME_FIELD_NAME, sheet.getSpreadSheetName());
          break;
        case SchemaBuilder.METADATA_FIELD_NAME:
          builder.set(SchemaBuilder.METADATA_FIELD_NAME, sheet.getMetadata());
          break;
        default:
          builder.set(name, gson.toJson(sheet.getHeaderedValues().get(name)));
      }
    }
    return builder.build();
  }

  private static String toCSV(List<List<Object>> values) {
    StringBuilder sb = new StringBuilder();
    if (values != null) {
      for (List<Object> row : values) {
        List<String> encodedValues = new ArrayList<>();
        for (Object element : row) {
          if (element instanceof String) {
            encodedValues.add(String.format("\"%s\"", (String) element));
          } else if (element instanceof BigDecimal) {
            encodedValues.add(((BigDecimal) element).toString());
          } else if (element instanceof Boolean) {
            encodedValues.add(((Boolean) element).toString());
          } else {
            throw new IllegalStateException(String.format("Invalid type [%s] for cell value", element.getClass()));
          }
        }
        sb.append(String.join("#", encodedValues));
        sb.append("\r\n");
      }
    }
    return sb.toString();
  }

  private static String toJson(List<List<Object>> values) {
    return gson.toJson(values);
  }
}
