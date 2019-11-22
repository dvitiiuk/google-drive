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

import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.source.utils.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

/**
 * Transforms {@link RowRecord} wrapper to {@link StructuredRecord} instance.
 */
public class SheetTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(SheetTransformer.class);

  private static final LocalDate SHEETS_START_DATE = LocalDate.of(1899, 12, 30);
  private static final ZonedDateTime SHEETS_START_DATE_TIME =
      ZonedDateTime.of(1899, 12, 30, 0, 0, 0, 0, ZoneId.ofOffset("UTC", ZoneOffset.UTC));

  public static StructuredRecord transform(RowRecord rowRecord, Schema schema, boolean extractMetadata,
                                           String metadataRecordName) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      if (name.equals(SchemaBuilder.SHEET_TITLE_FIELD_NAME)) {
        builder.set(SchemaBuilder.SHEET_TITLE_FIELD_NAME, rowRecord.getSheetTitle());
      } else if (name.equals(SchemaBuilder.SPREADSHEET_NAME_FIELD_NAME)) {
        builder.set(SchemaBuilder.SPREADSHEET_NAME_FIELD_NAME, rowRecord.getSpreadSheetName());
      } else if (extractMetadata && name.equals(metadataRecordName)) {
        builder.set(metadataRecordName, rowRecord.getMetadata());
      } else {
        CellData cellData = rowRecord.getHeaderedCells().get(name);
        if (cellData == null) {
          builder.set(name, null);
        } else {
          processCellData(builder, field, cellData);
        }
      }
    }
    return builder.build();
  }

  private static void processCellData(StructuredRecord.Builder builder, Schema.Field field, CellData cellData) {
    String fieldName = field.getName();
    Schema fieldSchema = field.getSchema();
    Schema.LogicalType fieldLogicalType = fieldSchema.getNonNullable().getLogicalType();
    Schema.Type fieldType = fieldSchema.getNonNullable().getType();

    if (Schema.LogicalType.DATE.equals(fieldLogicalType)) {
      ExtendedValue userEnteredValue = cellData.getUserEnteredValue();
      if (userEnteredValue != null) {
        builder.setDate(fieldName, getDateValue(userEnteredValue, fieldName));
      }

    } else if (Schema.LogicalType.TIMESTAMP_MILLIS.equals(fieldLogicalType)) {
      ExtendedValue userEnteredValue = cellData.getUserEnteredValue();
      if (userEnteredValue != null) {
        builder.setTimestamp(fieldName, getTimeStampValue(userEnteredValue, fieldName));
      }

    } else if (Schema.Type.LONG.equals(fieldType)) {
      ExtendedValue userEnteredValue = cellData.getUserEnteredValue();
      if (userEnteredValue != null) {
        builder.set(fieldName, getIntervalValue(userEnteredValue, fieldName));
      }

    } else if (Schema.Type.BOOLEAN.equals(fieldType)) {
      ExtendedValue effectiveValue = cellData.getEffectiveValue();
      if (effectiveValue != null) {
        builder.set(fieldName, effectiveValue.getBoolValue());
      }

    } else if (Schema.Type.STRING.equals(fieldType)) {
      builder.set(fieldName, cellData.getFormattedValue());

    } else if (Schema.Type.DOUBLE.equals(fieldType)) {
      ExtendedValue effectiveValue = cellData.getEffectiveValue();
      if (effectiveValue != null) {
        builder.set(fieldName, effectiveValue.getNumberValue());
      }
    }
  }

  private static LocalDate getDateValue(ExtendedValue userEnteredValue, String fieldName) {
    Double dataValue = userEnteredValue.getNumberValue();
    if (dataValue == null) {
      LOG.warn(String.format("Field '%s' has no DATE value, '%s' instead", fieldName, userEnteredValue.toString()));
      return null;
    }
    return SHEETS_START_DATE.plusDays(dataValue.intValue());
  }

  private static ZonedDateTime getTimeStampValue(ExtendedValue userEnteredValue, String fieldName) {
    Double dataValue = userEnteredValue.getNumberValue();
    if (dataValue == null) {
      LOG.warn(String.format("Field '%s' has no DATE value, '%s' instead", fieldName, userEnteredValue.toString()));
      return null;
    }
    long dayMicros = ChronoField.MICRO_OF_DAY.range().getMaximum();
    return SHEETS_START_DATE_TIME.plus((long) (dataValue * dayMicros), ChronoUnit.MICROS);
  }

  private static Long getIntervalValue(ExtendedValue userEnteredValue, String fieldName) {
    Double dataValue = userEnteredValue.getNumberValue();
    if (dataValue == null) {
      LOG.warn(String.format("Field '%s' has no DATE value, '%s' instead", fieldName, userEnteredValue.toString()));
      return null;
    }
    long dayMicros = ChronoField.MICRO_OF_DAY.range().getMaximum();
    return (long) (dataValue * dayMicros / 1000);
  }
}
