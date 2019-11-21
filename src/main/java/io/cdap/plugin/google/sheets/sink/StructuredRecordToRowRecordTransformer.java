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

import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.CellFormat;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.NumberFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;
import io.cdap.plugin.google.sheets.sink.utils.FlatternedRowsRecord;
import org.apache.commons.collections.CollectionUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Transforms a {@link StructuredRecord} to a {@link FlatternedRowsRecord}.
 */
public class StructuredRecordToRowRecordTransformer {
  public static final LocalDate SHEETS_START_DATE = LocalDate.of(1899, 12, 30);
  public static final ZoneId UTC_ZONE_ID = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
  public static final String SHEETS_CELL_DATE_TYPE = "DATE";
  public static final String SHEETS_CELL_TIME_TYPE = "TIME";
  public static final String SHEETS_CELL_DATE_TIME_TYPE = "DATE_TIME";

  private final String spreadSheetNameFieldName;
  private final String sheetNameFieldName;
  private final String spreadSheetName;
  private final String sheetName;

  public StructuredRecordToRowRecordTransformer(String spreadSheetNameFieldName,
                                                String sheetNameFieldName,
                                                String spreadSheetName,
                                                String sheetName) {
    this.spreadSheetNameFieldName = spreadSheetNameFieldName;
    this.sheetNameFieldName = sheetNameFieldName;
    this.spreadSheetName = spreadSheetName;
    this.sheetName = sheetName;
  }

  public FlatternedRowsRecord transform(StructuredRecord input) {
    List<List<CellData>> data = new ArrayList<>();
    List<GridRange> mergeRanges = new ArrayList<>();
    ComplexHeader header = new ComplexHeader(null);
    String spreadSheetName = null;
    String sheetName = null;

    data.add(new ArrayList<>());
    Schema schema = input.getSchema();
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      if (fieldName.equals(spreadSheetNameFieldName)) {
        spreadSheetName = input.get(spreadSheetNameFieldName);
      } else if (fieldName.equals(sheetNameFieldName)) {
        sheetName = input.get(sheetNameFieldName);
      }
      processField(field, input, data, header, mergeRanges, true);
    }
    if (spreadSheetName == null) {
      spreadSheetName = this.spreadSheetName;
    }

    if (sheetName == null) {
      sheetName = this.sheetName;
    }

    FlatternedRowsRecord flatternedRowsRecord = new FlatternedRowsRecord(spreadSheetName, sheetName, header,
        data, mergeRanges);
    return flatternedRowsRecord;
  }

  private void processField(Schema.Field field, StructuredRecord input, List<List<CellData>> data,
                            ComplexHeader header, List<GridRange> mergeRanges, boolean isComplexTypeSupported) {
    String fieldName = field.getName();
    Schema fieldSchema = field.getSchema();
    CellData cellData;
    ComplexHeader subHeader = new ComplexHeader(fieldName);

    Schema.LogicalType fieldLogicalType = getFieldLogicalType(fieldSchema);
    Schema.Type fieldType = getFieldType(fieldSchema);

    Object value = input == null ? null : input.get(fieldName);

    if (fieldLogicalType != null) {
      cellData = processDateTimeValue(fieldLogicalType, value);
      addDataValue(cellData, data, mergeRanges);
    } else if (isSimpleType(fieldType)) {
      cellData = processSimpleTypes(fieldType, value);
      addDataValue(cellData, data, mergeRanges);
    } else if (isComplexType(fieldType)) {
      if (isComplexTypeSupported) {
        processComplexTypes(fieldType, fieldName, input, data, subHeader, mergeRanges);
      } else {
        throw new IllegalStateException("Nested arrays/records are not supported.");
      }
    } else {
      throw new IllegalStateException(String.format("Data type '%s' is not supported.", fieldType));
    }
    header.addHeader(subHeader);
  }

  void addDataValue(CellData cellData, List<List<CellData>> data, List<GridRange> mergeRanges) {
    data.forEach(r -> r.add(cellData));
    mergeRanges.add(new GridRange().setStartRowIndex(0).setEndRowIndex(data.size())
        .setStartColumnIndex(data.get(0).size() - 1).setEndColumnIndex(data.get(0).size()));
  }

  CellData processDateTimeValue(Schema.LogicalType fieldLogicalType, Object value) {
    CellData cellData = new CellData();
    ExtendedValue userEnteredValue = new ExtendedValue();
    CellFormat userEnteredFormat = new CellFormat();
    NumberFormat dateFormat = new NumberFormat();
    switch (fieldLogicalType) {
      case DATE:
        if (value != null) {
          LocalDate date = LocalDate.ofEpochDay((Integer) value);
          userEnteredValue.setNumberValue(toSheetsDate(date));
        }
        dateFormat.setType(SHEETS_CELL_DATE_TYPE);
        break;
      case TIMESTAMP_MILLIS:
        if (value != null) {
          ZonedDateTime dateTime = getZonedDateTime((long) value, TimeUnit.MILLISECONDS,
            ZoneId.ofOffset("UTC", ZoneOffset.UTC));
          userEnteredValue.setNumberValue(toSheetsDateTime(dateTime));
        }
        dateFormat.setType(SHEETS_CELL_DATE_TIME_TYPE);
        break;
      case TIMESTAMP_MICROS:
        if (value != null) {
          ZonedDateTime dateTime = getZonedDateTime((long) value, TimeUnit.MICROSECONDS,
            ZoneId.ofOffset("UTC", ZoneOffset.UTC));
          userEnteredValue.setNumberValue(toSheetsDateTime(dateTime));
        }
        dateFormat.setType(SHEETS_CELL_DATE_TIME_TYPE);
        break;
      case TIME_MILLIS:
        if (value != null) {
          LocalTime time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos((Integer) value));
          userEnteredValue.setNumberValue(toSheetsTime(time));
        }
        dateFormat.setType(SHEETS_CELL_TIME_TYPE);
        break;
      case TIME_MICROS:
        if (value != null) {
          LocalTime time = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) value));
          userEnteredValue.setNumberValue(toSheetsTime(time));
        }
        dateFormat.setType(SHEETS_CELL_TIME_TYPE);
        break;
    }
    userEnteredFormat.setNumberFormat(dateFormat);
    cellData.setUserEnteredValue(userEnteredValue);
    cellData.setUserEnteredFormat(userEnteredFormat);
    return cellData;
  }

  CellData processSimpleTypes(Schema.Type fieldType, Object value) {
    if (value == null) {
      return new CellData();
    }
    CellData cellData = new CellData();
    ExtendedValue userEnteredValue = new ExtendedValue();
    CellFormat userEnteredFormat = new CellFormat();

    switch (fieldType) {
      case STRING:
        userEnteredValue.setStringValue((String) value);
        break;
      case BYTES:
        userEnteredValue.setStringValue(new String((byte[]) value));
        break;
      case BOOLEAN:
        userEnteredValue.setBoolValue((Boolean) value);
        break;
      case LONG:
        userEnteredValue.setNumberValue((double) (Long) value);
        break;
      case INT:
        userEnteredValue.setNumberValue((double) (Integer) value);
        break;
      case DOUBLE:
        userEnteredValue.setNumberValue((Double) value);
        break;
      case FLOAT:
        userEnteredValue.setNumberValue((double) (Long) value);
        break;
      case NULL:
        // do nothing
        break;
    }
    cellData.setUserEnteredValue(userEnteredValue);
    cellData.setUserEnteredFormat(userEnteredFormat);
    return cellData;
  }

  void processComplexTypes(Schema.Type fieldType, String fieldName, StructuredRecord input,
                                   List<List<CellData>> data, ComplexHeader header, List<GridRange> mergeRanges) {
    switch(fieldType) {
      case ARRAY:
        List<Object> arrayData = input.get(fieldName);
        if (CollectionUtils.isEmpty(arrayData)) {
          arrayData = Collections.singletonList(null);
        }
        List<CellData>[] extendedData = new ArrayList[data.size() * arrayData.size()];

        Schema componentFieldSchema = getNonNullableSchema(input.getSchema().getField(fieldName).getSchema())
          .getComponentSchema();
        Schema.LogicalType componentFieldLogicalType = getFieldLogicalType(componentFieldSchema);
        Schema.Type componentFieldType = getFieldType(componentFieldSchema);

        // update merges
        for (GridRange range : mergeRanges) {
          Integer newStartRowIndex = range.getStartRowIndex() * arrayData.size();
          Integer newEndRowIndex = newStartRowIndex +
              (range.getEndRowIndex() - range.getStartRowIndex()) * arrayData.size();
          range.setStartRowIndex(newStartRowIndex).setEndRowIndex(newEndRowIndex);
        }

        // flattern the array
        for (int i = 0; i < arrayData.size(); i++) {
          CellData nestedData;
          if (componentFieldLogicalType != null) {
            nestedData = processDateTimeValue(componentFieldLogicalType, arrayData.get(i));
          } else if (isSimpleType(componentFieldType)) {
            nestedData = processSimpleTypes(componentFieldType, arrayData.get(i));
          } else {
            throw new IllegalStateException("Nested complex data formats are not supported.");
          }
          for (int j = 0; j < data.size(); j++) {
            List<CellData> flattenRow = copyRow(data.get(j));
            flattenRow.add(nestedData);
            mergeRanges.add(new GridRange().setStartRowIndex(i + arrayData.size() * j)
                .setEndRowIndex(i + arrayData.size() * j + 1)
                .setStartColumnIndex(flattenRow.size() - 1)
                .setEndColumnIndex(flattenRow.size()));
            extendedData[i + arrayData.size() * j] = flattenRow;
          }
        }
        data.clear();
        data.addAll(Arrays.asList(extendedData));
        break;
      case RECORD:
        StructuredRecord nestedRecord = input.get(fieldName);
        Schema schema = getNonNullableSchema(input.getSchema().getField(fieldName).getSchema());
        for (Schema.Field field : schema.getFields()) {
          processField(field, nestedRecord, data, header, mergeRanges, false);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Complex data format '%s' is not supported.",
          fieldType.toString()));
    }
  }

  Schema.LogicalType getFieldLogicalType(Schema fieldSchema) {
    return fieldSchema.isNullable() ?
      fieldSchema.getNonNullable().getLogicalType() :
      fieldSchema.getLogicalType();
  }

  Schema.Type getFieldType(Schema fieldSchema) {
    return fieldSchema.isNullable() ?
      fieldSchema.getNonNullable().getType() :
      fieldSchema.getType();
  }

  Schema getNonNullableSchema(Schema fieldSchema) {
    return fieldSchema.isNullable() ?
      fieldSchema.getNonNullable() :
      fieldSchema;
  }

  List<CellData> copyRow(List<CellData> row) {
    List<CellData> copiedRow = new ArrayList<>();
    copiedRow.addAll(row);
    return copiedRow;
  }


  boolean isSimpleType(Schema.Type fieldType) {
    return Arrays.asList(Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.BOOLEAN, Schema.Type.LONG,
        Schema.Type.INT, Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.NULL).contains(fieldType);
  }

  boolean isComplexType(Schema.Type fieldType) {
    return fieldType.equals(Schema.Type.ARRAY) || fieldType.equals(Schema.Type.RECORD);
  }

  ZonedDateTime getZonedDateTime(long ts, TimeUnit unit, ZoneId zoneId) {
    long mod = unit.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (ts % mod);
    long tsInSeconds = unit.toSeconds(ts);
    Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, zoneId);
  }

  Double toSheetsDate(LocalDate date) {
    return (double) ChronoUnit.DAYS.between(SHEETS_START_DATE, date);
  }

  Double toSheetsDateTime(ZonedDateTime dateTime) {
    ZonedDateTime startOfTheDay = dateTime.toLocalDate().atStartOfDay(UTC_ZONE_ID);
    long daysNumber = ChronoUnit.DAYS.between(SHEETS_START_DATE, dateTime);
    long micros = ChronoUnit.MICROS.between(startOfTheDay, dateTime);
    return (double) daysNumber + (double) micros / (double) ChronoField.MICRO_OF_DAY.range().getMaximum();
  }

  Double toSheetsTime(LocalTime localTime) {
    long micros = localTime.getLong(ChronoField.MICRO_OF_DAY);
    return (double) micros / (double) ChronoField.MICRO_OF_DAY.range().getMaximum();
  }
}
