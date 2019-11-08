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
import com.google.api.services.sheets.v4.model.NumberFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.sheets.common.RowRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transforms a {@link StructuredRecord}
 * to a {@link FileFromFolder}
 */
public class StructuredRecordToRowRecordTransformer {
  public static final LocalDate SHEETS_START_DATE = LocalDate.of(1899, 12, 30);
  public static final ZoneId UTC_ZONE_ID = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
  public static final String SHEETS_CELL_DATE_TYPE = "DATE";
  public static final String SHEETS_CELL_TIME_TYPE = "TIME";
  public static final String SHEETS_CELL_DATE_TIME_TYPE = "DATE_TIME";
  public static final Integer RANDOM_FILE_NAME_LENGTH = 16;

  private final String spreadSheetNameFieldName;
  private final String sheetNameFieldName;
  private final String sheetName;
  private final ComplexDataFormatter complexDataFormatter;

  public StructuredRecordToRowRecordTransformer(String spreadSheetNameFieldName,
                                                String sheetNameFieldName,
                                                String sheetName,
                                                ComplexDataFormatter complexDataFormatter) {
    this.spreadSheetNameFieldName = spreadSheetNameFieldName;
    this.sheetNameFieldName = sheetNameFieldName;
    this.sheetName = sheetName;
    this.complexDataFormatter = complexDataFormatter;
  }

  public RowRecord transform(StructuredRecord input) throws IOException {
    Map<String, CellData> data = new HashMap<>();
    String spreadSheetName = null;
    String sheetName = null;

    Schema schema = input.getSchema();
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      if (fieldName.equals(spreadSheetNameFieldName)) {
        spreadSheetName = input.get(spreadSheetNameFieldName);
      } else if (fieldName.equals(sheetNameFieldName)) {
        sheetName = input.get(sheetNameFieldName);
      } else {
        Schema fieldSchema = field.getSchema();
        CellData cellData = new CellData();

        if (input.get(fieldName) == null) {
          data.put(fieldName, cellData);
          continue;
        }
        Schema.LogicalType fieldLogicalType = fieldSchema.isNullableSimple() ?
            fieldSchema.getNonNullable().getLogicalType() :
            fieldSchema.getLogicalType();
        if (fieldLogicalType != null) {
          cellData = processDateTimeValue(fieldLogicalType, fieldName, input);
        } else {
          cellData = processSimpleTypes(fieldSchema, fieldName, input);
        }
        data.put(fieldName, cellData);
      }
    }
    if (spreadSheetName == null) {
      spreadSheetName = generateRandomName();
    }

    if (sheetName == null) {
      sheetName = this.sheetName;
    }

    RowRecord rowRecord = new RowRecord(spreadSheetName, sheetName, null, data, false);
    return rowRecord;
  }

  CellData processDateTimeValue(Schema.LogicalType fieldLogicalType, String fieldName, StructuredRecord input) {
    CellData cellData = new CellData();
    ExtendedValue userEnteredValue = new ExtendedValue();
    CellFormat userEnteredFormat = new CellFormat();
    NumberFormat dateFormat = new NumberFormat();
    switch (fieldLogicalType) {
      case DATE:
        LocalDate date = input.getDate(fieldName);
        userEnteredValue.setNumberValue(toSheetsDate(date));

        dateFormat.setType(SHEETS_CELL_DATE_TYPE);
        break;
      case TIMESTAMP_MILLIS:
      case TIMESTAMP_MICROS:
        ZonedDateTime dateTime = input.getTimestamp(fieldName);
        userEnteredValue.setNumberValue(toSheetsDateTime(dateTime));

        dateFormat.setType(SHEETS_CELL_DATE_TIME_TYPE);
        break;
      case TIME_MILLIS:
      case TIME_MICROS:
        LocalTime time = input.getTime(fieldName);
        userEnteredValue.setNumberValue(toSheetsTime(time));

        dateFormat.setType(SHEETS_CELL_TIME_TYPE);
        break;
    }
    userEnteredFormat.setNumberFormat(dateFormat);
    cellData.setUserEnteredValue(userEnteredValue);
    cellData.setUserEnteredFormat(userEnteredFormat);
    return cellData;
  }

  CellData processSimpleTypes(Schema fieldSchema, String fieldName, StructuredRecord input) throws IOException {
    CellData cellData = new CellData();
    ExtendedValue userEnteredValue = new ExtendedValue();
    CellFormat userEnteredFormat = new CellFormat();
    Schema.Type fieldType = fieldSchema.isNullableSimple() ?
        fieldSchema.getNonNullable().getType() :
        fieldSchema.getType();
    switch (fieldType) {
      case STRING:
        userEnteredValue.setStringValue(input.get(fieldName));
        break;
      case BYTES:
        userEnteredValue.setStringValue(new String((byte[]) input.get(fieldName)));
        break;
      case BOOLEAN:
        userEnteredValue.setBoolValue(input.get(fieldName));
        break;
      case LONG:
        userEnteredValue.setNumberValue((double) (Long) input.get(fieldName));
        break;
      case INT:
        userEnteredValue.setNumberValue((double) (Integer) input.get(fieldName));
        break;
      case DOUBLE:
        userEnteredValue.setNumberValue(input.get(fieldName));
        break;
      case FLOAT:
        userEnteredValue.setNumberValue((double) (Long) input.get(fieldName));
        break;
      case NULL:
        // do nothing
        break;
      default:
        userEnteredValue = complexDataFormatter.format(fieldName, fieldSchema, fieldType, input.get(fieldName));
    }
    cellData.setUserEnteredValue(userEnteredValue);
    cellData.setUserEnteredFormat(userEnteredFormat);
    return cellData;
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

  private String generateRandomName() {
    return RandomStringUtils.randomAlphanumeric(RANDOM_FILE_NAME_LENGTH);
  }

  /**
   *
   */
  public interface ComplexDataFormatter {
    ExtendedValue format(String fieldName, Schema fieldSchema, Schema.Type fieldType, Object data) throws IOException;
  }

  /**
   *
   */
  public static class JSONComplexDataFormatter implements ComplexDataFormatter {

    @Override
    public ExtendedValue format(String fieldName, Schema fieldSchema, Schema.Type fieldType,
                                Object data) throws IOException {
      ExtendedValue result = new ExtendedValue();
      switch (fieldType) {
        case ARRAY:
        case MAP:
        case ENUM:
        case UNION:
          Schema wrapperSchema = Schema.recordOf(fieldName,
              Collections.singleton(Schema.Field.of(fieldName, fieldSchema)));
          StructuredRecord.Builder builder = StructuredRecord.builder(wrapperSchema);
          builder.set(fieldName, data);
          result.setStringValue(StructuredRecordStringConverter.toJsonString(builder.build()));
          break;
        case RECORD:
          result.setStringValue(StructuredRecordStringConverter.toJsonString((StructuredRecord) data));
          break;
        default:
          throw new IllegalStateException(String.format("'%s' data type is not supported", fieldType));
      }
      return result;
    }
  }

  /**
   *
   */
  public static class CSVComplexDataFormatter implements ComplexDataFormatter {

    @Override
    public ExtendedValue format(String fieldName, Schema fieldSchema, Schema.Type fieldType,
                                Object data) throws IOException {
      ExtendedValue result = new ExtendedValue();
      switch (fieldType) {
        case ARRAY:
          CSVFormat arrayFormat = CSVFormat.newFormat(',').withQuote('"')
            .withRecordSeparator("\r\n");
          CSVPrinter arrayPrinter = new CSVPrinter(new StringWriter(), arrayFormat);
          arrayPrinter.printRecord((List) data);
          result.setStringValue(arrayPrinter.getOut().toString());
          break;
        case MAP:
          Map<String, Object> mapData = (Map<String, Object>) data;
          CSVFormat mapFormat = CSVFormat.newFormat(',').withQuote('"')
              .withRecordSeparator("\r\n").withHeader(mapData.keySet().toArray(new String[]{}));
          CSVPrinter mapPrinter = new CSVPrinter(new StringWriter(), mapFormat);
          mapPrinter.printRecord(mapData.values());
          result.setStringValue(mapPrinter.getOut().toString());
          break;
        case ENUM:
        case UNION:
        case RECORD:
          StructuredRecord recordData = (StructuredRecord) data;
          CSVFormat recordFormat = CSVFormat.newFormat(',').withQuote('"')
              .withRecordSeparator("\r\n").withHeader(recordData.getSchema().getFields().stream()
                  .map(f -> f.getName()).collect(Collectors.toList()).toArray(new String[]{}));
          CSVPrinter recordPrinter = new CSVPrinter(new StringWriter(), recordFormat);
          recordPrinter.printRecord(recordData.getSchema().getFields().stream()
              .map(f -> recordData.get(f.getName())).collect(Collectors.toList()));
          result.setStringValue(recordPrinter.getOut().toString());
          break;
        default:
          throw new IllegalStateException(String.format("'%s' data type is not supported", fieldType));
      }
      return result;
    }
  }
}
