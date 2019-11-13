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
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.sheets.common.MultipleRowsRecord;
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

  public MultipleRowsRecord transform(StructuredRecord input) {
    List<List<CellData>> data = new ArrayList<>();
    //List<String> headers = new ArrayList<>();
    List<GridRange> mergeRanges = new ArrayList<>();
    ComplexHeader complexHeader = new ComplexHeader(null);
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
      processField(field, input, data, headers, mergeRanges, true);
    }
    if (spreadSheetName == null) {
      spreadSheetName = generateRandomName();
    }

    if (sheetName == null) {
      sheetName = this.sheetName;
    }

    MultipleRowsRecord multipleRowsRecord = new MultipleRowsRecord(spreadSheetName, sheetName, headers,
        data, mergeRanges);
    return multipleRowsRecord;
  }

  private void processField(Schema.Field field, StructuredRecord input, List<List<CellData>> data,
                            ComplexHeader complexHeader, List<GridRange> mergeRanges, boolean isComplexTypeSupported) {
    String fieldName = field.getName();
    Schema fieldSchema = field.getSchema();
    CellData cellData = new CellData();

    if (input.get(fieldName) == null) {
      addDataValue(cellData, data, mergeRanges);
      headers.add(fieldName);
      return;
    }
    Schema.LogicalType fieldLogicalType = getFieldLogicalType(fieldSchema);
    Schema.Type fieldType = getFieldType(fieldSchema);
    if (fieldLogicalType != null) {
      cellData = processDateTimeValue(fieldLogicalType, fieldName, input);
      addDataValue(cellData, data, mergeRanges);
    } else if (isSimpleType(fieldType)) {
      cellData = processSimpleTypes(fieldType, input.get(fieldName));
      addDataValue(cellData, data, mergeRanges);
    } else if (isComplexType(fieldType)) {
      if (isComplexTypeSupported) {
        processComplexTypes(fieldType, fieldName, input, data, headers, mergeRanges);
      } else {
        throw new IllegalStateException("Nested arrays/records are not supported");
      }
    } else {
      throw new IllegalStateException(String.format("Data type '%s' is not supported", fieldType));
    }
    headers.add(fieldName);
  }

  void addDataValue(CellData cellData, List<List<CellData>> data, List<GridRange> mergeRanges) {
    data.forEach(r -> r.add(cellData));
    mergeRanges.add(new GridRange().setStartRowIndex(0).setEndRowIndex(data.size())
        .setStartColumnIndex(data.get(0).size() - 1).setEndColumnIndex(data.get(0).size()));
  }

  Schema.LogicalType getFieldLogicalType(Schema fieldSchema) {
    return fieldSchema.isNullableSimple() ?
        fieldSchema.getNonNullable().getLogicalType() :
        fieldSchema.getLogicalType();
  }

  Schema.Type getFieldType(Schema fieldSchema) {
    return fieldSchema.isNullableSimple() ?
        fieldSchema.getNonNullable().getType() :
        fieldSchema.getType();
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

  CellData processSimpleTypes(Schema.Type fieldType, Object value) {
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

  private void processComplexTypes(Schema.Type fieldType, String fieldName, StructuredRecord input,
                                   List<List<CellData>> data, List<String> headers, List<GridRange> mergeRanges) {
    switch(fieldType) {
      case ARRAY:
        List<Object> arrayData = input.get(fieldName);
        List<CellData>[] extendedData = new ArrayList[data.size() * arrayData.size()];

        Schema componentFieldSchema = input.getSchema().getField(fieldName).getSchema().getComponentSchema();
        Schema.LogicalType componentFieldLogicalType = getFieldLogicalType(componentFieldSchema);
        Schema.Type componentFieldType = getFieldType(componentFieldSchema);

        for (GridRange range : mergeRanges) {
          Integer newStartRowIndex = range.getStartRowIndex() * arrayData.size();
          Integer newEndRowIndex = newStartRowIndex +
              (range.getEndRowIndex() - range.getStartRowIndex()) * arrayData.size();
          range.setStartRowIndex(newStartRowIndex).setEndRowIndex(newEndRowIndex);
        }
        for (int i = 0; i < arrayData.size(); i++) {
          CellData nestedData;
          if (componentFieldLogicalType != null) {
            nestedData = processDateTimeValue(componentFieldLogicalType, fieldName, input);
          } else if (isSimpleType(componentFieldType)) {
            nestedData = processSimpleTypes(componentFieldType, arrayData.get(i));
          } else {
            throw new IllegalStateException();
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
        Schema schema = nestedRecord.getSchema();
        for (Schema.Field field : schema.getFields()) {
          processField(field, nestedRecord, data, headers, mergeRanges, false);
        }
        break;
    }
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
