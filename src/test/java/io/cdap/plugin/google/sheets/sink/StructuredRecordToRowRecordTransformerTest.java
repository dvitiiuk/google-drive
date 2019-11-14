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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

public class StructuredRecordToRowRecordTransformerTest {
  private static final String SCHEMA_NAME = "default";
  private static final String DATE_FIELD_NAME = "date";
  private static final String TIME_FIELD_NAME = "time";
  private static final String DATE_TIME_FIELD_NAME = "date_time";
  private static final String SPREADSHEET_NAME_FIELD_NAME = "spreadsheet";
  private static final String STRING_FIELD_NAME = "testString";
  private static final String SHEET_TITLE_FIELD_NAME = "sheet";

  private static final LocalDate TEST_DATE = LocalDate.of(2019, 03, 14);
  private static final LocalTime TEST_TIME = LocalTime.of(13, 03, 14);
  private static final ZonedDateTime TEST_DATE_TIME = ZonedDateTime.of(TEST_DATE, TEST_TIME,
      StructuredRecordToRowRecordTransformer.UTC_ZONE_ID);
  private static final String SPREADSHEET_NAME = "spName";
  private static final String SHEET_TITLE = "title";
  private static final String PRESET_SHEET_TITLE = "generalTitle";
  private static final String STRING_VALUE = "any string";

  @Test
  public void testToSheetsDate() {
    LocalDate testDate = LocalDate.of(2020, 01, 11);
    StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer("", "", "");
    Double expected = transformer.toSheetsDate(testDate);
    Assert.assertEquals(Double.valueOf(43841.0), expected);
  }

  @Test
  public void testToSheetsDateTime() {
    ZonedDateTime testZonedDateTime = ZonedDateTime.of(1991, 03, 8, 13, 54, 20, 0,
        StructuredRecordToRowRecordTransformer.UTC_ZONE_ID);
    StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer("", "", "");
    Double expected = transformer.toSheetsDateTime(testZonedDateTime);
    Assert.assertEquals(Double.valueOf(33305.57939814815), expected, Math.pow(10, -11));
  }

  @Test
  public void testToSheetsTime() {
    ZonedDateTime testZonedDateTime = ZonedDateTime.of(1991, 03, 8, 13, 54, 20, 0,
        StructuredRecordToRowRecordTransformer.UTC_ZONE_ID);
    StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer("", "", "");
    Double expected = transformer.toSheetsDateTime(testZonedDateTime);
    Assert.assertEquals(Double.valueOf(33305.57939814815), expected, Math.pow(10, -11));
  }

  @Test
  public void testProcessDateTimeDateValue() {
    StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer("", "", "");

    Schema dataSchema = Schema.recordOf(SCHEMA_NAME,
        Schema.Field.of(DATE_FIELD_NAME, Schema.of(Schema.LogicalType.DATE)));
    StructuredRecord.Builder builder = StructuredRecord.builder(dataSchema);
    builder.setDate(DATE_FIELD_NAME, TEST_DATE);
    StructuredRecord dateRecord = builder.build();

    CellData resultCell = transformer.processDateTimeValue(
        dataSchema.getField(DATE_FIELD_NAME).getSchema().getLogicalType(),
        DATE_FIELD_NAME,
        dateRecord);
    Assert.assertNotNull(resultCell.getUserEnteredFormat());
    Assert.assertNotNull(resultCell.getUserEnteredValue());
    Assert.assertNotNull(resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertNotNull(resultCell.getUserEnteredFormat().getNumberFormat());

    Assert.assertEquals(transformer.toSheetsDate(TEST_DATE),
        resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertEquals(StructuredRecordToRowRecordTransformer.SHEETS_CELL_DATE_TYPE,
        resultCell.getUserEnteredFormat().getNumberFormat().getType());
  }

  @Test
  public void testProcessDateTimeTimeValue() {
    StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer("", "", "");

    Schema timeSchema = Schema.recordOf(SCHEMA_NAME,
        Schema.Field.of(TIME_FIELD_NAME, Schema.of(Schema.LogicalType.TIME_MILLIS)));
    StructuredRecord.Builder builder = StructuredRecord.builder(timeSchema);
    builder.setTime(TIME_FIELD_NAME, TEST_TIME);
    StructuredRecord dateRecord = builder.build();

    CellData resultCell = transformer.processDateTimeValue(
        timeSchema.getField(TIME_FIELD_NAME).getSchema().getLogicalType(),
        TIME_FIELD_NAME,
        dateRecord);
    Assert.assertNotNull(resultCell.getUserEnteredFormat());
    Assert.assertNotNull(resultCell.getUserEnteredValue());
    Assert.assertNotNull(resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertNotNull(resultCell.getUserEnteredFormat().getNumberFormat());

    Assert.assertEquals(transformer.toSheetsTime(TEST_TIME),
        resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertEquals(StructuredRecordToRowRecordTransformer.SHEETS_CELL_TIME_TYPE,
        resultCell.getUserEnteredFormat().getNumberFormat().getType());
  }

  @Test
  public void testProcessDateTimeDateTimeValue() {
    StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer(
        "", "", "");

    Schema dateTimeSchema = Schema.recordOf(SCHEMA_NAME,
        Schema.Field.of(DATE_TIME_FIELD_NAME, Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)));
    StructuredRecord.Builder builder = StructuredRecord.builder(dateTimeSchema);
    builder.setTimestamp(DATE_TIME_FIELD_NAME, TEST_DATE_TIME);
    StructuredRecord dateRecord = builder.build();

    CellData resultCell = transformer.processDateTimeValue(
        dateTimeSchema.getField(DATE_TIME_FIELD_NAME).getSchema().getLogicalType(),
        DATE_TIME_FIELD_NAME,
        dateRecord);
    Assert.assertNotNull(resultCell.getUserEnteredFormat());
    Assert.assertNotNull(resultCell.getUserEnteredValue());
    Assert.assertNotNull(resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertNotNull(resultCell.getUserEnteredFormat().getNumberFormat());

    Assert.assertEquals(transformer.toSheetsDateTime(TEST_DATE_TIME),
        resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertEquals(StructuredRecordToRowRecordTransformer.SHEETS_CELL_DATE_TIME_TYPE,
        resultCell.getUserEnteredFormat().getNumberFormat().getType());
  }

  @Test
  public void testTransformWithSpreadsheetAndSheetNames() throws IOException {
    /*StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer(
        SPREADSHEET_NAME_FIELD_NAME,
        SHEET_TITLE_FIELD_NAME,
        PRESET_SHEET_TITLE,
        new StructuredRecordToRowRecordTransformer.JSONComplexDataFormatter());
    StructuredRecord testRecord = getTestTransformRecord();

    RowRecord result = transformer.transform(testRecord);

    Assert.assertEquals(SPREADSHEET_NAME, result.getSpreadSheetName());
    Assert.assertEquals(SHEET_TITLE, result.getSheetTitle());
    Assert.assertNull(result.getMetadata());
    Assert.assertEquals(1, result.getHeaderedCells().size());
    Assert.assertNotNull(result.getHeaderedCells().get(STRING_FIELD_NAME));
    Assert.assertEquals(STRING_VALUE, result.getHeaderedCells().get(STRING_FIELD_NAME)
        .getUserEnteredValue().getStringValue());*/
  }

  @Test
  public void testTransformWithDefaultSheetName() throws IOException {
    /*StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer(
        "",
        "",
        PRESET_SHEET_TITLE,
        new StructuredRecordToRowRecordTransformer.JSONComplexDataFormatter());
    StructuredRecord testRecord = getTestTransformRecord();

    RowRecord result = transformer.transform(testRecord);

    Assert.assertEquals(StructuredRecordToRowRecordTransformer.RANDOM_FILE_NAME_LENGTH,
        (Integer) result.getSpreadSheetName().length());
    Assert.assertEquals(PRESET_SHEET_TITLE, result.getSheetTitle());
    Assert.assertNull(result.getMetadata());
    Assert.assertEquals(3, result.getHeaderedCells().size());

    Assert.assertNotNull(result.getHeaderedCells().get(SPREADSHEET_NAME_FIELD_NAME));
    Assert.assertEquals(SPREADSHEET_NAME, result.getHeaderedCells().get(SPREADSHEET_NAME_FIELD_NAME)
        .getUserEnteredValue().getStringValue());

    Assert.assertNotNull(result.getHeaderedCells().get(SHEET_TITLE_FIELD_NAME));
    Assert.assertEquals(SHEET_TITLE, result.getHeaderedCells().get(SHEET_TITLE_FIELD_NAME)
        .getUserEnteredValue().getStringValue());

    Assert.assertNotNull(result.getHeaderedCells().get(STRING_FIELD_NAME));
    Assert.assertEquals(STRING_VALUE, result.getHeaderedCells().get(STRING_FIELD_NAME)
        .getUserEnteredValue().getStringValue());*/
  }

  @Test
  public void testTransformWithSpreadsheetAndDefaultSheetNames() throws IOException {
    /*StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer(
        SPREADSHEET_NAME_FIELD_NAME,
        "",
        PRESET_SHEET_TITLE,
        new StructuredRecordToRowRecordTransformer.JSONComplexDataFormatter());
    StructuredRecord testRecord = getTestTransformRecord();

    RowRecord result = transformer.transform(testRecord);

    Assert.assertEquals(SPREADSHEET_NAME, result.getSpreadSheetName());
    Assert.assertEquals(PRESET_SHEET_TITLE, result.getSheetTitle());
    Assert.assertNull(result.getMetadata());
    Assert.assertEquals(2, result.getHeaderedCells().size());

    Assert.assertNotNull(result.getHeaderedCells().get(SHEET_TITLE_FIELD_NAME));
    Assert.assertEquals(SHEET_TITLE, result.getHeaderedCells().get(SHEET_TITLE_FIELD_NAME)
        .getUserEnteredValue().getStringValue());

    Assert.assertNotNull(result.getHeaderedCells().get(STRING_FIELD_NAME));
    Assert.assertEquals(STRING_VALUE, result.getHeaderedCells().get(STRING_FIELD_NAME)
        .getUserEnteredValue().getStringValue());*/
  }



  private StructuredRecord getTestTransformRecord() {
    Schema testSchema = Schema.recordOf(SCHEMA_NAME,
        Schema.Field.of(SPREADSHEET_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(SHEET_TITLE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(STRING_FIELD_NAME, Schema.of(Schema.Type.STRING)));

    StructuredRecord.Builder builder = StructuredRecord.builder(testSchema);
    builder.set(SPREADSHEET_NAME_FIELD_NAME, SPREADSHEET_NAME);
    builder.set(SHEET_TITLE_FIELD_NAME, SHEET_TITLE);
    builder.set(STRING_FIELD_NAME, STRING_VALUE);
    return builder.build();
  }


}
