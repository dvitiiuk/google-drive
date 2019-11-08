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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.common.RowRecord;
import io.cdap.plugin.google.sheets.sink.StructuredRecordToRowRecordTransformer;
import io.cdap.plugin.google.sheets.source.utils.HeaderSelection;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JSONComplexDataFormatterTest {
  private static final String SCHEMA_NAME = "default";
  private static final String TEST_RECORD_FIELD_NAME = "testRecord";
  private static final String TEST_RECORD_SUB_1_FIELD_NAME = "string";
  private static final String TEST_RECORD_SUB_2_FIELD_NAME = "boolean";
  private static final String TEST_RECORD_SUB_3_FIELD_NAME = "double";
  private static final String TEST_ARRAY_FIELD_NAME = "testArray";
  private static final String TEST_MAP_FIELD_NAME = "testMap";
  private static final String TEST_ENUM_FIELD_NAME = "testEnum";
  private static final String TEST_UNION_FIELD_NAME = "testUnion";

  private static final String TEST_STRING_VALUE = "fdgdfg";
  private static final Double TEST_DOUBLE_VALUE = 452.564;
  private static final Boolean TEST_BOOLEAN_VALUE = true;

  @Test
  public void testFormat() throws IOException {
    List<String> testList = Arrays.asList("firstElement", "secondElement", "thirdElement");
    Map<String, String> testMap = new HashMap<>();
    testMap.put("firstKey", "firstValue");
    testMap.put("secondKey", "secondValue");
    testMap.put("thirdKey", "thirdValue");

    StructuredRecordToRowRecordTransformer transformer = new StructuredRecordToRowRecordTransformer(
        "", "", "",
        new StructuredRecordToRowRecordTransformer.JSONComplexDataFormatter());

    Schema testSchema = Schema.recordOf(SCHEMA_NAME,
        Schema.Field.of(TEST_RECORD_FIELD_NAME, Schema.recordOf(
            TEST_RECORD_FIELD_NAME,
            Schema.Field.of(TEST_RECORD_SUB_1_FIELD_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(TEST_RECORD_SUB_2_FIELD_NAME, Schema.of(Schema.Type.BOOLEAN)),
            Schema.Field.of(TEST_RECORD_SUB_3_FIELD_NAME, Schema.of(Schema.Type.DOUBLE))
        )),
        Schema.Field.of(TEST_ARRAY_FIELD_NAME, Schema.arrayOf(
            Schema.of(Schema.Type.STRING)
        )),
        Schema.Field.of(TEST_MAP_FIELD_NAME, Schema.mapOf(
            Schema.of(Schema.Type.STRING),
            Schema.of(Schema.Type.STRING)
        )),
        Schema.Field.of(TEST_ENUM_FIELD_NAME, Schema.enumWith(
            HeaderSelection.FIRST_ROW_AS_COLUMNS.getValue(),
            HeaderSelection.NO_COLUMN_NAMES.getValue(),
            HeaderSelection.CUSTOM_ROW_AS_COLUMNS.getValue()
        ))/*,
        Schema.Field.of(TEST_UNION_FIELD_NAME, Schema.unionOf(
            Schema.of(Schema.Type.STRING),
            Schema.of(Schema.Type.BOOLEAN)
        ))*/);

    StructuredRecord.Builder nestedBuilder = StructuredRecord.builder(
        testSchema.getField(TEST_RECORD_FIELD_NAME).getSchema());
    nestedBuilder.set(TEST_RECORD_SUB_1_FIELD_NAME, TEST_STRING_VALUE);
    nestedBuilder.set(TEST_RECORD_SUB_2_FIELD_NAME, TEST_BOOLEAN_VALUE);
    nestedBuilder.set(TEST_RECORD_SUB_3_FIELD_NAME, TEST_DOUBLE_VALUE);
    StructuredRecord nestedRecord = nestedBuilder.build();

    /*StructuredRecord.Builder unionBuilder = StructuredRecord.builder(
        testSchema.getField(TEST_UNION_FIELD_NAME).getSchema());
    StructuredRecord union = unionBuilder.build();*/

    StructuredRecord.Builder builder = StructuredRecord.builder(testSchema);
    builder.set(TEST_RECORD_FIELD_NAME, nestedRecord);
    builder.set(TEST_ARRAY_FIELD_NAME, testList);
    builder.set(TEST_MAP_FIELD_NAME, testMap);
    builder.set(TEST_ENUM_FIELD_NAME, HeaderSelection.FIRST_ROW_AS_COLUMNS.getValue());
    //builder.set(TEST_UNION_FIELD_NAME, union);

    StructuredRecord record = builder.build();

    RowRecord rowRecord = transformer.transform(record);

    Assert.assertNotNull(rowRecord.getHeaderedCells());
    Assert.assertEquals(4, rowRecord.getHeaderedCells().size());

    CellData recordData = rowRecord.getHeaderedCells().get(TEST_RECORD_FIELD_NAME);
    Assert.assertNotNull(recordData);
    Assert.assertEquals("{\"string\":\"fdgdfg\",\"boolean\":true,\"double\":452.564}",
        recordData.getUserEnteredValue().getStringValue());

    CellData arrayData = rowRecord.getHeaderedCells().get(TEST_ARRAY_FIELD_NAME);
    Assert.assertNotNull(arrayData);
    Assert.assertEquals("{\"testArray\":[\"firstElement\",\"secondElement\",\"thirdElement\"]}",
        arrayData.getUserEnteredValue().getStringValue());

    CellData mapData = rowRecord.getHeaderedCells().get(TEST_MAP_FIELD_NAME);
    Assert.assertNotNull(mapData);
    Assert.assertEquals("{\"testMap\":{\"firstKey\":\"firstValue\",\"thirdKey\":\"thirdValue\"," +
            "\"secondKey\":\"secondValue\"}}",
        mapData.getUserEnteredValue().getStringValue());

    CellData enumData = rowRecord.getHeaderedCells().get(TEST_ENUM_FIELD_NAME);
    Assert.assertNotNull(enumData);
    Assert.assertEquals("{\"testEnum\":\"firstRowAsColumns\"}",
        enumData.getUserEnteredValue().getStringValue());

  }
}
