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

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.validation.DefaultFailureCollector;
import io.cdap.plugin.google.sheets.source.utils.CellCoordinate;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoogleSheetsSourceConfigTest {

  private GoogleSheetsSourceConfig config = new GoogleSheetsSourceConfig();

  @Test
  public void testMetadataInputToMap() {
    String metadataCellsInput = "A1:name1,A2:name2,A5:name3";
    Map<String, String> expectedKeyCells = new HashMap<String, String>() {{
      put("A1", "name1");
      put("A2", "name2");
      put("A5", "name3");
    }};
    Map<String, String> expectedValueCells = new HashMap<String, String>() {{
      put("name1", "A1");
      put("name2", "A2");
      put("name3", "A5");
    }};

    Assert.assertEquals(expectedKeyCells, config.metadataInputToMap(metadataCellsInput, true));
    Assert.assertEquals(expectedValueCells, config.metadataInputToMap(metadataCellsInput, false));
  }

  @Test
  public void testEmptyMetadataInputToMap() {
    Assert.assertEquals(Collections.EMPTY_MAP, config.metadataInputToMap("", true));
    Assert.assertEquals(Collections.EMPTY_MAP, config.metadataInputToMap("", false));
  }

  @Test
  public void testGetNumberOfColumn() {
    Assert.assertEquals(1, config.getNumberOfColumn("A"));
    Assert.assertEquals(26, config.getNumberOfColumn("Z"));
    Assert.assertEquals(27, config.getNumberOfColumn("AA"));
    Assert.assertEquals(46, config.getNumberOfColumn("AT"));
  }

  @Test
  public void testToCoordinate() {
    Assert.assertEquals(new CellCoordinate(1, 1), config.toCoordinate("A1"));
    Assert.assertEquals(new CellCoordinate(100, 26), config.toCoordinate("Z100"));
    Assert.assertEquals(new CellCoordinate(100, 52), config.toCoordinate("AZ100"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToCoordinateInvalidValue() {
    Assert.assertEquals(new CellCoordinate(1, 1), config.toCoordinate("1A"));
  }

  @Test
  public void testGetMetadataCoordinates() throws NoSuchFieldException, IllegalAccessException {
    setStringField("metadataKeyCells", "A1:name1,A2:name2,A5:name3");
    setStringField("metadataValueCells", "name1:B2,name2:B4,name3:B7");

    List<MetadataKeyValueAddress> metadataCoordinates = config.getMetadataCoordinates();

    List<MetadataKeyValueAddress> expectedCoordinates = new ArrayList<>();
    expectedCoordinates.add(new MetadataKeyValueAddress(new CellCoordinate(1, 1),
        new CellCoordinate(2, 2)));
    expectedCoordinates.add(new MetadataKeyValueAddress(new CellCoordinate(2, 1),
        new CellCoordinate(4, 2)));
    expectedCoordinates.add(new MetadataKeyValueAddress(new CellCoordinate(5, 1),
        new CellCoordinate(7, 2)));

    Assert.assertEquals(expectedCoordinates, metadataCoordinates);
  }

  @Test
  public void testValidateMetadataCellsOnlyHeader() throws NoSuchFieldException, IllegalAccessException {
    setStringField("firstHeaderRow", "2");
    setStringField("lastHeaderRow", "2");
    setStringField("firstFooterRow", "-1");
    setStringField("lastFooterRow", "-1");

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from row with index 2
    config.validateMetadataCells(collector, "A3:name1,Z3:name2,B3:name3",
        GoogleSheetsSourceConfig.METADATA_KEY_CELLS, false);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 1 and 3 indexes
    String beforeAddress = "A2";
    String afterAddress = "Z4";
    config.validateMetadataCells(collector, String.format("%s:name1,%s:name2,B3:name3", beforeAddress, afterAddress),
        GoogleSheetsSourceConfig.METADATA_KEY_CELLS, false);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", beforeAddress))
        || failures.get(0).getMessage().equals(
            String.format("Metadata cell '%s' is out of header or footer rows", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", beforeAddress))
        || failures.get(1).getMessage().equals(
            String.format("Metadata cell '%s' is out of header or footer rows", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  @Test
  public void testValidateMetadataCellsOnlyFooter() throws NoSuchFieldException, IllegalAccessException {
    setStringField("firstHeaderRow", "-1");
    setStringField("lastHeaderRow", "-1");
    setStringField("firstFooterRow", "5");
    setStringField("lastFooterRow", "7");

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from rows with indexes 5-7
    config.validateMetadataCells(collector, "A6:name1,Z7:name2,B8:name3",
        GoogleSheetsSourceConfig.METADATA_KEY_CELLS, false);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 4 and 8 indexes
    String beforeAddress = "B5";
    String afterAddress = "X9";
    config.validateMetadataCells(collector,
        String.format("A6:name1,Z7:name2,%s:name3,%s:name4", beforeAddress, afterAddress),
        GoogleSheetsSourceConfig.METADATA_KEY_CELLS, false);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", beforeAddress))
        || failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", beforeAddress))
        || failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  @Test
  public void testValidateMetadataCellsHeaderAndFooter() throws NoSuchFieldException, IllegalAccessException {
    setStringField("firstHeaderRow", "2");
    setStringField("lastHeaderRow", "2");
    setStringField("firstFooterRow", "5");
    setStringField("lastFooterRow", "7");

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from rows with indexes 2-2 and 5-7
    config.validateMetadataCells(collector, "D3:name0,A6:name1,Z7:name2,B8:name3",
        GoogleSheetsSourceConfig.METADATA_KEY_CELLS, false);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 4 and 8 indexes
    String beforeAddress = "B5";
    String afterAddress = "X9";
    config.validateMetadataCells(collector,
        String.format("A6:name1,Z7:name2,%s:name3,%s:name4", beforeAddress, afterAddress),
        GoogleSheetsSourceConfig.METADATA_KEY_CELLS, false);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", beforeAddress))
        || failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", beforeAddress))
        || failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  private void setStringField(String fieldName, String fieldValue) throws NoSuchFieldException, IllegalAccessException {
    Field metadataKeyCellsField = config.getClass().getDeclaredField(fieldName);
    metadataKeyCellsField.setAccessible(true);
    metadataKeyCellsField.set(config, fieldValue);
  }
}
