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
    String metadataCellsInput = "A1:B1,A2:B2,A5:B3";
    Map<String, String> expectedKeyValues = new HashMap<String, String>() {{
      put("A1", "B1");
      put("A2", "B2");
      put("A5", "B3");
    }};

    Assert.assertEquals(expectedKeyValues, config.metadataInputToMap(metadataCellsInput));
  }

  @Test
  public void testEmptyMetadataInputToMap() {
    Assert.assertEquals(Collections.EMPTY_MAP, config.metadataInputToMap(""));
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
    setStringField("metadataCells", "A1:B2,A2:B4,A5:B7");
    setBooleanField("extractMetadata", true);

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
    setStringField("metadataCells", "A3:C3,Z3:AA3,B3:YU3");
    setStringField("firstHeaderRow", "3");
    setStringField("lastHeaderRow", "3");
    setStringField("firstFooterRow", "-1");
    setStringField("lastFooterRow", "-1");

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from row with index 3
    config.validateMetadataCells(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 2 and 4 indexes
    String beforeAddress = "A2";
    String afterAddress = "Z4";
    setStringField("metadataCells", String.format("%s:C3,Z3:%s,B3:YU3", beforeAddress, afterAddress));
    config.validateMetadataCells(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(0).getMessage().equals(
            String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(1).getMessage().equals(
            String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  @Test
  public void testValidateMetadataCellsOnlyFooter() throws NoSuchFieldException, IllegalAccessException {
    setStringField("metadataCells", "A6:B7,Z7:U6,B8:A8");
    setStringField("firstHeaderRow", "-1");
    setStringField("lastHeaderRow", "-1");
    setStringField("firstFooterRow", "6");
    setStringField("lastFooterRow", "8");

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from rows with indexes 6-8
    config.validateMetadataCells(collector);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 5 and 9 indexes
    String beforeAddress = "B5";
    String afterAddress = "X9";
    setStringField("metadataCells",
        String.format("A6:B7,Z7:U6,%s:A8,B8:%s", beforeAddress, afterAddress));
    config.validateMetadataCells(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  @Test
  public void testValidateMetadataCellsHeaderAndFooter() throws NoSuchFieldException, IllegalAccessException {
    setStringField("metadataCells", "D3:A3,A6:B6,Z7:F3,B8:V6");
    setStringField("firstHeaderRow", "3");
    setStringField("lastHeaderRow", "3");
    setStringField("firstFooterRow", "6");
    setStringField("lastFooterRow", "8");

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from rows with indexes 3-3 and 6-8
    config.validateMetadataCells(collector);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 5 and 9 indexes
    String beforeAddress = "B5";
    String afterAddress = "X9";
    setStringField("metadataCells",
        String.format("A6:A3,Z7:B8,%s:C8,B8:%s", beforeAddress, afterAddress));
    config.validateMetadataCells(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  private void setStringField(String fieldName, String fieldValue) throws NoSuchFieldException, IllegalAccessException {
    Field metadataKeyCellsField = config.getClass().getDeclaredField(fieldName);
    metadataKeyCellsField.setAccessible(true);
    metadataKeyCellsField.set(config, fieldValue);
  }

  private void setBooleanField(String fieldName, Boolean fieldValue)
      throws NoSuchFieldException, IllegalAccessException {
    Field metadataKeyCellsField = config.getClass().getDeclaredField(fieldName);
    metadataKeyCellsField.setAccessible(true);
    metadataKeyCellsField.set(config, fieldValue);
  }
}
