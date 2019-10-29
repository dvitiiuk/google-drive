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

import com.google.api.services.drive.model.File;
import com.google.api.services.sheets.v4.model.Sheet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleDriveFilteringClient;
import io.cdap.plugin.google.common.GoogleFilteringSourceConfig;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.drive.source.utils.ExportedType;
import io.cdap.plugin.google.sheets.source.utils.CellCoordinate;
import io.cdap.plugin.google.sheets.source.utils.Formatting;
import io.cdap.plugin.google.sheets.source.utils.HeaderSelection;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import io.cdap.plugin.google.sheets.source.utils.Partitioning;
import io.cdap.plugin.google.sheets.source.utils.SheetsToPull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configurations for Google Sheets Batch Source plugin.
 */
public class GoogleSheetsSourceConfig extends GoogleFilteringSourceConfig {

  public static final String SHEETS_TO_PULL = "sheetsToPull";
  public static final String SHEETS_IDENTIFIERS = "sheetsIdentifiers";
  public static final String PARTITIONING = "partitioning";
  public static final String FORMATTING = "formatting";
  public static final String SKIP_EMPTY_DATA = "skipEmptyData";
  public static final String COLUMN_NAMES_SELECTION = "columnNamesSelection";
  public static final String CUSTOM_COLUMN_NAMES_ROW = "customColumnNamesRow";
  public static final String EXTRACT_METADATA = "extractMetadata";
  public static final String FIRST_HEADER_ROW = "firstHeaderRow";
  public static final String LAST_HEADER_ROW = "lastHeaderRow";
  public static final String FIRST_FOOTER_ROW = "firstFooterRow";
  public static final String LAST_FOOTER_ROW = "lastFooterRow";
  public static final String LAST_DATA_COLUMN = "lastDataColumn";
  public static final String LAST_DATA_ROW = "lastDataRow";
  public static final String METADATA_KEY_CELLS = "metadataKeyCells";
  public static final String METADATA_VALUE_CELLS = "metadataValueCells";
  public static final String NAME_SCHEMA = "schema";

  public static final String HEADERS_SELECTION_LABEL = "Header selection";
  public static final String SHEETS_TO_PULL_LABEL = "Sheets to pull";
  public static final String PARTITIONING_LABEL = "Partitioning";
  public static final String FORMATTING_LABEL = "Formatting";

  private transient Pattern cellAddress = Pattern.compile("^([A-Z]+)([0-9]+)$");

  @Name(SHEETS_TO_PULL)
  @Description("Filter for specifying set of sheets to process.  " +
      "For 'numbers' or 'titles' selections user can populate specific values in 'Sheets identifiers' field")
  @Macro
  protected String sheetsToPull;

  @Nullable
  @Name(SHEETS_IDENTIFIERS)
  @Description("Set of sheets' numbers/titles to process. " +
      "Is shown only when 'titles' or 'numbers' are selected for 'Sheets to pull' field. ")
  @Macro
  protected String sheetsIdentifiers;

  @Nullable
  @Name(NAME_SCHEMA)
  @Description("The schema of the table to read.")
  @Macro
  private transient Schema schema = null;

  @Name(PARTITIONING)
  @Description("")
  @Macro
  private String partitioning;

  @Name(FORMATTING)
  @Description("Output format for numeric sheet cells. " +
      "In 'Formatted values' case the value will contain appropriate format of source cell e.g. '1.23$', '123%'." +
      "For 'Values only' only number value will be returned.")
  @Macro
  private String formatting;

  @Name(SKIP_EMPTY_DATA)
  @Description("")
  @Macro
  private boolean skipEmptyData;

  @Name(COLUMN_NAMES_SELECTION)
  @Description("Source for column names.")
  @Macro
  private String columnNamesSelection;

  @Nullable
  @Name(CUSTOM_COLUMN_NAMES_ROW)
  @Description("Row number of the row to be treated as a header." +
      "Only shown when the 'Column Names Selection' field is set to 'Custom row as column names' header.")
  @Macro
  private Integer customColumnNamesRow;

  @Name(EXTRACT_METADATA)
  @Description("Field to enable metadata extraction. Metadata extraction is useful when user wants to specify " +
      "a header or a footer for a sheet. The rows in headers and footers are not available as data records. " +
      "Instead, they are available in every record as a field called 'metadata', " +
      "which is a record of the specified metadata.")
  @Macro
  private boolean extractMetadata;

  @Nullable
  @Name(FIRST_HEADER_ROW)
  @Description("Row number of the first row to be treated as header.")
  @Macro
  private String firstHeaderRow;

  @Nullable
  @Name(LAST_HEADER_ROW)
  @Description("Row number of the last row to be treated as header.")
  @Macro
  private String lastHeaderRow;

  @Nullable
  @Name(FIRST_FOOTER_ROW)
  @Description("Row number of the first row to be treated as footer.")
  @Macro
  private String firstFooterRow;

  @Nullable
  @Name(LAST_FOOTER_ROW)
  @Description("Row number of the last row to be treated as footer.")
  @Macro
  private String lastFooterRow;

  @Nullable
  @Name(LAST_DATA_COLUMN)
  @Description("")
  @Macro
  private String lastDataColumn;

  @Nullable
  @Name(LAST_DATA_ROW)
  @Description("")
  @Macro
  private String lastDataRow;

  @Nullable
  @Name(METADATA_KEY_CELLS)
  @Description("")
  @Macro
  private String metadataKeyCells;

  @Nullable
  @Name(METADATA_VALUE_CELLS)
  @Description("")
  @Macro
  private String metadataValueCells;

  private static LinkedHashMap<Integer, String> headerTitlesRow = null;

  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(this, headerTitlesRow);
    }
    return schema;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(filter) && collector.getValidationFailures().isEmpty()) {
      GoogleDriveFilteringClient driveClient = new GoogleDriveFilteringClient(this);
      GoogleSheetsSourceClient sheetsSourceClient = new GoogleSheetsSourceClient(this);
      List<File> spreadSheetsFiles =
          driveClient.getFilesSummary(Collections.singletonList(ExportedType.SPREADSHEETS));

      // validate source folder is not empty
      validateSourceFolder(collector, spreadSheetsFiles);

      // validate titles/numbers set
      validateSheetIdentifiers(collector, sheetsSourceClient, spreadSheetsFiles);

      // validate all sheets have the same schema
      validateSheetsSchema(collector, sheetsSourceClient, spreadSheetsFiles);
    }

    // validate metadata
    validateMetadata(collector);
  }

  private void validateSourceFolder(FailureCollector collector, List<File> spreadSheetsFiles) {
    if (spreadSheetsFiles.isEmpty()) {
      collector.addFailure(String.format("Not spreadsheets were found in '%s' folder with '%s' filter",
          getDirectoryIdentifier(), getFilter()), null)
          .withConfigProperty(DIRECTORY_IDENTIFIER).withConfigProperty(FILTER);
    }
  }

  private void validateSheetIdentifiers(FailureCollector collector, GoogleSheetsSourceClient sheetsSourceClient,
                                        List<File> spreadSheetsFiles) {
    if (!containsMacro(sheetsToPull) && collector.getValidationFailures().isEmpty()
        && !getSheetsToPull().equals(SheetsToPull.ALL)
        && checkPropertyIsSet(collector, sheetsIdentifiers, SHEETS_IDENTIFIERS, "")) {

      String currentSpreadSheetId = null;
      try {
        Map<String, List<String>> allTitles = new HashMap<>();
        Map<String, List<Integer>> allIndexes = new HashMap<>();
        for (File spreadSheetFile : spreadSheetsFiles) {
          currentSpreadSheetId = spreadSheetFile.getId();
          List<Sheet> sheets = sheetsSourceClient.getSheets(currentSpreadSheetId);
          allTitles.put(currentSpreadSheetId, sheets.stream()
              .map(s -> s.getProperties().getTitle()).collect(Collectors.toList()));
          allIndexes.put(currentSpreadSheetId, sheets.stream()
              .map(s -> s.getProperties().getIndex()).collect(Collectors.toList()));
        }

        SheetsToPull sheetsToPull = getSheetsToPull();
        switch (sheetsToPull) {
          case TITLES:
            List<String> titles = getSheetsIdentifiers();
            checkSheetIdentifiers(collector, titles, allTitles);
            break;
          case NUMBERS:
            List<Integer> indexes = getSheetsIdentifiers().stream().map(Integer::parseInt).collect(Collectors.toList());
            checkSheetIdentifiers(collector, indexes, allIndexes);
            break;
          default:
            throw new InvalidPropertyTypeException("", sheetsToPull.toString());
        }
      } catch (IOException e) {
        collector.addFailure(
            String.format("Exception during sheet identifiers check, spreadsheet id: '%s'",
                currentSpreadSheetId), null);
      }
    }
  }

  private <I> void checkSheetIdentifiers(FailureCollector collector, List<I> requiredIdentifiers,
                                         Map<String, List<I>> availableIdentifiers) {
    for (Map.Entry<String, List<I>> spreadSheetTitles : availableIdentifiers.entrySet()) {
      List<I> availableTitles = spreadSheetTitles.getValue();
      if (!availableTitles.containsAll(requiredIdentifiers)) {
        collector.addFailure(
            String.format("Spreadsheet '%s' doesn't have all required sheets. Required: %s, available: %s",
                spreadSheetTitles.getKey(), requiredIdentifiers, availableTitles), null)
            .withConfigProperty(sheetsIdentifiers);
      }
    }
  }

  private void validateSheetsSchema(FailureCollector collector, GoogleSheetsSourceClient sheetsSourceClient,
                                    List<File> spreadSheetsFiles) {
    if (collector.getValidationFailures().isEmpty()) {

      String currentSpreadSheetId = null;
      String currentSheetTitle = null;
      try {
        Map<String, List<String>> requiredTitles = new HashMap<>();
        for (File spreadSheetFile : spreadSheetsFiles) {
          currentSpreadSheetId = spreadSheetFile.getId();
          SheetsToPull sheetsToPull = getSheetsToPull();
          switch (sheetsToPull) {
            case ALL:
              requiredTitles.put(currentSpreadSheetId, sheetsSourceClient.getSheetsTitles(currentSpreadSheetId));
              break;
            case TITLES:
              requiredTitles.put(currentSpreadSheetId, getSheetsIdentifiers());
              break;
            case NUMBERS:
              requiredTitles.put(currentSpreadSheetId, sheetsSourceClient.getSheetsTitles(currentSpreadSheetId,
                  getSheetsIdentifiers().stream().map(Integer::parseInt).collect(Collectors.toList())));
              break;
            default:
              throw new InvalidPropertyTypeException("", sheetsToPull.toString());
          }
        }

        if (!getColumnNamesSelection().equals(HeaderSelection.NO_COLUMN_NAMEs)) {
          LinkedHashMap<Integer, String> resolvedHeaders = new LinkedHashMap<>();
          List<String> resultHeaderTitles = null;
          String resultHeaderSheetTitle = null;
          String resultHeaderSpreadsheetId = null;
          boolean noFailures = true;

          for (Map.Entry<String, List<String>> fileTitles : requiredTitles.entrySet()) {
            currentSpreadSheetId = fileTitles.getKey();
            currentSheetTitle = null;

            for (String sheetTitle : fileTitles.getValue()) {
              currentSheetTitle = sheetTitle;
              List<Object> headerRow = sheetsSourceClient.getRow(currentSpreadSheetId, currentSheetTitle,
                  getCustomColumnNamesRow() + 1);
              List<String> headerTitles = new ArrayList<>();
              for (int i = 0; i < headerRow.size(); i++) {
                Object headerCell = headerRow.get(i);
                if (!(headerCell instanceof String)) {
                  collector.addFailure(
                      String.format("Cell which will been used as header is not a string, actual type: '%s', " +
                              "value: '%s', sheet title: '%s, spreadsheet id: '%s'",
                          headerCell.getClass(), headerCell.toString(), currentSheetTitle, currentSpreadSheetId),
                      null);
                  return;
                } else if (StringUtils.isEmpty(headerCell.toString())) {
                  if (true) {
                    resolvedHeaders.put(i, defaultGeneratedHeader(i));
                  }
                } else {
                  if (resultHeaderTitles == null) {
                    resolvedHeaders.put(i, headerCell.toString());
                  }
                  headerTitles.add(headerCell.toString());
                }
              }
              if (resultHeaderTitles == null) {
                resultHeaderTitles = headerTitles;
                resultHeaderSheetTitle = currentSheetTitle;
                resultHeaderSpreadsheetId = currentSpreadSheetId;
              } else {
                // compare current with general
                if (resultHeaderTitles.size() != headerTitles.size() || resultHeaderTitles.equals(headerTitles)) {
                  collector.addFailure(
                      String.format("Headers form (spreadsheetId '%s', sheet title '%s') " +
                              "and (spreadsheetId '%s', sheet title '%s') have different value: '%s' and '%s'",
                          resultHeaderSheetTitle, resultHeaderSpreadsheetId, sheetTitle, currentSpreadSheetId,
                          resultHeaderTitles.toString(), headerTitles.toString()), null);
                  noFailures = false;
                }
              }
            }
          }
          if (noFailures) {
            headerTitlesRow = resolvedHeaders;
          }
        } else {
          // read first row of data and get column number
          // if row is empty use last column

          // get first data row number
          int firstDataRow = 0;
          if (isExtractMetadata() && getLastHeaderRow() > -1) {
            firstDataRow = getLastHeaderRow() + 1;
          }
          Map.Entry<String, List<String>> firstFileTitles = requiredTitles.entrySet().iterator().next();
          List<Object> data = sheetsSourceClient.getRow(firstFileTitles.getKey(), firstFileTitles.getValue().get(0),
              firstDataRow + 1);
          if (CollectionUtils.isEmpty(data)) {
            headerTitlesRow = defaultGeneratedHeaders(getLastDataColumn());
          } else {
            headerTitlesRow = defaultGeneratedHeaders(data.size());
          }
        }
      } catch (IOException e) {
        collector.addFailure(
            String.format("Exception during headers preparing, spreadsheet id: '%s', sheet title: '%s'",
                currentSpreadSheetId, currentSheetTitle), null);
      }
    }
  }

  public int getActualFirstDataRow() {
    int firstDataRow = 0;
    if (isExtractMetadata() && getLastHeaderRow() > -1) {
      firstDataRow = getLastHeaderRow() + 1;
    }
    return firstDataRow;
  }

  public int getActualLastDataRow() {
    int lastDataRow = getLastDataRow();
    if (isExtractMetadata() && getFirstFooterRow() > -1) {
      lastDataRow = Math.min(lastDataRow, getFirstFooterRow() - 1);
    }
    return lastDataRow;
  }

  private LinkedHashMap<Integer, String> defaultGeneratedHeaders(int length) {
    LinkedHashMap<Integer, String> headers = new LinkedHashMap<>();
    for (int i = 1; i <= length; i++) {
      headers.put(i - 1, defaultGeneratedHeader(i));
    }
    return headers;
  }

  private String defaultGeneratedHeader(int length) {
    StringBuilder columnName = new StringBuilder();
    while (length > 0) {
      // Find remainder
      int rem = length % 26;

      // If remainder is 0, then a
      // 'Z' must be there in output
      if (rem == 0) {
        columnName.append("Z");
        length = (length / 26) - 1;
      } else {
        columnName.append((char) ((rem - 1) + 'A'));
        length = length / 26;
      }
    }

    // Reverse the string and print result
    return columnName.reverse().toString();
  }

  public void validateMetadata(FailureCollector collector) {
    if (extractMetadata
        && checkPropertyIsSet(collector, metadataKeyCells, METADATA_KEY_CELLS, "")
        && checkPropertyIsSet(collector, metadataValueCells, METADATA_VALUE_CELLS, "")
        & checkPropertyIsValid(collector, firstFooterRow != null && getFirstHeaderRow() > -2,
        FIRST_HEADER_ROW, getFirstHeaderRow(), "")
        & checkPropertyIsValid(collector, lastHeaderRow != null && getLastHeaderRow() > -2,
        LAST_HEADER_ROW, getFirstHeaderRow(), "")
        & checkPropertyIsValid(collector, firstFooterRow != null && getFirstFooterRow() > -2,
        FIRST_FOOTER_ROW, firstFooterRow, "")
        & checkPropertyIsValid(collector, lastFooterRow != null && getLastFooterRow() > -2,
        LAST_FOOTER_ROW, lastFooterRow, "")) {

      if (getFirstHeaderRow() == -1 && getLastHeaderRow() == -1
          && getFirstFooterRow() == -1 && getLastFooterRow() == -1) {
        collector.addFailure("No header or footer rows specified", null)
            .withConfigProperty(firstHeaderRow)
            .withConfigProperty(lastHeaderRow)
            .withConfigProperty(firstFooterRow)
            .withConfigProperty(lastFooterRow);
        return;
      }
      if ((getFirstHeaderRow() != -1 && getLastHeaderRow() == -1)
          || (getFirstHeaderRow() == -1 && getLastHeaderRow() != -1)) {
        collector.addFailure("Both first and last rows should be specified or not for header", null)
            .withConfigProperty(firstHeaderRow)
            .withConfigProperty(lastHeaderRow);
        return;
      }
      if ((getFirstFooterRow() != -1 && getLastFooterRow() == -1)
          || (getFirstFooterRow() == -1 && getLastFooterRow() != -1)) {
        collector.addFailure("Both first and last rows should be specified or not for footer", null)
            .withConfigProperty(firstFooterRow)
            .withConfigProperty(lastFooterRow);
        return;
      }
      if (getFirstHeaderRow() > getLastHeaderRow()) {
        collector.addFailure("Header first row cannot be less of header last row", null)
            .withConfigProperty(firstHeaderRow)
            .withConfigProperty(lastHeaderRow);
        return;
      }
      if (getFirstFooterRow() > getLastFooterRow()) {
        collector.addFailure("Footer first row cannot be less of footer last row", null)
            .withConfigProperty(firstFooterRow)
            .withConfigProperty(lastFooterRow);
        return;
      }
      // we should have at least one data row
      if (getLastHeaderRow() + 1 >= getFirstFooterRow()) {
        collector.addFailure("Header and footer are intersected or there are no any data row between them",
            null)
            .withConfigProperty(firstFooterRow)
            .withConfigProperty(lastFooterRow);
        return;
      }

      Map<String, String> keyPairs = validateMetadataKeyCells(collector);
      Map<String, String> valuePairs = validateMetadataValueCells(collector);
      if (MapUtils.isNotEmpty(keyPairs) || MapUtils.isNotEmpty(valuePairs)) {
        for (Map.Entry<String, String> key : keyPairs.entrySet()) {
          String keyAddress = key.getKey();
          String keyName = key.getValue();
          if (!valuePairs.containsKey(keyAddress)) {
            collector.addFailure(
                String.format("There is no value cell for key '%s' with name '%s'", keyAddress, keyName), null)
                .withConfigProperty(metadataKeyCells).withConfigProperty(metadataValueCells);
          }
        }
        for (Map.Entry<String, String> value : valuePairs.entrySet()) {
          String valueAddress = value.getKey();
          String valueName = value.getValue();
          if (!valuePairs.containsKey(valueAddress)) {
            collector.addFailure(
                String.format("There is no key cell for value '%s' with name '%s'", valueAddress, valueName), null)
                .withConfigProperty(metadataKeyCells).withConfigProperty(metadataValueCells);
          }
        }
      }
    }
  }

  public Map<String, String> validateMetadataKeyCells(FailureCollector collector) {
    return validateMetadataCells(collector, metadataKeyCells, METADATA_KEY_CELLS, false);
  }

  public Map<String, String> validateMetadataValueCells(FailureCollector collector) {
    return validateMetadataCells(collector, metadataValueCells, METADATA_VALUE_CELLS, true);
  }

  Map<String, String> validateMetadataCells(FailureCollector collector, String propertyValue,
                                                    String propertyName, boolean directOrder) {
    Map<String, String> pairs = metadataInputToMap(propertyValue, directOrder);
    Set<String> keys = new HashSet<>();
    Set<String> values = new HashSet<>();
    for (Map.Entry<String, String> pairEntry : pairs.entrySet()) {
      String name = pairEntry.getKey();
      String address = pairEntry.getValue();
      Matcher m = cellAddress.matcher(address);
      if (m.find()) {
        Integer row = Integer.parseInt(m.group(2));
        if (!((row < getFirstHeaderRow() + 1 || row > getLastHeaderRow() + 1)
            ^ (row < getFirstFooterRow() + 1 || row > getLastFooterRow() + 1))) {
          collector.addFailure(String.format("Metadata cell '%s' is out of header or footer rows", address),
              null)
              .withConfigProperty(propertyName);
        }
        if (keys.contains(address)) {
          collector.addFailure(String.format("Duplicate address '%s'", address), null)
              .withConfigProperty(propertyName);
        }
        keys.add(address);
        if (values.contains(name)) {
          collector.addFailure(String.format("Duplicate name '%s'", name), null)
              .withConfigProperty(propertyName);
        }
        values.add(name);
      } else {
        collector.addFailure(String.format("Invalid cell address '%s'", address), null)
            .withConfigProperty(propertyName);
      }
    }
    return pairs;
  }

  Map<String, String> metadataInputToMap(String input, boolean directOrder) {
    return Arrays.stream(input.split(",")).map(p -> p.split(":"))
        .filter(p -> p.length == 2)
        .collect(Collectors.toMap(p -> directOrder ? p[0] : p[1], p -> directOrder ? p[1] : p[0]));
  }

  public Formatting getFormatting() {
    return Formatting.fromValue(formatting);
  }

  public boolean isSkipEmptyData() {
    return skipEmptyData;
  }

  public HeaderSelection getColumnNamesSelection() {
    return HeaderSelection.fromValue(columnNamesSelection);
  }

  public int getCustomColumnNamesRow() {
    return customColumnNamesRow;
  }

  public boolean isExtractMetadata() {
    return extractMetadata;
  }

  @Nullable
  public Integer getFirstHeaderRow() {
    return Integer.parseInt(firstHeaderRow);
  }

  @Nullable
  public Integer getLastHeaderRow() {
    return Integer.parseInt(lastHeaderRow);
  }

  @Nullable
  public Integer getFirstFooterRow() {
    return Integer.parseInt(firstFooterRow);
  }

  @Nullable
  public Integer getLastFooterRow() {
    return Integer.parseInt(lastFooterRow);
  }

  @Nullable
  public Integer getLastDataColumn() {
    return Integer.parseInt(lastDataColumn);
  }

  @Nullable
  public Integer getLastDataRow() {
    return Integer.parseInt(lastDataRow);
  }

  @Nullable
  public String getMetadataKeyCells() {
    return metadataKeyCells;
  }

  @Nullable
  public String getMetadataValueCells() {
    return metadataValueCells;
  }

  public Partitioning getPartitioning() {
    return Partitioning.fromValue(partitioning);
  }

  public SheetsToPull getSheetsToPull() {
    return SheetsToPull.fromValue(sheetsToPull);
  }

  @Nullable
  public List<String> getSheetsIdentifiers() {
    return Arrays.asList(sheetsIdentifiers.split(","));
  }

  public LinkedHashMap<Integer, String> getHeaderTitlesRow() {
    return headerTitlesRow;
  }

  public List<MetadataKeyValueAddress> getMetadataCoordinates() {
    List<MetadataKeyValueAddress> metadataCoordinates = new ArrayList<>();
    Map<String, String> keyPairs = metadataInputToMap(metadataKeyCells, true);
    Map<String, String> valuePairs = metadataInputToMap(metadataValueCells, true);

    for (Map.Entry<String, String> keyPair : keyPairs.entrySet()) {
      metadataCoordinates.add(new MetadataKeyValueAddress(toCoordinate(keyPair.getKey()),
          toCoordinate(valuePairs.get(keyPair.getValue()))));
    }
    return metadataCoordinates;
  }

  CellCoordinate toCoordinate(String address) {
    Matcher m = cellAddress.matcher(address);
    if (m.find()) {
      return new CellCoordinate(Integer.parseInt(m.group(2)), getNumberOfColumn(m.group(1)));
    }
    throw new IllegalArgumentException(String.format("Cannot to parse '%s' cell address", address));
  }

  int getNumberOfColumn(String input) {
    int sum = 0;
    final int base = 26;
    final int sub = 64;
    for (int i = 0; i < input.length(); ++i) {
      int representation = input.charAt(i);
      representation -= sub;
      int value = (int) (representation * Math.pow(base, input.length() - i - 1));
      sum += value;
    }
    return sum;
  }
}
