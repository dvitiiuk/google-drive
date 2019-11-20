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

import com.github.rholder.retry.RetryException;
import com.google.api.services.drive.model.File;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.CellFormat;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.Sheet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleDriveFilteringClient;
import io.cdap.plugin.google.common.GoogleFilteringSourceConfig;
import io.cdap.plugin.google.common.ValidationResult;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.drive.source.utils.ExportedType;
import io.cdap.plugin.google.sheets.source.utils.CellCoordinate;
import io.cdap.plugin.google.sheets.source.utils.ColumnComplexSchemaInfo;
import io.cdap.plugin.google.sheets.source.utils.Formatting;
import io.cdap.plugin.google.sheets.source.utils.HeaderSelection;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import io.cdap.plugin.google.sheets.source.utils.SheetsToPull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configurations for Google Sheets Batch Source plugin.
 */
public class GoogleSheetsSourceConfig extends GoogleFilteringSourceConfig {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSheetsSourceConfig.class);

  public static final String SHEETS_TO_PULL = "sheetsToPull";
  public static final String SHEETS_IDENTIFIERS = "sheetsIdentifiers";
  public static final String FORMATTING = "formatting";
  public static final String SKIP_EMPTY_DATA = "skipEmptyData";
  public static final String COLUMN_NAMES_SELECTION = "columnNamesSelection";
  public static final String CUSTOM_COLUMN_NAMES_ROW = "customColumnNamesRow";
  public static final String METADATA_RECORD_NAME = "metadataRecordName";
  public static final String EXTRACT_METADATA = "extractMetadata";
  public static final String FIRST_HEADER_ROW = "firstHeaderRow";
  public static final String LAST_HEADER_ROW = "lastHeaderRow";
  public static final String FIRST_FOOTER_ROW = "firstFooterRow";
  public static final String LAST_FOOTER_ROW = "lastFooterRow";
  public static final String LAST_DATA_COLUMN = "lastDataColumn";
  public static final String LAST_DATA_ROW = "lastDataRow";
  public static final String METADATA_CELLS = "metadataCells";
  public static final String NAME_SCHEMA = "schema";

  public static final String HEADERS_SELECTION_LABEL = "Header selection";
  public static final String SHEETS_TO_PULL_LABEL = "Sheets to pull";
  public static final String FORMATTING_LABEL = "Formatting";
  public static final String SHEETS_IDENTIFIERS_LABEL = "Sheets identifiers";

  private transient Pattern cellAddress = Pattern.compile("^([A-Z]+)([0-9]+)$");
  private transient Pattern columnName = Pattern.compile("^[A-Za-z_][A-Za-z0-9_-]*$");

  @Name(SHEETS_TO_PULL)
  @Description("Filter for specifying set of sheets to process.  " +
      "For 'numbers' or 'titles' selections user can populate specific values in 'Sheets identifiers' field.")
  @Macro
  private String sheetsToPull;

  @Nullable
  @Name(SHEETS_IDENTIFIERS)
  @Description("Set of sheets' numbers/titles to process. " +
      "Is shown only when 'titles' or 'numbers' are selected for 'Sheets to pull' field.")
  @Macro
  private String sheetsIdentifiers;

  @Nullable
  @Name(NAME_SCHEMA)
  @Description("The schema of the table to read.")
  @Macro
  private transient Schema schema = null;

  @Name(FORMATTING)
  @Description("Output format for numeric sheet cells. " +
      "In 'Formatted values' case the value will contain appropriate format of source cell e.g. '1.23$', '123%'." +
      "For 'Values only' only number value will be returned.")
  @Macro
  private String formatting;

  @Name(SKIP_EMPTY_DATA)
  @Description("Field to allow skipping of empty structure records.")
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

  @Name(METADATA_RECORD_NAME)
  @Description("Name of the record with metadata content. " +
      "It is needed to distinguish metadata record from possible column with the same name.")
  @Macro
  private String metadataRecordName;

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
  @Description("Last column number of the maximal field of plugin work for data.")
  @Macro
  private String lastDataColumn;

  @Nullable
  @Name(LAST_DATA_ROW)
  @Description("Last row number of the maximal field of plugin work for data.")
  @Macro
  private String lastDataRow;

  @Nullable
  @Name(METADATA_CELLS)
  @Description("Set of the cells for key-value pairs to extract as metadata from the specified metadata sections. " +
      "Only shown if Extract metadata is set to true. The cell numbers should be within the header or footer.")
  @Macro
  private String metadataCells;

  private static LinkedHashMap<Integer, ColumnComplexSchemaInfo> dataSchemaInfo = new LinkedHashMap<>();
  private static final int FILES_TO_VALIDATE = 5;

  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(this, dataSchemaInfo);
    }
    return schema;
  }

  public ValidationResult validate(FailureCollector collector) {
    ValidationResult validationResult = super.validate(collector);
    if (!containsMacro(filter) && collector.getValidationFailures().isEmpty()
        && validationResult.isCredentialsAvailable()) {
      GoogleDriveFilteringClient driveClient = null;
      GoogleSheetsSourceClient sheetsSourceClient = null;
      try {
        driveClient = new GoogleDriveFilteringClient(this);
        sheetsSourceClient = new GoogleSheetsSourceClient(this);
      } catch (IOException e) {
        collector.addFailure(String.format("", ""), null);
        return validationResult;
      }
      List<File> spreadSheetsFiles = null;
      try {
        spreadSheetsFiles = driveClient
            .getFilesSummary(Collections.singletonList(ExportedType.SPREADSHEETS), FILES_TO_VALIDATE);
      } catch (ExecutionException | RetryException e) {
        collector.addFailure("Files summary retrieving failed.", null)
            .withStacktrace(e.getStackTrace());
      }

      // validate source folder is not empty
      validateSourceFolder(collector, spreadSheetsFiles);

      try {
        // validate titles/numbers set
        validateSheetIdentifiers(collector, sheetsSourceClient, spreadSheetsFiles);

        // validate all sheets have the same schema
        validateSheetsSchema(collector, sheetsSourceClient, spreadSheetsFiles);
      } catch (ExecutionException | RetryException e) {
        collector.addFailure(String.format("Exception during validation."), null)
            .withStacktrace(e.getStackTrace());
      }
    }

    // validate metadata
    validateMetadata(collector);
    return validationResult;
  }

  private void validateSourceFolder(FailureCollector collector, List<File> spreadSheetsFiles) {
    if (spreadSheetsFiles.isEmpty()) {
      collector.addFailure(String.format("Not spreadsheets were found in '%s' folder with '%s' filter.",
          getDirectoryIdentifier(), getFilter()), null)
          .withConfigProperty(DIRECTORY_IDENTIFIER).withConfigProperty(FILTER);
    }
  }

  private void validateSheetIdentifiers(FailureCollector collector, GoogleSheetsSourceClient sheetsSourceClient,
                                        List<File> spreadSheetsFiles) throws ExecutionException, RetryException {
    if (!containsMacro(sheetsToPull) && collector.getValidationFailures().isEmpty()
        && !getSheetsToPull().equals(SheetsToPull.ALL)
        && checkPropertyIsSet(collector, sheetsIdentifiers, SHEETS_IDENTIFIERS, SHEETS_IDENTIFIERS_LABEL)) {

      String currentSpreadSheetId = null;
      Map<String, List<String>> allTitles = new HashMap<>();
      Map<String, List<Integer>> allIndexes = new HashMap<>();
      for (int i = 0; i < spreadSheetsFiles.size() && i < FILES_TO_VALIDATE; i++) {
        File spreadSheetFile = spreadSheetsFiles.get(i);
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
          throw new InvalidPropertyTypeException(SHEETS_TO_PULL_LABEL, sheetsToPull.toString(),
            SheetsToPull.getAllowedValues());
      }
    }
  }

  private <I> void checkSheetIdentifiers(FailureCollector collector, List<I> requiredIdentifiers,
                                         Map<String, List<I>> availableIdentifiers) {
    for (Map.Entry<String, List<I>> spreadSheetTitles : availableIdentifiers.entrySet()) {
      List<I> availableTitles = spreadSheetTitles.getValue();
      if (!availableTitles.containsAll(requiredIdentifiers)) {
        collector.addFailure(
            String.format("Spreadsheet '%s' doesn't have all required sheets. Required: %s, available: %s.",
                spreadSheetTitles.getKey(), requiredIdentifiers, availableTitles), null)
            .withConfigProperty(sheetsIdentifiers);
      }
    }
  }

  private void validateSheetsSchema(FailureCollector collector, GoogleSheetsSourceClient sheetsSourceClient,
                                    List<File> spreadSheetsFiles) throws ExecutionException, RetryException {
    if (collector.getValidationFailures().isEmpty()) {

      String currentSpreadSheetId = null;
      String currentSheetTitle = null;
      try {
        Map<String, List<String>> requiredTitles = new HashMap<>();
        for (int i = 0; i < spreadSheetsFiles.size() && i < FILES_TO_VALIDATE; i++) {
          File spreadSheetFile = spreadSheetsFiles.get(i);
          currentSpreadSheetId = spreadSheetFile.getId();
          SheetsToPull sheetsToPull = getSheetsToPull();
          switch (sheetsToPull) {
            case ALL:
              requiredTitles.put(currentSpreadSheetId,
                  sheetsSourceClient.getSheetsTitles(currentSpreadSheetId));
              break;
            case TITLES:
              requiredTitles.put(currentSpreadSheetId, getSheetsIdentifiers());
              break;
            case NUMBERS:
              requiredTitles.put(currentSpreadSheetId, sheetsSourceClient.getSheetsTitles(currentSpreadSheetId,
                  getSheetsIdentifiers().stream().map(Integer::parseInt).collect(Collectors.toList())));
              break;
            default:
              throw new InvalidPropertyTypeException(SHEETS_TO_PULL_LABEL, sheetsToPull.toString(),
                SheetsToPull.getAllowedValues());
          }
        }

        int columnNamesRow = getCustomColumnNamesRow();
        int firstDataRow = getActualFirstDataRow();
        if (!getColumnNamesSelection().equals(HeaderSelection.NO_COLUMN_NAMES)) {
          LinkedHashMap<Integer, ColumnComplexSchemaInfo> resolvedSchemas = new LinkedHashMap<>();
          List<String> resultHeaderTitles = null;
          String resultHeaderSheetTitle = null;
          String resultHeaderSpreadsheetId = null;

          for (Map.Entry<String, List<String>> fileTitles : requiredTitles.entrySet()) {
            currentSpreadSheetId = fileTitles.getKey();
            currentSheetTitle = null;

            for (String sheetTitle : fileTitles.getValue()) {
              currentSheetTitle = sheetTitle;

              Map<Integer, List<CellData>> headerDataRows = sheetsSourceClient.getSingleRows(currentSpreadSheetId,
                  currentSheetTitle, Arrays.asList(new Integer[]{columnNamesRow, firstDataRow}));

              List<CellData> columnsRow = headerDataRows.get(columnNamesRow);
              List<CellData> dataRow = headerDataRows.get(firstDataRow);
              if (CollectionUtils.isEmpty(columnsRow)) {
                collector.addFailure(
                    String.format("No headers found for row '%d', spreadsheet id '%s', sheet title '%s'.",
                        columnNamesRow, currentSpreadSheetId, currentSheetTitle), null)
                    .withConfigProperty(CUSTOM_COLUMN_NAMES_ROW);
                return;
              }
              List<String> headerTitles = new ArrayList<>();
              for (int i = 0; i < columnsRow.size(); i++) {
                CellData columnHeaderCell = columnsRow.get(i);
                String title = columnHeaderCell.getFormattedValue();
                if (StringUtils.isNotEmpty(title)) {
                  if (!columnName.matcher(title).matches()) {
                    String defaultColumnName = defaultGeneratedHeader(i + 1);
                    LOG.warn(String.format("Original column name '%s' doesn't satisfy column name requirements '%s', " +
                      "the default column name '%s' will be used.", title, columnName.pattern(), defaultColumnName));
                    title = defaultColumnName;
                  }
                  if (resultHeaderTitles == null) {
                    Schema dataSchema = Schema.of(Schema.Type.STRING);
                    if (dataRow != null && dataRow.size() > i) {
                      CellData dataCell = dataRow.get(i);
                      dataSchema = getCellSchema(dataCell);
                    } else {
                      LOG.warn(String.format("There is empty data cell for '%s' column during data types defining.",
                          title));
                    }
                    resolvedSchemas.put(i, new ColumnComplexSchemaInfo(title, dataSchema));
                  }
                  if (headerTitles.contains(title)) {
                    collector.addFailure(String.format("Duplicate column name '%s'.", title),
                        null);
                  }
                  headerTitles.add(title);
                }
              }
              if (resultHeaderTitles == null) {
                resultHeaderTitles = headerTitles;
                resultHeaderSheetTitle = currentSheetTitle;
                resultHeaderSpreadsheetId = currentSpreadSheetId;
              } else {
                // compare current with general
                if (resultHeaderTitles.size() != headerTitles.size() || !resultHeaderTitles.equals(headerTitles)) {
                  collector.addFailure(
                      String.format("Headers form (spreadsheetId '%s', sheet title '%s') " +
                              "and (spreadsheetId '%s', sheet title '%s') have different value: '%s' and '%s'.",
                          resultHeaderSheetTitle, resultHeaderSpreadsheetId, sheetTitle, currentSpreadSheetId,
                          resultHeaderTitles.toString(), headerTitles.toString()), null);
                }
              }
            }
          }
          if (collector.getValidationFailures().isEmpty()) {
            dataSchemaInfo = resolvedSchemas;
          }
        } else {
          // read first row of data and get column number
          // if row is empty use last column

          Map.Entry<String, List<String>> firstFileTitles = requiredTitles.entrySet().iterator().next();
          Map<Integer, List<CellData>> firstRowData = sheetsSourceClient.getSingleRows(firstFileTitles.getKey(),
              firstFileTitles.getValue().get(0), Collections.singletonList(firstDataRow));
          List<CellData> dataCells = firstRowData.get(firstDataRow);
          if (CollectionUtils.isEmpty(dataCells)) {
            dataSchemaInfo = defaultGeneratedHeaders(getLastDataColumn());
          } else {
            dataSchemaInfo = defaultGeneratedHeaders(dataCells.size());
          }
        }
      } catch (IOException e) {
        collector.addFailure(
            String.format("Exception during headers preparing, spreadsheet id: '%s', sheet title: '%s'.",
                currentSpreadSheetId, currentSheetTitle), null);
      }
    }
  }

  public int getActualFirstDataRow() {
    int firstDataRow = 1;
    if (isExtractMetadata() && getLastHeaderRow() > -1) {
      firstDataRow = getLastHeaderRow() + 1;
    }
    if (!getColumnNamesSelection().equals(HeaderSelection.NO_COLUMN_NAMES)) {
      firstDataRow = Math.max(firstDataRow, getCustomColumnNamesRow() + 1);
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

  Schema getCellSchema(CellData cellData) {
    if (cellData == null || cellData.size() == 0) {
      return Schema.of(Schema.Type.STRING);
    }
    ExtendedValue value = cellData.getEffectiveValue();
    if (value.getNumberValue() != null) {
      CellFormat userEnteredFormat = cellData.getUserEnteredFormat();
      if (userEnteredFormat != null && userEnteredFormat.getNumberFormat() != null) {
        String type = userEnteredFormat.getNumberFormat().getType();
        switch (type) {
          case "DATE":
            return Schema.of(Schema.LogicalType.DATE);
          case "TIME":
            return Schema.of(Schema.Type.LONG);
          case "DATE_TIME":
            return Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
        }
      }
      if (getFormatting().equals(Formatting.VALUES_ONLY)) {
        return Schema.of(Schema.Type.DOUBLE);
      } else {
        return Schema.of(Schema.Type.STRING);
      }
    } else if (value.getBoolValue() != null) {
      return Schema.of(Schema.Type.BOOLEAN);
    } else {
      return Schema.of(Schema.Type.STRING);
    }
  }

  private LinkedHashMap<Integer, ColumnComplexSchemaInfo> defaultGeneratedHeaders(int length) {
    LinkedHashMap<Integer, ColumnComplexSchemaInfo> headers = new LinkedHashMap<>();
    for (int i = 1; i <= length; i++) {
      headers.put(i - 1, new ColumnComplexSchemaInfo(defaultGeneratedHeader(i), Schema.of(Schema.Type.STRING)));
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
        && checkPropertyIsSet(collector, metadataCells, METADATA_CELLS, "")
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
        collector.addFailure("No header or footer rows specified.", null)
            .withConfigProperty(firstHeaderRow)
            .withConfigProperty(lastHeaderRow)
            .withConfigProperty(firstFooterRow)
            .withConfigProperty(lastFooterRow);
        return;
      }
      if ((getFirstHeaderRow() != -1 && getLastHeaderRow() == -1)
          || (getFirstHeaderRow() == -1 && getLastHeaderRow() != -1)) {
        collector.addFailure("Both first and last rows should be specified or not for header.", null)
            .withConfigProperty(firstHeaderRow)
            .withConfigProperty(lastHeaderRow);
        return;
      }
      if ((getFirstFooterRow() != -1 && getLastFooterRow() == -1)
          || (getFirstFooterRow() == -1 && getLastFooterRow() != -1)) {
        collector.addFailure("Both first and last rows should be specified or not for footer.", null)
            .withConfigProperty(firstFooterRow)
            .withConfigProperty(lastFooterRow);
        return;
      }
      if (getFirstHeaderRow() > getLastHeaderRow()) {
        collector.addFailure("Header first row cannot be less of header last row.", null)
            .withConfigProperty(firstHeaderRow)
            .withConfigProperty(lastHeaderRow);
        return;
      }
      if (getFirstFooterRow() > getLastFooterRow()) {
        collector.addFailure("Footer first row cannot be less of footer last row.", null)
            .withConfigProperty(firstFooterRow)
            .withConfigProperty(lastFooterRow);
        return;
      }
      // we should have at least one data row
      if (getLastHeaderRow() + 1 >= getFirstFooterRow()) {
        collector.addFailure("Header and footer are intersected or there are no any data row between them.",
            null)
            .withConfigProperty(firstFooterRow)
            .withConfigProperty(lastFooterRow);
        return;
      }

      List<String> columnNames = dataSchemaInfo.values().stream().map(c -> c.getHeaderTitle())
          .collect(Collectors.toList());
      if (columnNames.contains(metadataRecordName)) {
        collector.addFailure(String.format("Metadata record name '%s' coincides with one of the column names.",
            metadataRecordName), null).withConfigProperty(METADATA_RECORD_NAME);
      }

      validateMetadataCells(collector);
    }
  }

  Map<String, String> validateMetadataCells(FailureCollector collector) {
    Map<String, String> pairs = metadataInputToMap(metadataCells);
    Set<String> keys = new HashSet<>();
    Set<String> values = new HashSet<>();
    for (Map.Entry<String, String> pairEntry : pairs.entrySet()) {
      String keyAddress = pairEntry.getKey();
      String valueAddress = pairEntry.getValue();
      if (validateMetadataAddress(collector, keyAddress) & validateMetadataAddress(collector, valueAddress)) {
        if (keys.contains(keyAddress)) {
          collector.addFailure(String.format("Duplicate key address '%s'.", keyAddress), null)
              .withConfigProperty(METADATA_CELLS);
        }
        keys.add(keyAddress);
        if (values.contains(valueAddress)) {
          collector.addFailure(String.format("Duplicate value address '%s'.", valueAddress), null)
              .withConfigProperty(METADATA_CELLS);
        }
        values.add(valueAddress);
      }
    }
    return pairs;
  }

  boolean validateMetadataAddress(FailureCollector collector, String address) {
    Matcher m = cellAddress.matcher(address);
    if (m.find()) {
      Integer row = Integer.parseInt(m.group(2));
      if (!((row < getFirstHeaderRow() || row > getLastHeaderRow())
          ^ (row < getFirstFooterRow() || row > getLastFooterRow()))) {
        collector.addFailure(String.format("Metadata cell '%s' is out of header or footer rows.", address),
            null)
            .withConfigProperty(METADATA_CELLS);
        return false;
      }
    } else {
      collector.addFailure(String.format("Invalid cell address '%s'.", address), null)
          .withConfigProperty(METADATA_CELLS);
      return false;
    }
    return true;
  }

  Map<String, String> metadataInputToMap(String input) {
    return Arrays.stream(input.split(",")).map(p -> p.split(":"))
        .filter(p -> p.length == 2)
        .collect(Collectors.toMap(p -> p[0], p -> p[1]));
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
    return customColumnNamesRow == null ? 1 : customColumnNamesRow;
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
  public String getMetadataCells() {
    return metadataCells;
  }

  public String getMetadataRecordName() {
    return metadataRecordName;
  }

  public SheetsToPull getSheetsToPull() {
    return SheetsToPull.fromValue(sheetsToPull);
  }

  @Nullable
  public List<String> getSheetsIdentifiers() {
    return Arrays.asList(sheetsIdentifiers.split(","));
  }

  public Map<Integer, String> getHeaderTitlesRow() {
    return dataSchemaInfo.entrySet().stream()
        .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().getHeaderTitle()));
  }

  public List<MetadataKeyValueAddress> getMetadataCoordinates() {
    List<MetadataKeyValueAddress> metadataCoordinates = new ArrayList<>();
    if (extractMetadata) {
      Map<String, String> keyValuePairs = metadataInputToMap(metadataCells);

      for (Map.Entry<String, String> pair : keyValuePairs.entrySet()) {
        metadataCoordinates.add(new MetadataKeyValueAddress(toCoordinate(pair.getKey()),
            toCoordinate(pair.getValue())));
      }
    }
    return metadataCoordinates;
  }

  CellCoordinate toCoordinate(String address) {
    Matcher m = cellAddress.matcher(address);
    if (m.find()) {
      return new CellCoordinate(Integer.parseInt(m.group(2)), getNumberOfColumn(m.group(1)));
    }
    throw new IllegalArgumentException(String.format("Cannot to parse '%s' cell address.", address));
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
