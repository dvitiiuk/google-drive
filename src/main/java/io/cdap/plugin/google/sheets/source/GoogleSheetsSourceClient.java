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

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.GridData;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import io.cdap.plugin.google.common.APIRequest;
import io.cdap.plugin.google.common.APIRequestRepeater;
import io.cdap.plugin.google.sheets.common.GoogleSheetsClient;
import io.cdap.plugin.google.sheets.common.Sheet;
import io.cdap.plugin.google.sheets.source.utils.CellCoordinate;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Client for getting data via Google Sheets API.
 */
public class GoogleSheetsSourceClient extends GoogleSheetsClient<GoogleSheetsSourceConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSheetsSourceClient.class);

  public GoogleSheetsSourceClient(GoogleSheetsSourceConfig config) {
    super(config);
  }

  @Override
  protected List<String> getRequiredScopes() {
    return Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);
  }

  public List<com.google.api.services.sheets.v4.model.Sheet> getSheets(String spreadSheetId)
      throws IOException, InterruptedException {
    return new APIRequestRepeater<APIRequest<List<com.google.api.services.sheets.v4.model.Sheet>>,
        List<com.google.api.services.sheets.v4.model.Sheet>>() {
    }.doRepeatable(new APIRequest<List<com.google.api.services.sheets.v4.model.Sheet>>() {
      @Override
      public List<com.google.api.services.sheets.v4.model.Sheet> doRequest() throws IOException {
        Spreadsheet spreadsheet = service.spreadsheets().get(spreadSheetId).execute();
        return spreadsheet.getSheets();
      }

      @Override
      public String getLog() {
        return String.format("Sheets reading, spreadsheetId '%s'", spreadSheetId);
      }
    });
  }

  public List<String> getSheetsTitles(String spreadSheetId, List<Integer> indexes)
      throws IOException, InterruptedException {
    return new APIRequestRepeater<APIRequest<List<String>>, List<String>>() {
    }.doRepeatable(new APIRequest<List<String>>() {
      @Override
      public List<String> doRequest() throws IOException {
        Spreadsheet spreadsheet = service.spreadsheets().get(spreadSheetId).execute();
        return spreadsheet.getSheets().stream().filter(s -> indexes.contains(s.getProperties().getIndex()))
            .map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
      }

      @Override
      public String getLog() {
        return String.format("Sheet titles reading from indexes, spreadsheetId '%s'", spreadSheetId);
      }
    });
  }

  public List<String> getSheetsTitles(String spreadSheetId) throws IOException, InterruptedException {
    return new APIRequestRepeater<APIRequest<List<String>>, List<String>>() {
    }.doRepeatable(new APIRequest<List<String>>() {
      @Override
      public List<String> doRequest() throws IOException {
        Spreadsheet spreadsheet = service.spreadsheets().get(spreadSheetId).execute();
        return spreadsheet.getSheets().stream().map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
      }

      @Override
      public String getLog() {
        return String.format("Sheet titles reading, spreadsheetId '%s'", spreadSheetId);
      }
    });
  }

  public Sheet getSheetContentWithQuoteBypass(String spreadSheetId, String sheetTitle, int rowNumber,
                                              GoogleSheetsSourceConfig config,
                                              LinkedHashMap<Integer, String> resolvedHeaders,
                                              List<MetadataKeyValueAddress> metadataCoordinates)
      throws IOException, InterruptedException {
    /*return new APIRequestRepeater<SheetRequest, SheetResponse>() {
    }.doRepeatable(new SheetRequest(spreadSheetId, sheetTitle, rowNumber, config,
        resolvedHeaders, metadataCoordinates)).getSheet();*/
    return new APIRequestRepeater<APIRequest<Sheet>, Sheet>() {
    }.doRepeatable(new APIRequest<Sheet>() {

      @Override
      public Sheet doRequest() throws IOException {
        return getSheetContent(spreadSheetId, sheetTitle, rowNumber,
            config, resolvedHeaders, metadataCoordinates);
      }

      @Override
      public String getLog() {
        return String.format("Sheet reading, spreadsheetId '%s', sheet title '%s', row number '%d'",
            spreadSheetId, sheetTitle, rowNumber);
      }
    });
  }

  public Sheet getSheetContent(String spreadSheetId, String sheetTitle, int rowNumber,
                               GoogleSheetsSourceConfig config,
                               LinkedHashMap<Integer, String> resolvedHeaders,
                               List<MetadataKeyValueAddress> metadataCoordinates) throws IOException {
    String dataRange = String.format("%s!%d:%d", sheetTitle, rowNumber, rowNumber);
    String headerRange = null;
    String footerRange = null;
    if (config.isExtractMetadata()) {
      if (config.getFirstHeaderRow() != -1) {
        headerRange = String.format("%s!%d:%d", sheetTitle,
            config.getFirstHeaderRow(), config.getLastHeaderRow());
      }
      if (config.getFirstFooterRow() != -1) {
        footerRange = String.format("%s!%d:%d", sheetTitle,
            config.getFirstFooterRow(), config.getLastFooterRow());
      }
    }

    Sheets.Spreadsheets.Get request = service.spreadsheets().get(spreadSheetId);
    List<String> ranges = new ArrayList<>();
    ranges.add(dataRange);
    if (headerRange != null) {
      ranges.add(headerRange);
    }
    if (footerRange != null) {
      ranges.add(footerRange);
    }
    request.setRanges(ranges);
    request.setIncludeGridData(true);

    Spreadsheet response = request.execute();
    List<GridData> grids = response.getSheets().get(0).getData();

    Map<String, CellData> headers = new HashMap<>();
    Map<String, String> metadata = new HashMap<>();
    boolean isEmptyData = false;
    for (GridData gridData : grids) {
      List<RowData> rows = gridData.getRowData();
      int startRow = gridData.getStartRow() == null ? 0 : gridData.getStartRow();
      startRow++;
      if (startRow == rowNumber) {
        for (Map.Entry<Integer, String> headerEntry : resolvedHeaders.entrySet()) {
          int columnIndex = headerEntry.getKey();
          CellData value;
          // populate empty values if row is empty
          if (rows == null) {
            value = null;
            isEmptyData = true;
          } else {
            if (rows.size() > 1) {
              throw new RuntimeException(String.format("Excess rows during data retrieving"));
            } else {
              RowData rowData = rows.get(0);
              if (rowData == null) {
                value = null;
                isEmptyData = true;
              } else {
                List<CellData> cells = rowData.getValues();
                if (cells.size() <= columnIndex) {
                  value = null;
                } else {
                  value = cells.get(columnIndex);
                }
              }
            }
          }
          headers.put(headerEntry.getValue(), value);
        }
      } else if (startRow == config.getFirstHeaderRow() || startRow == config.getFirstFooterRow()) {
        if (CollectionUtils.isNotEmpty(rows)) {
          // retrieve header and footer metadata
          for (MetadataKeyValueAddress metadataCoordinate : metadataCoordinates) {
            CellCoordinate nameCoordinate = metadataCoordinate.getNameCoordinate();
            CellCoordinate valueCoordinate = metadataCoordinate.getValueCoordinate();
            String name = getCellValue(nameCoordinate, rows, startRow);
            String value = getCellValue(valueCoordinate, rows, startRow);
            if (StringUtils.isNotEmpty(name)) {
              metadata.put(name, value);
            }
          }
        }
      } else {
        throw new IllegalStateException(String.format("Range with invalid start row '%d'", startRow));
      }
    }

    return new Sheet(response.getProperties().getTitle(), sheetTitle, metadata, headers, isEmptyData);
  }

  private String getCellValue(CellCoordinate coordinate, List<RowData> rows, int startRow) {
    int rowIndexInList = coordinate.getRowNumber() - startRow;
    if (rows.size() > rowIndexInList && rowIndexInList >= 0) {
      RowData rowData = rows.get(rowIndexInList);
      int columnIndex = coordinate.getColumnNumber() - 1;
      if (rowData.getValues().size() > columnIndex && columnIndex >= 0) {
        return rowData.getValues().get(columnIndex).getFormattedValue();
      }
    }
    return "";
  }

  public Map<Integer, List<CellData>> getSingleRows(String spreadSheetId, String sheetTitle,
                                             List<Integer> rowNumbers) throws IOException {
    Map<Integer, List<CellData>> result = new HashMap<>();

    Sheets.Spreadsheets.Get request = service.spreadsheets().get(spreadSheetId);
    List<String> ranges = rowNumbers.stream().map(n -> String.format("%s!%2$d:%2$d", sheetTitle, n))
        .collect(Collectors.toList());
    request.setRanges(ranges);
    request.setIncludeGridData(true);
    Spreadsheet response = request.execute();
    List<GridData> grids = response.getSheets().get(0).getData();
    for (GridData gridData : grids) {
      List<RowData> rows = gridData.getRowData();
      int startRow = gridData.getStartRow() == null ? 0 : gridData.getStartRow();
      startRow++;
      for (Integer rowNumber : rowNumbers) {
        if (startRow == rowNumber) {
          if (rows == null) {
            result.put(rowNumber, null);
          } else if (rows.size() > 1) {
            throw new RuntimeException(String.format("Excess rows during data retrieving"));
          } else {
            result.put(rowNumber, rows.get(0).getValues());
          }
        }

      }
    }
    return result;
  }

  /**
   *
   */
  /*class SheetsRepeater extends APIRequestRepeater<SheetRequest, SheetResponse> {

  }*/

  /**
   *
   */
  /*class SheetRequest implements APIRequest<SheetResponse> {
    private final String spreadSheetId;
    private final String sheetTitle;
    private final int rowNumber;
    private final GoogleSheetsSourceConfig config;
    private final LinkedHashMap<Integer, String> resolvedHeaders;
    private final List<MetadataKeyValueAddress> metadataCoordinates;

    SheetRequest(String spreadSheetId, String sheetTitle, int rowNumber, GoogleSheetsSourceConfig config,
                 LinkedHashMap<Integer, String> resolvedHeaders, List<MetadataKeyValueAddress> metadataCoordinates) {
      this.spreadSheetId = spreadSheetId;
      this.sheetTitle = sheetTitle;
      this.rowNumber = rowNumber;
      this.config = config;
      this.resolvedHeaders = resolvedHeaders;
      this.metadataCoordinates = metadataCoordinates;
    }

    @Override
    public SheetResponse doRequest() throws IOException {
      return new SheetResponse(GoogleSheetsSourceClient.this.getSheetContent(spreadSheetId, sheetTitle, rowNumber,
          config, resolvedHeaders, metadataCoordinates));
    }

    @Override
    public String getLog() {
      return String.format("Resources limit exhausted during sheet reading, " +
          "wait for '%%d' seconds before next attempt, spreadsheetId '%s', sheet title '%s', row number '%d'",
          spreadSheetId, sheetTitle, rowNumber);
    }
  }

  *//**
   *
   *//*
  class SheetResponse implements APIResponse {
    private final Sheet sheet;

    SheetResponse(Sheet sheet) {
      this.sheet = sheet;
    }

    public Sheet getSheet() {
      return sheet;
    }
  }

  *//**
   *
   *//*
  class TitlesRequest implements APIRequest<List<String>> {
    private final String spreadSheetId;

    TitlesRequest(String spreadSheetId) {
      this.spreadSheetId = spreadSheetId;
    }

    @Override
    public List<String> doRequest() throws IOException {
      return getSheetsTitles(spreadSheetId);
    }

    @Override
    public String getLog() {
      return String.format("Resources limit exhausted during sheet titles reading, " +
              "wait for '%%d' seconds before next attempt, spreadsheetId '%s'",
          spreadSheetId);
    }
  }*/
}
