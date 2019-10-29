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
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.GridData;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.cdap.plugin.google.sheets.common.GoogleSheetsClient;
import io.cdap.plugin.google.sheets.common.Sheet;
import io.cdap.plugin.google.sheets.source.utils.CellCoordinate;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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

  public GoogleSheetsSourceClient(GoogleSheetsSourceConfig config) {
    super(config);
  }

  @Override
  protected String getRequiredScope() {
    return READONLY_PERMISSIONS_SCOPE;
  }

  public List<com.google.api.services.sheets.v4.model.Sheet> getSheets(String spreadSheetId) throws IOException {
    Spreadsheet spreadsheet = service.spreadsheets().get(spreadSheetId).execute();
    return spreadsheet.getSheets();
  }

  public List<String> getSheetsTitles(String spreadSheetId, List<Integer> indexes) throws IOException {
    Spreadsheet spreadsheet = service.spreadsheets().get(spreadSheetId).execute();
    return spreadsheet.getSheets().stream().filter(s -> indexes.contains(s.getProperties().getIndex()))
        .map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
  }

  public List<String> getSheetsTitles(String spreadSheetId) throws IOException {
    Spreadsheet spreadsheet = service.spreadsheets().get(spreadSheetId).execute();
    return spreadsheet.getSheets().stream().map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
  }

  public Sheet getSheetContent(String spreadSheetId, String sheetTitle, int rowNumber,
                               GoogleSheetsSourceConfig config,
                               LinkedHashMap<Integer, String> resolvedHeaders,
                               List<MetadataKeyValueAddress> metadataCoordinates) throws IOException {
    int firstDataRow;
    int lastDataRow;
    if (rowNumber != -1) {
      firstDataRow = rowNumber + 1;
      lastDataRow = rowNumber + 1;
    } else {
      firstDataRow = config.getActualFirstDataRow() + 1;
      lastDataRow = config.getActualLastDataRow() + 1;
    }
    String dataRange = String.format("%s!%d:%d", sheetTitle, firstDataRow, lastDataRow);
    String headerRange = null;
    String footerRange = null;
    if (config.isExtractMetadata()) {
      if (config.getFirstHeaderRow() != -1) {
        headerRange = String.format("%s!%d:%d", sheetTitle,
            config.getFirstHeaderRow() + 1, config.getLastHeaderRow() + 1);
      }
      if (config.getFirstFooterRow() != -1) {
        footerRange = String.format("%s!%d:%d", sheetTitle,
            config.getFirstFooterRow() + 1, config.getLastFooterRow() + 1);
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

    Map<String, List<String>> headers = new HashMap<>();
    Map<String, String> metadata = new HashMap<>();
    boolean isEmptyData = false;
    for (GridData gridData : grids) {
      List<RowData> rows = gridData.getRowData();
      int startRow = gridData.getStartRow() == null ? 0 : gridData.getStartRow();
      if (startRow == firstDataRow - 1) {


        for (Map.Entry<Integer, String> headerEntry : resolvedHeaders.entrySet()) {
          int columnIndex = headerEntry.getKey();
          List<String> values = new ArrayList<>();
          // populate empty values if row is empty
          if (rows == null) {
            for (int i = firstDataRow; i <= lastDataRow; i++) {
              values.add(null);
              isEmptyData = true;
            }
          } else {
            for (RowData rowData : rows) {
              if (rowData == null) {
                values.add(null);
              } else {
                List<CellData> cells = rowData.getValues();
                if (cells.size() <= columnIndex) {
                  values.add(null);
                } else {
                  values.add(cells.get(columnIndex).getFormattedValue());
                }
              }
            }
          }
          headers.put(headerEntry.getValue(), values);
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
    int rowIndexInList = coordinate.getRowNumber() - startRow - 1;
    if (rows.size() > rowIndexInList && rowIndexInList >= 0) {
      RowData rowData = rows.get(rowIndexInList);
      if (rowData.getValues().size() > coordinate.getColumnNumber() - 1) {
        return rowData.getValues().get(coordinate.getColumnNumber() - 1).getFormattedValue();
      }
    }
    return "";
  }

  public List<Object> getRow(String spreadSheetId, String sheetTitle, int headerRowNumber) throws IOException {
    Sheets.Spreadsheets.Values.Get request = service.spreadsheets().values().get(spreadSheetId,
        String.format("%s!%2$d:%2$d", sheetTitle, headerRowNumber));
    request.setValueRenderOption("FORMULA");
    request.setDateTimeRenderOption("SERIAL_NUMBER");
    ValueRange range = request.execute();
    if (CollectionUtils.isEmpty(range.getValues())) {
      return Collections.emptyList();
    } else {
      return range.getValues().get(0);
    }
  }

  interface Formatter {
    String format(CellData cellData);
  }

  class NoFormatter implements Formatter {

    @Override
    public String format(CellData cellData) {
      if (cellData.getEffectiveValue().getStringValue() != null) {
        return cellData.getEffectiveValue().getStringValue();
      } else if (cellData.getEffectiveValue().getNumberValue() != null) {
        return cellData.getEffectiveValue().getNumberValue().toString();
      }
      return cellData.getEffectiveValue().getStringValue();
    }
  }
}
