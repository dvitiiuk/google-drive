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

import com.github.rholder.retry.RetryException;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendCellsRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.MergeCellsRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import io.cdap.plugin.google.common.APIRequestRepeater;
import io.cdap.plugin.google.sheets.common.GoogleSheetsClient;
import io.cdap.plugin.google.sheets.common.MultipleRowsRecord;
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Client for writing data via Google Drive API.
 */
public class GoogleSheetsSinkClient extends GoogleSheetsClient<GoogleSheetsSinkConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSheetsSinkClient.class);

  public GoogleSheetsSinkClient(GoogleSheetsSinkConfig config) {
    super(config);
  }

  public void createFile(MultipleRowsRecord rowsRecord) throws ExecutionException, RetryException {
    String spreadsheetName = rowsRecord.getSpreadSheetName();
    String sheetTitle = rowsRecord.getSheetTitle();
    Spreadsheet spreadsheet = (Spreadsheet) APIRequestRepeater.getRetryer(config,
        String.format("Creation of empty spreadsheet, name: '%s', sheet title: '%s'.",
            spreadsheetName, sheetTitle))
        .call(() ->
            createEmptySpreadsheet(spreadsheetName, sheetTitle)
        );

    String spreadSheetsId = spreadsheet.getSpreadsheetId();
    Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();

    APIRequestRepeater.getRetryer(config,
        String.format("Populating of spreadsheet '%s' with record, sheet title name '%s'.",
            spreadsheetName, sheetTitle))
        .call(() -> {
          populateCells(spreadSheetsId, sheetId, rowsRecord);
          return null;
        });

    APIRequestRepeater.getRetryer(config,
        String.format("Moving the spreadsheet '%s' to destination folder.",
            spreadsheetName, sheetTitle))
        .call(() -> {
          moveSpreadsheetToDestinationFolder(spreadSheetsId);
          return null;
        });
  }

  public Spreadsheet createEmptySpreadsheet(String spreadsheetName, String sheetTitle)
      throws IOException {

    Spreadsheet spreadsheet = new Spreadsheet();

    SpreadsheetProperties spreadsheetProperties = new SpreadsheetProperties();
    spreadsheetProperties.setTitle(spreadsheetName);

    com.google.api.services.sheets.v4.model.Sheet sheetToPast = new com.google.api.services.sheets.v4.model.Sheet();
    sheetToPast.setProperties(new SheetProperties().setTitle(sheetTitle));

    spreadsheet.setProperties(spreadsheetProperties);
    spreadsheet.setSheets(Collections.singletonList(sheetToPast));
    spreadsheet = service.spreadsheets().create(spreadsheet).execute();

    return spreadsheet;
  }

  public void populateCells(String spreadSheetsId, Integer sheetId, MultipleRowsRecord rowsRecord)
      throws IOException {

    BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
    requestBody.setRequests(new ArrayList<>());

    requestBody.getRequests().add(prepareContentRequest(sheetId, rowsRecord));
    requestBody.getRequests().addAll(prepareMergeRequests(sheetId, rowsRecord));

    Sheets.Spreadsheets.BatchUpdate request =
        service.spreadsheets().batchUpdate(spreadSheetsId, requestBody);

    request.execute();
  }

  private Request prepareContentRequest(Integer sheetId, MultipleRowsRecord rowsRecord) {
    AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
    appendCellsRequest.setSheetId(sheetId);
    appendCellsRequest.setFields("*");
    List<RowData> rows = new ArrayList<>();
    if (config.isWriteSchema()) {
      // root header is not displayed
      int headersDepth = rowsRecord.getHeader().getDepth() - 1;
      int headersWidth = rowsRecord.getHeader().getWidth();
      List<ComplexHeader> realHeaders = rowsRecord.getHeader().getSubHeaders();
      List<RowData> headerRows = new ArrayList<>();
      for (int rowIndex = 0; rowIndex < headersDepth; rowIndex++) {
        List<CellData> emptyCells = new ArrayList<>();
        for (int columnIndex = 0; columnIndex < headersWidth; columnIndex++) {
          emptyCells.add(new CellData());
        }
        headerRows.add(new RowData().setValues(emptyCells));
      }
      int widthShift = 0;
      for (int headerIndex = 0; headerIndex < realHeaders.size(); headerIndex++) {
        ComplexHeader header = realHeaders.get(headerIndex);
        populateHeaderRows(header, headerRows, 0, widthShift);
        widthShift += header.getWidth();
      }

      rows.addAll(headerRows);
    }
    rows.addAll(rowsRecord.getSingleRowRecords().stream()
        .map(r -> new RowData().setValues(r)).collect(Collectors.toList()));
    appendCellsRequest.setRows(rows);
    return new Request().setAppendCells(appendCellsRequest);
  }

  private List<Request> prepareMergeRequests(Integer sheetId, MultipleRowsRecord rowsRecord) {
    // prepare header merges
    List<MergeCellsRequest> mergeRequests = new ArrayList<>();
    int headersDepth = 0;
    if (config.isWriteSchema()) {
      // root header is not displayed
      headersDepth = rowsRecord.getHeader().getDepth() - 1;
      if (headersDepth > 1) {
        List<ComplexHeader> realHeaders = rowsRecord.getHeader().getSubHeaders();

        int widthShift = 0;
        List<GridRange> headerRanges = new ArrayList<>();
        for (int headerIndex = 0; headerIndex < realHeaders.size(); headerIndex++) {
          ComplexHeader header = realHeaders.get(headerIndex);
          calcHeaderMerges(header, headerRanges, 0, headersDepth, widthShift);
          widthShift += header.getWidth();
        }
        mergeRequests.addAll(prepareMergeRequests(headerRanges, sheetId, 0));
      }
    }

    // prepare content merges
    if (config.isMergeDataCells()) {
      mergeRequests.addAll(prepareMergeRequests(rowsRecord.getMergeRanges(), sheetId, headersDepth));
    }
    return mergeRequests.stream().map(r -> new Request().setMergeCells(r))
        .collect(Collectors.toList());
  }

  private void populateHeaderRows(ComplexHeader header, List<RowData> headerRows, int depth, int widthShift) {
    headerRows.get(depth).getValues().get(widthShift)
        .setUserEnteredValue(new ExtendedValue().setStringValue(header.getName()));
    // add empty cells to this level
    for (int i = 0; i < header.getWidth() - 1; i++) {
      headerRows.get(depth).getValues().add(new CellData());
    }
    int widthSubShift = widthShift;
    for (ComplexHeader subHeader : header.getSubHeaders()) {
      populateHeaderRows(subHeader, headerRows, depth + 1, widthSubShift);
      widthSubShift += subHeader.getWidth();
    }
  }

  private void calcHeaderMerges(ComplexHeader header, List<GridRange> headerRanges, int depth, int headersDepth,
                                int widthShift) {
    if (CollectionUtils.isEmpty(header.getSubHeaders())) {
      if (depth + 1 < headersDepth) {
        // add vertical merging
        headerRanges.add(new GridRange().setStartRowIndex(depth).setEndRowIndex(headersDepth)
            .setStartColumnIndex(widthShift).setEndColumnIndex(widthShift + 1));
      }
    } else {
      // add horizontal merging
      headerRanges.add(new GridRange().setStartRowIndex(depth).setEndRowIndex(depth + 1)
          .setStartColumnIndex(widthShift).setEndColumnIndex(widthShift + header.getWidth()));

      int widthSubShift = widthShift;
      for (ComplexHeader subHeader : header.getSubHeaders()) {
        calcHeaderMerges(subHeader, headerRanges, depth + 1, headersDepth, widthSubShift);
        widthSubShift += subHeader.getWidth();
      }
    }
  }

  private List<MergeCellsRequest> prepareMergeRequests(List<GridRange> ranges, int sheetId, int rowsShift) {
    return ranges.stream()
        .filter(r -> r.getStartRowIndex() != r.getEndRowIndex() || r.getStartColumnIndex() != r.getEndColumnIndex())
        .map(r -> new MergeCellsRequest().setMergeType("MERGE_ALL").setRange(r
            .setSheetId(sheetId)
            .setStartRowIndex(r.getStartRowIndex() + rowsShift)
            .setEndRowIndex(r.getEndRowIndex() + rowsShift)))
        .collect(Collectors.toList());
  }

  public void moveSpreadsheetToDestinationFolder(String spreadSheetsId) throws IOException {
    drive.files().update(spreadSheetsId, null)
        .setAddParents(config.getDirectoryIdentifier())
        .setRemoveParents("root")
        .setFields("id, parents")
        .execute();

    /*if (new Random().nextInt(100) >= 1) {
      drive.files().delete(spreadSheetsId).execute();
    }*/
  }

  @Override
  protected List<String> getRequiredScopes() {
    return Arrays.asList(SheetsScopes.SPREADSHEETS, DriveScopes.DRIVE);
  }
}
