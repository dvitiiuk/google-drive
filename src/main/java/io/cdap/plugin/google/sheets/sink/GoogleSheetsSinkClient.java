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
import com.github.rholder.retry.Retryer;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AddSheetRequest;
import com.google.api.services.sheets.v4.model.AppendDimensionRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetResponse;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridCoordinate;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.MergeCellsRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.UpdateCellsRequest;
import io.cdap.plugin.google.common.APIRequestRetryer;
import io.cdap.plugin.google.sheets.common.GoogleSheetsClient;
import io.cdap.plugin.google.sheets.common.MultipleRowsRecord;
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;
import io.cdap.plugin.google.sheets.sink.utils.FlatternedRecordRequest;
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

  public GoogleSheetsSinkClient(GoogleSheetsSinkConfig config) throws IOException {
    super(config);
  }

  public Spreadsheet createEmptySpreadsheet(String spreadsheetName, String sheetTitle)
    throws ExecutionException, RetryException {
    Retryer<Spreadsheet> createSpreadsheetRetryer = APIRequestRetryer.getRetryer(config,
      String.format("Creation of empty spreadsheet, name: '%s', sheet title: '%s'.",
        spreadsheetName, sheetTitle));
    return createSpreadsheetRetryer.call(() -> {
      Spreadsheet spreadsheet = new Spreadsheet();

      SpreadsheetProperties spreadsheetProperties = new SpreadsheetProperties();
      spreadsheetProperties.setTitle(spreadsheetName);

      com.google.api.services.sheets.v4.model.Sheet sheetToPast = new com.google.api.services.sheets.v4.model.Sheet();
      sheetToPast.setProperties(new SheetProperties().setTitle(sheetTitle));

      spreadsheet.setProperties(spreadsheetProperties);
      spreadsheet.setSheets(Collections.singletonList(sheetToPast));
      spreadsheet = service.spreadsheets().create(spreadsheet).execute();

      return spreadsheet;
    });
  }

  public Integer createSheet(String spreadSheetId, String spreadSheetName, String sheetTitle)
    throws ExecutionException, RetryException {
    Retryer<Integer> createSheetRetryer = APIRequestRetryer.getRetryer(config,
      String.format("Creation of empty sheet, spreadsheet name: '%s', sheet title: '%s'.",
        spreadSheetName, sheetTitle));
    Integer sheetId = createSheetRetryer.call(() -> {
      BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();

      AddSheetRequest addSheetRequest = new AddSheetRequest();
      addSheetRequest.setProperties(new SheetProperties().setTitle(sheetTitle));
      requestBody.setRequests(Collections.singletonList(new Request().setAddSheet(addSheetRequest)));

      Sheets.Spreadsheets.BatchUpdate request =
        service.spreadsheets().batchUpdate(spreadSheetId, requestBody);

      BatchUpdateSpreadsheetResponse response = request.execute();

      return response.getReplies().get(0).getAddSheet().getProperties().getSheetId();
    });
    return sheetId;
  }

  public void extendDimension(String spreadSheetsId, String spreadSheetsName, String sheetTitle,
                              int sheetId, int rowsToAdd) throws ExecutionException, RetryException {
    if (rowsToAdd <= 0) {
      return;
    }
    AppendDimensionRequest appendDimensionRequest = new AppendDimensionRequest();
    appendDimensionRequest.setSheetId(sheetId);
    appendDimensionRequest.setDimension("ROWS");
    appendDimensionRequest.setLength(rowsToAdd);
    Request appendRequest = new Request().setAppendDimension(appendDimensionRequest);

    APIRequestRetryer.getRetryer(config,
      String.format("Appending dimension of '%d' rows for spreadsheet '%s', sheet name '%s'.",
        rowsToAdd, spreadSheetsName, sheetTitle))
      .call(() -> {
        BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
        requestBody.setRequests(Collections.singletonList(appendRequest));

        Sheets.Spreadsheets.BatchUpdate request =
          service.spreadsheets().batchUpdate(spreadSheetsId, requestBody);

        request.execute();
        return null;
      });
  }

  public void populateCells(String spreadSheetsId, String spreadSheetName, List<String> sheetTitles,
                           List<Request> contentRequests, List<Request> mergeRequests)
    throws ExecutionException, RetryException {

    APIRequestRetryer.getRetryer(config,
      String.format("Populating of spreadsheet '%s' with records, sheet title names '%s'.",
        spreadSheetName, sheetTitles.toString()))
      .call(() -> {
        BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
        requestBody.setRequests(new ArrayList<>());

        requestBody.getRequests().addAll(contentRequests);
        requestBody.getRequests().addAll(mergeRequests);

        Sheets.Spreadsheets.BatchUpdate request =
          service.spreadsheets().batchUpdate(spreadSheetsId, requestBody);

        request.execute();
        return null;
      });
  }

  public FlatternedRecordRequest prepareFlatternedRequest(Integer sheetId, MultipleRowsRecord record,
                                                     boolean addHeaders, int shift) {
    UpdateCellsRequest updateCellsRequest = new UpdateCellsRequest();
    updateCellsRequest.setFields("*");
    updateCellsRequest.setStart(new GridCoordinate().setSheetId(sheetId).setColumnIndex(0).setRowIndex(shift));
    List<RowData> rows = new ArrayList<>();
    int rowsInHeader = 0;
    int rowsInRecord = record.getSingleRowRecords().size();
    if (config.isWriteSchema() && addHeaders) {
      // root header is not displayed
      int headersDepth = record.getHeader().getDepth() - 1;
      int headersWidth = record.getHeader().getWidth();
      List<ComplexHeader> realHeaders = record.getHeader().getSubHeaders();
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
      rowsInHeader = headerRows.size();
      rows.addAll(headerRows);
    }

    rows.addAll(record.getSingleRowRecords().stream()
      .map(r -> new RowData().setValues(r)).collect(Collectors.toList()));
    updateCellsRequest.setRows(rows);

    return new FlatternedRecordRequest(new Request().setUpdateCells(updateCellsRequest),
      prepareMergeRequests(sheetId, record, addHeaders, shift), sheetId, rowsInRecord,
      rowsInHeader, shift + updateCellsRequest.getRows().size());
  }

  public List<Request> prepareMergeRequests(Integer sheetId, MultipleRowsRecord rowsRecord, boolean addHeaders,
                                            int startShift) {
    // prepare header merges
    List<MergeCellsRequest> mergeRequests = new ArrayList<>();
    int contentShift = startShift;
    if (config.isWriteSchema() && addHeaders) {
      // root header is not displayed
      int headersDepth = rowsRecord.getHeader().getDepth() - 1;
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
      contentShift += headersDepth;
    }

    // prepare content merges
    if (config.isMergeDataCells()) {
      mergeRequests.addAll(prepareMergeRequests(rowsRecord.getMergeRanges(), sheetId, contentShift));
    }
    return mergeRequests.stream().map(r -> new Request().setMergeCells(r))
      .collect(Collectors.toList());
  }

  private void populateHeaderRows(ComplexHeader header, List<RowData> headerRows, int depth, int widthShift) {
    headerRows.get(depth).getValues().get(widthShift)
      .setUserEnteredValue(new ExtendedValue().setStringValue(header.getName()));
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

  public void moveSpreadsheetToDestinationFolder(String spreadSheetsId, String spreadsheetName, String sheetTitle)
    throws ExecutionException, RetryException {
    APIRequestRetryer.getRetryer(config,
      String.format("Moving the spreadsheet '%s' to destination folder.", spreadsheetName, sheetTitle))
      .call(() -> {
        drive.files().update(spreadSheetsId, null)
          .setAddParents(config.getDirectoryIdentifier())
          .setRemoveParents("root")
          .setFields("id, parents")
          .execute();
        return null;
      });

    /*if (new Random().nextInt(100) >= 1) {
      drive.files().delete(spreadSheetsId).execute();
    }*/
  }

  @Override
  protected List<String> getRequiredScopes() {
    return Arrays.asList(SheetsScopes.SPREADSHEETS, DriveScopes.DRIVE);
  }
}
