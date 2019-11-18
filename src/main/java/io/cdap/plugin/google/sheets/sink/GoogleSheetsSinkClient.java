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
import com.google.api.services.sheets.v4.model.AppendCellsRequest;
import com.google.api.services.sheets.v4.model.AppendDimensionRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetResponse;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.DeleteDimensionRequest;
import com.google.api.services.sheets.v4.model.DimensionRange;
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
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /*private void createFile(MultipleRowsRecord rowsRecord) throws ExecutionException, RetryException {
    String spreadsheetName = rowsRecord.getSpreadSheetName();
    String sheetTitle = rowsRecord.getSheetTitle();
    Spreadsheet spreadsheet = createEmptySpreadsheet(spreadsheetName, sheetTitle);

    String spreadSheetsId = spreadsheet.getSpreadsheetId();
    Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();

    populateCells(spreadSheetsId, sheetId, rowsRecord, true, 0);
    moveSpreadsheetToDestinationFolder(spreadSheetsId, spreadsheetName, sheetTitle);
  }*/

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

    startDeleteDimensions(spreadSheetId, sheetId, sheetTitle);
    return sheetId;
  }

  public void startDeleteDimensions(String spreadSheetId, Integer sheetId, String sheetTitle)
    throws ExecutionException, RetryException {
    APIRequestRetryer.getRetryer(config,
      String.format("Pre-removing of sheet '%s' dimensions", sheetTitle))
      .call(() -> {
        BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();

        DeleteDimensionRequest deleteDimensionRequest = new DeleteDimensionRequest();
        deleteDimensionRequest.setRange(
          new DimensionRange().setSheetId(sheetId).setDimension("ROWS").setStartIndex(0).setEndIndex(999));
        requestBody.setRequests(Collections.singletonList(new Request().setDeleteDimension(deleteDimensionRequest)));

        Sheets.Spreadsheets.BatchUpdate request =
          service.spreadsheets().batchUpdate(spreadSheetId, requestBody);

        request.execute();
        return null;
      });
  }

  /*public int populateCells(String spreadSheetsId, Integer sheetId, MultipleRowsRecord rowsRecord, boolean addHeaders,
                           int contentShift)
    throws ExecutionException, RetryException {
    Request contentRequest = prepareContentRequest(sheetId, rowsRecord);
    List<Request> mergeRequests = prepareMergeRequests(sheetId, rowsRecord, addHeaders, contentShift);

    APIRequestRetryer.getRetryer(config,
      String.format("Populating of spreadsheet '%s' with record, sheet title name '%s'.",
        rowsRecord.getSpreadSheetName(), rowsRecord.getSheetTitle()))
      .call(() -> {
        BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
        requestBody.setRequests(new ArrayList<>());

        requestBody.getRequests().add(contentRequest);
        requestBody.getRequests().addAll(mergeRequests);

        Sheets.Spreadsheets.BatchUpdate request =
          service.spreadsheets().batchUpdate(spreadSheetsId, requestBody);

        request.execute();
        return null;
      });
    return contentRequest.getAppendCells().getRows().size();
  }*/

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

  public int populateCells(String spreadSheetsId, String spreadSheetName, String sheetTitle,
                           Request contentRequest, List<Request> mergeRequests,
                           int shift)
    throws ExecutionException, RetryException {

    APIRequestRetryer.getRetryer(config,
      String.format("Populating of spreadsheet '%s' with record, sheet title name '%s', shift: %d.",
        spreadSheetName, sheetTitle, shift))
      .call(() -> {
        BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
        requestBody.setRequests(new ArrayList<>());

        requestBody.getRequests().add(contentRequest);
        requestBody.getRequests().addAll(mergeRequests);

        Sheets.Spreadsheets.BatchUpdate request =
          service.spreadsheets().batchUpdate(spreadSheetsId, requestBody);

        request.execute();
        return null;
      });
    return contentRequest.getUpdateCells().getRows().size();
  }

  public ContentRequest prepareContentRequest(Integer sheetId, List<MultipleRowsRecord> rowsRecords,
                                              boolean addHeaders, int shift) {
    UpdateCellsRequest updateCellsRequest = new UpdateCellsRequest();
    updateCellsRequest.setFields("*");
    updateCellsRequest.setStart(new GridCoordinate().setSheetId(sheetId).setColumnIndex(0).setRowIndex(shift));
    List<RowData> rows = new ArrayList<>();
    int rowsInHeader = 0;
    int rowsInRecord = rowsRecords.get(0).getSingleRowRecords().size();
    boolean headerProcessed = false;
    for (MultipleRowsRecord rowsRecord : rowsRecords) {
      if (config.isWriteSchema() && addHeaders && !headerProcessed) {
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
        rowsInHeader = headerRows.size();
        rows.addAll(headerRows);
        headerProcessed = true;
      }
      rows.addAll(rowsRecord.getSingleRowRecords().stream()
        .map(r -> new RowData().setValues(r)).collect(Collectors.toList()));
      updateCellsRequest.setRows(rows);
    }

    return new ContentRequest(new Request().setUpdateCells(updateCellsRequest), rowsRecords.size(),
      rowsInRecord, rowsInHeader, shift + updateCellsRequest.getRows().size());
  }

  public Request prepareContentRequest(Integer sheetId, MultipleRowsRecord rowsRecord) {
    AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
    appendCellsRequest.setSheetId(sheetId);
    appendCellsRequest.setFields("*");
    List<RowData> rows = new ArrayList<>();

    rows.addAll(rowsRecord.getSingleRowRecords().stream()
      .map(r -> new RowData().setValues(r)).collect(Collectors.toList()));
    appendCellsRequest.setRows(rows);
    return new Request().setAppendCells(appendCellsRequest);
  }

  /*public Request prepareContentRequest(Integer sheetId, MultipleRowsRecord rowsRecord, boolean addHeaders) {
    AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
    appendCellsRequest.setSheetId(sheetId);
    appendCellsRequest.setFields("*");
    List<RowData> rows = new ArrayList<>();
    if (config.isWriteSchema() && addHeaders) {
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
  }*/

  public List<Request> prepareMergeRequests(Integer sheetId, List<MultipleRowsRecord> rowsRecords, boolean addHeaders,
                                             int startShift) {
    // prepare header merges
    List<MergeCellsRequest> mergeRequests = new ArrayList<>();
    boolean headerProcessed = false;
    int contentShift = startShift;
    for (MultipleRowsRecord rowsRecord : rowsRecords) {
      if (config.isWriteSchema() && addHeaders && !headerProcessed) {
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
        headerProcessed = true;
      }

      // prepare content merges
      if (config.isMergeDataCells()) {
        mergeRequests.addAll(prepareMergeRequests(rowsRecord.getMergeRanges(), sheetId, contentShift));
      }
      contentShift += rowsRecord.getSingleRowRecords().size();
    }
    return mergeRequests.stream().map(r -> new Request().setMergeCells(r))
      .collect(Collectors.toList());
  }

  private void populateHeaderRows(ComplexHeader header, List<RowData> headerRows, int depth, int widthShift) {
    headerRows.get(depth).getValues().get(widthShift)
      .setUserEnteredValue(new ExtendedValue().setStringValue(header.getName()));
    // add empty cells to this level
    /*for (int i = 0; i < header.getWidth() - 1; i++) {
      headerRows.get(depth).getValues().add(new CellData());
    }*/
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

  /**
   *
   */
  public class ContentRequest {
    private final Request contentRequest;
    private final int numberOfRequests;
    private final int rowsInRequest;
    private final int rowsInHeader;
    private final int lastRowIndex;

    public ContentRequest(Request contentRequest, int numberOfRequests, int rowsInRequest, int rowsInHeader,
                          int lastRowIndex) {
      this.contentRequest = contentRequest;
      this.numberOfRequests = numberOfRequests;
      this.rowsInRequest = rowsInRequest;
      this.rowsInHeader = rowsInHeader;
      this.lastRowIndex = lastRowIndex;
    }

    public Request getContentRequest() {
      return contentRequest;
    }

    public int getNumberOfRequests() {
      return numberOfRequests;
    }

    public int getRowsInRequest() {
      return rowsInRequest;
    }

    public int getRowsInHeader() {
      return rowsInHeader;
    }

    public int getLastRowIndex() {
      return lastRowIndex;
    }
  }
}
