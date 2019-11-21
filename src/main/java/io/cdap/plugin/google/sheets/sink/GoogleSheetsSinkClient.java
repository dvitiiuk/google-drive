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
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;
import io.cdap.plugin.google.sheets.sink.utils.DimensionType;
import io.cdap.plugin.google.sheets.sink.utils.FlatternedRowsRecord;
import io.cdap.plugin.google.sheets.sink.utils.FlatternedRowsRequest;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Client for writing data via Google Sheets API.
 */
public class GoogleSheetsSinkClient extends GoogleSheetsClient<GoogleSheetsSinkConfig> {

  public GoogleSheetsSinkClient(GoogleSheetsSinkConfig config) throws IOException {
    super(config);
  }

  /**
   * Method to create spreadSheet with single empty sheet.
   * @param spreadsheetName name of spreadSheet.
   * @param sheetTitle name of sheet.
   * @return created spreadSheet.
   * @throws ExecutionException when Sheets API threw some not repeatable exception.
   * @throws RetryException when API retry count was exceeded.
   */
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

  /**
   * Method to create new empty sheet for existing spreadSheet.
   * @param spreadSheetId ID of the target spreadSheet file.
   * @param spreadSheetName name of the target spreadSheet file.
   * @param sheetTitle name of the new sheet.
   * @return ID of created sheet.
   * @throws ExecutionException when Sheets API threw some not repeatable exception.
   * @throws RetryException when API retry count was exceeded.
   */
  public SheetProperties createEmptySheet(String spreadSheetId, String spreadSheetName, String sheetTitle)
    throws ExecutionException, RetryException {
    Retryer<SheetProperties> createSheetRetryer = APIRequestRetryer.getRetryer(config,
      String.format("Creation of empty sheet, spreadsheet name: '%s', sheet title: '%s'.",
        spreadSheetName, sheetTitle));
    return createSheetRetryer.call(() -> {
      BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();

      AddSheetRequest addSheetRequest = new AddSheetRequest();
      addSheetRequest.setProperties(new SheetProperties().setTitle(sheetTitle));
      requestBody.setRequests(Collections.singletonList(new Request().setAddSheet(addSheetRequest)));

      Sheets.Spreadsheets.BatchUpdate request =
        service.spreadsheets().batchUpdate(spreadSheetId, requestBody);

      BatchUpdateSpreadsheetResponse response = request.execute();

      return response.getReplies().get(0).getAddSheet().getProperties();
    });
  }

  /**
   * Method to extend existing sheet with new rows or columns.
   * @param spreadSheetsId ID of the target spreadSheet file.
   * @param spreadSheetsName name of the target spreadSheet file.
   * @param sheetTitle name of the target sheet.
   * @param sheetId ID of the target sheet.
   * @param rowsToAdd number of rows to add.
   * @param dimensionType defines the dimension of extension.
   * @throws ExecutionException when Sheets API threw some not repeatable exception.
   * @throws RetryException when API retry count was exceeded.
   */
  public void extendDimension(String spreadSheetsId, String spreadSheetsName, String sheetTitle,
                              int sheetId, int rowsToAdd, DimensionType dimensionType)
    throws ExecutionException, RetryException {
    if (rowsToAdd <= 0) {
      return;
    }
    AppendDimensionRequest appendDimensionRequest = new AppendDimensionRequest();
    appendDimensionRequest.setSheetId(sheetId);
    appendDimensionRequest.setDimension(dimensionType.getValue());
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

  /**
   * Method that units and executes content and merge requests with single Sheets API request.
   * Requests may relate to separate sheets.
   * @param spreadSheetsId ID of the target spreadSheet file.
   * @param spreadSheetName name of the target spreadSheet file.
   * @param sheetTitles name of the target sheet.
   * @param contentRequests list of content requests.
   * @param mergeRequests list of merge requests.
   * @throws ExecutionException when Sheets API threw some not repeatable exception.
   * @throws RetryException when API retry count was exceeded.
   */
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

  /**
   * Method that creates API requests for flatterned rows record.
   * @param sheetId ID of the target sheet.
   * @param record flatterned rows record.
   * @param shift index of the start row.
   * @return requests ready for sending to Sheets API.
   */
  public FlatternedRowsRequest prepareFlatternedRequest(Integer sheetId, FlatternedRowsRecord record, int shift) {
    UpdateCellsRequest updateCellsRequest = new UpdateCellsRequest();
    updateCellsRequest.setFields("*");
    updateCellsRequest.setStart(new GridCoordinate().setSheetId(sheetId).setColumnIndex(0).setRowIndex(shift));
    List<RowData> rows = new ArrayList<>();
    if (config.isWriteSchema() && shift == 0) {
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
      rows.addAll(headerRows);
    }

    rows.addAll(record.getSingleRowRecords().stream()
      .map(r -> new RowData().setValues(r)).collect(Collectors.toList()));
    updateCellsRequest.setRows(rows);

    return new FlatternedRowsRequest(new Request().setUpdateCells(updateCellsRequest),
      prepareMergeRequests(sheetId, record, shift),
      shift + updateCellsRequest.getRows().size());
  }

  /**
   * Method to prepare merge requests (content and header) for flatterned rows record.
   * @param sheetId ID of the target sheet.
   * @param record flatterned rows record.
   * @param startShift index of the start row.
   * @return list of merge requests.
   */
  private List<Request> prepareMergeRequests(Integer sheetId, FlatternedRowsRecord record, int startShift) {
    // prepare header merges
    List<MergeCellsRequest> mergeRequests = new ArrayList<>();
    int contentShift = startShift;
    if (config.isWriteSchema() && startShift == 0) {
      // root header is not displayed
      int headersDepth = record.getHeader().getDepth() - 1;
      if (headersDepth > 1) {
        List<ComplexHeader> realHeaders = record.getHeader().getSubHeaders();

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
      mergeRequests.addAll(prepareMergeRequests(record.getMergeRanges(), sheetId, contentShift));
    }
    return mergeRequests.stream().map(r -> new Request().setMergeCells(r))
      .collect(Collectors.toList());
  }

  /**
   * Method to populate header cells.
   * @param header  complex nested header.
   * @param headerRows list of the rows to extend.
   * @param depth level of depth of complex header to process.
   * @param widthShift width of initial complex header.
   */
  private void populateHeaderRows(ComplexHeader header, List<RowData> headerRows, int depth, int widthShift) {
    headerRows.get(depth).getValues().get(widthShift)
      .setUserEnteredValue(new ExtendedValue().setStringValue(header.getName()));
    int widthSubShift = widthShift;
    for (ComplexHeader subHeader : header.getSubHeaders()) {
      populateHeaderRows(subHeader, headerRows, depth + 1, widthSubShift);
      widthSubShift += subHeader.getWidth();
    }
  }

  /**
   * Method to generate relative merge ranges for headers.
   * @param header complex nested header.
   * @param headerRanges merge ranges to be extended with news.
   * @param depth level of depth of complex header to process.
   * @param headersDepth depth of initial complex header.
   * @param widthShift index of column to start from.
   */
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

  /**
   * Method to convert relative merge coordinated to absolute with use of shift value.
   * @param ranges merge ranges with relative coordinates.
   * @param sheetId target sheet ID.
   * @param rowsShift shift of ranges from the sheet start.
   * @return merge requests with absolute coordinates.
   */
  private List<MergeCellsRequest> prepareMergeRequests(List<GridRange> ranges, int sheetId, int rowsShift) {
    return ranges.stream()
      .filter(r -> r.getStartRowIndex() != r.getEndRowIndex() || r.getStartColumnIndex() != r.getEndColumnIndex())
      .map(r -> new MergeCellsRequest().setMergeType("MERGE_ALL").setRange(r
        .setSheetId(sheetId)
        .setStartRowIndex(r.getStartRowIndex() + rowsShift)
        .setEndRowIndex(r.getEndRowIndex() + rowsShift)))
      .collect(Collectors.toList());
  }

  /**
   * Method to move file from root to destination folder.
   * @param spreadSheetsId spreadSheet file id.
   * @param spreadsheetName spreadSheet name.
   * @throws ExecutionException when Sheets API threw some not repeatable exception.
   * @throws RetryException when API retry count was exceeded.
   */
  public void moveSpreadsheetToDestinationFolder(String spreadSheetsId, String spreadsheetName)
    throws ExecutionException, RetryException {
    APIRequestRetryer.getRetryer(config,
      String.format("Moving the spreadsheet '%s' to destination folder.", spreadsheetName))
      .call(() -> {
        drive.files().update(spreadSheetsId, null)
          .setAddParents(config.getDirectoryIdentifier())
          .setRemoveParents("root")
          .setFields("id, parents")
          .execute();
        return null;
      });
  }

  @Override
  protected List<String> getRequiredScopes() {
    return Arrays.asList(SheetsScopes.SPREADSHEETS, DriveScopes.DRIVE);
  }
}
