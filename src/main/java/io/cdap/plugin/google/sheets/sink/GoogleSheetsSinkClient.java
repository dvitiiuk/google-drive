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
import io.cdap.plugin.google.sheets.common.RowRecord;
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

  public void createFile(RowRecord rowRecord) throws ExecutionException, RetryException {
    Spreadsheet spreadsheet = (Spreadsheet) APIRequestRepeater.getRetryer(config,
        String.format("Creation of empty spreadsheet, name: '%s', sheet title: '%s'.",
            rowRecord.getSpreadSheetName(), rowRecord.getSheetTitle()))
        .call(() ->
            createEmptySpreadsheet(rowRecord.getSpreadSheetName(), rowRecord.getSheetTitle())
        );

    String spreadSheetsId = spreadsheet.getSpreadsheetId();
    Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();

    APIRequestRepeater.getRetryer(config,
        String.format("Populating of spreadsheet '%s' with record, sheet title name '%s'.",
            rowRecord.getSpreadSheetName(), rowRecord.getSheetTitle()))
        .call(() -> {
          populateCells(spreadSheetsId, sheetId, rowRecord);
          return null;
        });

    APIRequestRepeater.getRetryer(config,
        String.format("Moving the spreadsheet '%s' to destination folder.",
            rowRecord.getSpreadSheetName(), rowRecord.getSheetTitle()))
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

  public void populateCells(String spreadSheetsId, Integer sheetId, RowRecord rowRecord)
      throws IOException {
    //pr(spreadSheetsId, sheetId);
    AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
    appendCellsRequest.setSheetId(sheetId);
    appendCellsRequest.setFields("*");
    List<RowData> rows = new ArrayList<>();
    if (config.isWriteSchema()) {
      if (rowRecord == null) {
        LOG.error("RowRecord is null");
      }
      if (rowRecord.getHeaderedCells() == null) {
        LOG.error("Headered cells are null");
      }
      if (rowRecord.getHeaderedCells().size() == 0) {
        LOG.error("Headered cells are empty");
      }
      List<CellData> headerCells = rowRecord.getHeaderedCells().keySet().stream()
          .map(h -> new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(h)))
          .collect(Collectors.toList());
      rows.add(new RowData().setValues(headerCells));
    }
    rows.add(new RowData().setValues(rowRecord.getHeaderedCells().values().stream().collect(Collectors.toList())));
    appendCellsRequest.setRows(rows);

    BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
    requestBody.setRequests(Collections.singletonList(new Request()
        .setAppendCells(appendCellsRequest)));

    Sheets.Spreadsheets.BatchUpdate request =
        service.spreadsheets().batchUpdate(spreadSheetsId, requestBody);

    request.execute();


    pr(spreadSheetsId, sheetId);
  }

  void pr(String spreadSheetsId, Integer sheetId) throws IOException {
    BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
    MergeCellsRequest mergeCellsRequest = new MergeCellsRequest();
    mergeCellsRequest.setMergeType("MERGE_COLUMNS");
    mergeCellsRequest.setRange(new GridRange()
        .setSheetId(sheetId)
        .setStartColumnIndex(3)
        .setEndColumnIndex(5)
        .setStartRowIndex(0)
        .setEndRowIndex(5));
    requestBody.setRequests(Collections.singletonList(new Request()
        .setMergeCells(mergeCellsRequest)));
    Sheets.Spreadsheets.BatchUpdate request =
        service.spreadsheets().batchUpdate(spreadSheetsId, requestBody);
    request.execute();
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
