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

import com.google.api.services.drive.DriveScopes;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendCellsRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import io.cdap.plugin.google.common.APIRequest;
import io.cdap.plugin.google.common.APIRequestRepeater;
import io.cdap.plugin.google.sheets.common.GoogleSheetsClient;
import io.cdap.plugin.google.sheets.common.Sheet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Client for writing data via Google Drive API.
 */
public class GoogleSheetsSinkClient extends GoogleSheetsClient<GoogleSheetsSinkConfig> {

  public GoogleSheetsSinkClient(GoogleSheetsSinkConfig config) {
    super(config);
  }

  /*public void createFileRetrieble(Sheet sheet) throws IOException, InterruptedException {
    new SheetWriter().doRepeatable(new WriteSheetRequest(sheet));
  }*/

  public void createFile(Sheet sheet) throws IOException, InterruptedException {
    Spreadsheet spreadsheet = createEmptySpreadsheet(sheet.getSpreadSheetName(), sheet.getSheetTitle());

    String spreadSheetsId = spreadsheet.getSpreadsheetId();
    Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();

    populateCells(spreadSheetsId, sheetId, sheet);

    drive.files().update(spreadSheetsId, null)
      .setAddParents(config.getDirectoryIdentifier())
      .setRemoveParents("root")
      .setFields("id, parents")
      .execute();
  }

  private Spreadsheet createEmptySpreadsheet(String spreadsheetName, String sheetTitle)
      throws IOException, InterruptedException {
    return new APIRequestRepeater<APIRequest<Spreadsheet>, Spreadsheet>() {
    }.doRepeatable(new APIRequest<Spreadsheet>() {
      @Override
      public Spreadsheet doRequest() throws IOException {
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

      @Override
      public String getLog() {
        return null;
      }
    });
  }

  private void populateCells(String spreadSheetsId, Integer sheetId, Sheet sheet)
      throws IOException, InterruptedException {
    new APIRequestRepeater<APIRequest<Void>, Void>() {
    }.doRepeatable(new APIRequest<Void>() {
      @Override
      public Void doRequest() throws IOException {
        AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
        appendCellsRequest.setSheetId(sheetId);
        appendCellsRequest.setFields("*");
        List<RowData> rows = new ArrayList<>();
        if (config.isWriteSchema()) {
          List<CellData> headerCells = sheet.getHeaderedValues().keySet().stream()
              .map(h -> new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(h)))
              .collect(Collectors.toList());
          rows.add(new RowData().setValues(headerCells));
        }
        rows.add(new RowData().setValues(sheet.getHeaderedValues().values().stream().collect(Collectors.toList())));
        appendCellsRequest.setRows(rows);

        BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
        requestBody.setRequests(Collections.singletonList(new Request().setAppendCells(appendCellsRequest)));

        Sheets.Spreadsheets.BatchUpdate request =
            service.spreadsheets().batchUpdate(spreadSheetsId, requestBody);

        request.execute();
        return null;
      }

      @Override
      public String getLog() {
        return null;
      }
    });
  }

  @Override
  protected List<String> getRequiredScopes() {
    return Arrays.asList(SheetsScopes.SPREADSHEETS, DriveScopes.DRIVE);
  }

  /**
   *
   */
  /*class SheetWriter extends APIRequestRepeater<WriteSheetRequest, WriteSheetResponse> {

  }

  *//**
   *
   *//*
  class WriteSheetRequest implements APIRequest<WriteSheetResponse> {

    private final Sheet sheet;

    WriteSheetRequest(Sheet sheet) {
      this.sheet = sheet;
    }

    @Override
    public WriteSheetResponse doRequest() throws IOException {
      GoogleSheetsSinkClient.this.createFile(sheet);
      return new WriteSheetResponse();
    }

    @Override
    public String getLog() {
      return String.format("Resources limit exhausted during sheet writing, " +
              "wait for '%%d' seconds before next attempt, spreadsheetName '%s', sheet title '%s'",
          sheet.getSpreadSheetName(), sheet.getSheetTitle());
    }
  }

  *//**
   *
   *//*
  class WriteSheetResponse implements APIResponse {

  }*/

}
