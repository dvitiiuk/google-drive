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

import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.cdap.plugin.google.sheets.common.GoogleSheetsClient;
import io.cdap.plugin.google.sheets.common.Sheet;

import java.io.IOException;
import java.util.Collections;

/**
 * Client for writing data via Google Drive API.
 */
public class GoogleSheetsSinkClient extends GoogleSheetsClient<GoogleSheetsSinkConfig> {

  public GoogleSheetsSinkClient(GoogleSheetsSinkConfig config) {
    super(config);
  }

  public void createFile(Sheet sheet) throws IOException {
    Spreadsheet spreadsheet = new Spreadsheet();

    SpreadsheetProperties spreadsheetProperties = new SpreadsheetProperties();
    spreadsheetProperties.setTitle(sheet.getSpreadSheetName());

    com.google.api.services.sheets.v4.model.Sheet sheetToPast = new com.google.api.services.sheets.v4.model.Sheet();
    sheetToPast.setProperties(new SheetProperties().setTitle(sheet.getSheetTitle()));

    spreadsheet.setProperties(spreadsheetProperties);
    spreadsheet.setSheets(Collections.singletonList(sheetToPast));
    spreadsheet = service.spreadsheets().create(spreadsheet).execute();

    String spreadSheetsId = spreadsheet.getSpreadsheetId();

    ValueRange body = null; /*//new ValueRange()
      .setValues(sheet.getValues());*/
    service.spreadsheets().values().update(spreadSheetsId, sheet.getSheetTitle(), body)
        .setValueInputOption("USER_ENTERED")
        .execute();

    drive.files().update(spreadSheetsId, null)
      .setAddParents(config.getDirectoryIdentifier())
      .setRemoveParents("root")
      .setFields("id, parents")
      .execute();
  }

  @Override
  protected String getRequiredScope() {
    return FULL_PERMISSIONS_SCOPE;
  }
}
