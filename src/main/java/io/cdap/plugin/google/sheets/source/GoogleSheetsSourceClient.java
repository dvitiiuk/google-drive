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
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.cdap.plugin.google.sheets.common.GoogleSheetsClient;
import io.cdap.plugin.google.sheets.common.Sheet;

import java.io.IOException;
import java.util.List;
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

  public List<String> getSheetsTitles(String spreadSheetId) throws IOException {
    Spreadsheet spreadsheet = service.spreadsheets().get(spreadSheetId).execute();
    return spreadsheet.getSheets().stream().map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
  }

  public Sheet getSheetContent(String spreadSheetId, String sheetTitle) throws IOException {
    Sheets.Spreadsheets.Values.Get request = service.spreadsheets().values().get(spreadSheetId, sheetTitle);
    request.setValueRenderOption("FORMULA");
    request.setDateTimeRenderOption("SERIAL_NUMBER");
    ValueRange range = request.execute();

    String spreadSheetName = service.spreadsheets().get(spreadSheetId).execute().getProperties().getTitle();
    return new Sheet(range.getValues(), spreadSheetName, sheetTitle);
  }
}
