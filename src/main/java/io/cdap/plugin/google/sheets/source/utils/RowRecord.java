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

package io.cdap.plugin.google.sheets.source.utils;

import com.google.api.services.sheets.v4.model.CellData;

import java.util.Map;

/**
 * Representation for single row data.
 */
public class RowRecord {
  private String spreadSheetName;
  private String sheetTitle;
  private Map<String, String> metadata;
  private Map<String, CellData> headeredCells;
  private boolean isEmptyData;

  public RowRecord(String spreadSheetName, String sheetTitle, Map<String, String> metadata,
                   Map<String, CellData> headeredCells, boolean isEmptyData) {
    this.spreadSheetName = spreadSheetName;
    this.sheetTitle = sheetTitle;
    this.metadata = metadata;
    this.headeredCells = headeredCells;
    this.isEmptyData = isEmptyData;
  }

  public String getSpreadSheetName() {
    return spreadSheetName;
  }

  public String getSheetTitle() {
    return sheetTitle;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public Map<String, CellData> getHeaderedCells() {
    return headeredCells;
  }

  public boolean isEmptyData() {
    return isEmptyData;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }
}
