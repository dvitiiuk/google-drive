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

package io.cdap.plugin.google.sheets.common;

import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.GridRange;
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;

import java.util.List;

/**
 *
 */
public class MultipleRowsRecord {
  private String spreadSheetName;
  private String sheetTitle;
  private ComplexHeader header;
  private List<List<CellData>> singleRowRecords;
  private List<GridRange> mergeRanges;

  public MultipleRowsRecord(String spreadSheetName, String sheetTitle, ComplexHeader header,
                            List<List<CellData>> singleRowRecords, List<GridRange> mergeRanges) {
    this.spreadSheetName = spreadSheetName;
    this.sheetTitle = sheetTitle;
    this.header = header;
    this.singleRowRecords = singleRowRecords;
    this.mergeRanges = mergeRanges;
  }

  public String getSpreadSheetName() {
    return spreadSheetName;
  }

  public String getSheetTitle() {
    return sheetTitle;
  }

  public ComplexHeader getHeader() {
    return header;
  }

  public List<List<CellData>> getSingleRowRecords() {
    return singleRowRecords;
  }

  public List<GridRange> getMergeRanges() {
    return mergeRanges;
  }
}
