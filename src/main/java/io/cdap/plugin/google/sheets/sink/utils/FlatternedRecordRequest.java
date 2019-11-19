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

package io.cdap.plugin.google.sheets.sink.utils;

import com.google.api.services.sheets.v4.model.Request;

import java.util.List;

/**
 *
 */
public class FlatternedRecordRequest {
  private final Request contentRequest;
  private final List<Request> mergeRequests;
  private final Integer sheetId;
  private final int rowsInRequest;
  private final int rowsInHeader;
  private final int lastRowIndex;

  private String spreadSheetName;
  private String sheetTitle;
  private String spreadSheetId;

  public FlatternedRecordRequest(Request contentRequest, List<Request> mergeRequests,
                                 Integer sheetId, int rowsInRequest, int rowsInHeader,
                                 int lastRowIndex) {

    this.contentRequest = contentRequest;
    this.mergeRequests = mergeRequests;
    this.sheetId = sheetId;
    this.rowsInRequest = rowsInRequest;
    this.rowsInHeader = rowsInHeader;
    this.lastRowIndex = lastRowIndex;
  }

  public Request getContentRequest() {
    return contentRequest;
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

  public List<Request> getMergeRequests() {
    return mergeRequests;
  }

  public String getSpreadSheetName() {
    return spreadSheetName;
  }

  public String getSheetTitle() {
    return sheetTitle;
  }

  public Integer getSheetId() {
    return sheetId;
  }

  public String getSpreadSheetId() {
    return spreadSheetId;
  }

  public void setSpreadSheetName(String spreadSheetName) {
    this.spreadSheetName = spreadSheetName;
  }

  public void setSheetTitle(String sheetTitle) {
    this.sheetTitle = sheetTitle;
  }

  public void setSpreadSheetId(String spreadSheetId) {
    this.spreadSheetId = spreadSheetId;
  }
}
