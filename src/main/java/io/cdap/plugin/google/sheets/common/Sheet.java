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

import java.util.List;

/**
 * Representation for single sheet.
 */
public class Sheet {
  private List<List<Object>> values;
  private String spreadSheetName;
  private String sheetTitle;

  public Sheet(List<List<Object>> values, String spreadSheetName, String sheetTitle) {
    this.values = values;
    this.spreadSheetName = spreadSheetName;
    this.sheetTitle = sheetTitle;
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public String getSpreadSheetName() {
    return spreadSheetName;
  }

  public String getSheetTitle() {
    return sheetTitle;
  }
}
