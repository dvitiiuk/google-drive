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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.sheets.common.Sheet;
import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transforms a {@link StructuredRecord}
 * to a {@link FileFromFolder}
 */
public class StructuredRecordToSheetTransformer {
  public static final Integer RANDOM_FILE_NAME_LENGTH = 16;
  private final String bodyFieldName;
  private final String spreadSheetNameFieldName;
  private final String sheetNameFieldName;
  private final String sheetName;

  public StructuredRecordToSheetTransformer(String bodyFieldName, String spreadSheetNameFieldName,
                                            String sheetNameFieldName, String sheetName) {
    this.bodyFieldName = bodyFieldName;
    this.spreadSheetNameFieldName = spreadSheetNameFieldName;
    this.sheetNameFieldName = sheetNameFieldName;
    this.sheetName = sheetName;
  }

  public Sheet transform(StructuredRecord input) {
    List<List<Object>> values = new ArrayList<>();
    String spreadSheetName = null;
    String sheetName = null;

    Schema schema = input.getSchema();
    if (schema.getField(bodyFieldName) != null) {
      values = fromCSV(input.get(bodyFieldName));
    }

    if (schema.getField(spreadSheetNameFieldName) != null) {
      spreadSheetName = input.get(spreadSheetNameFieldName);
    }
    if (spreadSheetName == null) {
      spreadSheetName = generateRandomName();
    }

    if (schema.getField(sheetNameFieldName) != null) {
      sheetName = input.get(sheetNameFieldName);
    }
    if (sheetName == null) {
      sheetName = this.sheetName;
    }

    Sheet sheet = null; //new Sheet(values, spreadSheetName, sheetName);
    return sheet;
  }

  private List<List<Object>> fromCSV(String body) {
    List<List<String>> unparsedCSV =
      Arrays.stream(body.split("\r\n")).map(row -> Arrays.asList(row.split("#")))
      .collect(Collectors.toList());
    List<List<Object>> parsedCSV = new ArrayList<>(unparsedCSV.size());
    for (List<String> unparsedRow : unparsedCSV) {
      List<Object> parsedRow = new ArrayList<>(unparsedRow.size());
      for (String unparsedCell : unparsedRow) {
        if (Strings.isNullOrEmpty(unparsedCell)) {
          parsedRow.add("");
        } else if (unparsedCell.startsWith("\"") && unparsedCell.endsWith("\"")) {
          parsedRow.add(unparsedCell.substring(1, unparsedCell.length() - 1));
        } else if (unparsedCell.equals("true") || unparsedCell.equals("false")) {
          parsedRow.add(Boolean.valueOf(unparsedCell));
        } else {
          parsedRow.add(new BigDecimal(unparsedCell));
        }
      }
      parsedCSV.add(parsedRow);
    }
    return parsedCSV;
  }

  private String generateRandomName() {
    return RandomStringUtils.randomAlphanumeric(RANDOM_FILE_NAME_LENGTH);
  }
}
