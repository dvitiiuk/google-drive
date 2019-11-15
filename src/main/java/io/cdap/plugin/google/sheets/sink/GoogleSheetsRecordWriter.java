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
import com.google.api.services.sheets.v4.model.Spreadsheet;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.drive.sink.GoogleDriveOutputFormatProvider;
import io.cdap.plugin.google.drive.sink.GoogleDriveSinkClient;
import io.cdap.plugin.google.sheets.common.MultipleRowsRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Writes {@link FileFromFolder} records to Google Drive via {@link GoogleDriveSinkClient}
 */
public class GoogleSheetsRecordWriter extends RecordWriter<NullWritable, MultipleRowsRecord> {

  private GoogleSheetsSinkClient sheetsSinkClient;
  private GoogleSheetsSinkConfig googleSheetsSinkConfig;

  private final Map<String, Map<String, Integer>> availableSheets = new HashMap<>();
  private final Map<String, Map<String, Integer>> sheetContentShifts = new HashMap<>();
  private final Map<String, String> availableFiles = new HashMap<>();

  public GoogleSheetsRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveOutputFormatProvider.PROPERTY_CONFIG_JSON);
    googleSheetsSinkConfig =
      GoogleSheetsOutputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSinkConfig.class);

    sheetsSinkClient = new GoogleSheetsSinkClient(googleSheetsSinkConfig);
  }

  @Override
  public void write(NullWritable nullWritable, MultipleRowsRecord rowsRecord) throws InterruptedException {
    try {
      String spreadSheetName = rowsRecord.getSpreadSheetName();
      String sheetTitle = rowsRecord.getSheetTitle();
      boolean newSheet = false;
      if (!availableFiles.keySet().contains(spreadSheetName)) {
        // create spreadsheet file with sheet

        Spreadsheet spreadsheet = sheetsSinkClient.createEmptySpreadsheet(spreadSheetName, sheetTitle);
        String spreadSheetId = spreadsheet.getSpreadsheetId();
        Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();

        sheetsSinkClient.moveSpreadsheetToDestinationFolder(spreadSheetId, spreadSheetName, sheetTitle);

        availableFiles.put(spreadSheetName, spreadSheetId);
        availableSheets.put(spreadSheetName, new HashMap<>());
        availableSheets.get(spreadSheetName).put(sheetTitle, sheetId);
        sheetContentShifts.put(spreadSheetName, new HashMap<>());
        sheetContentShifts.get(spreadSheetName).put(sheetTitle, 0);
        newSheet = true;
      }
      String spreadSheetId = availableFiles.get(spreadSheetName);
      if (!availableSheets.get(spreadSheetName).keySet().contains(sheetTitle)) {

        // create new sheet
        Integer sheetId = sheetsSinkClient.createSheet(spreadSheetId, spreadSheetName, sheetTitle);
        availableSheets.get(spreadSheetName).put(sheetTitle, sheetId);
        sheetContentShifts.get(spreadSheetName).put(sheetTitle, 0);
        newSheet = true;
      }
      Integer spreadId = availableSheets.get(spreadSheetName).get(sheetTitle);
      Integer contentShift = sheetContentShifts.get(spreadSheetName).get(sheetTitle);
      int addedRowsNumber = sheetsSinkClient.populateCells(spreadSheetId, spreadId, rowsRecord, newSheet, contentShift);
      sheetContentShifts.get(spreadSheetName).put(sheetTitle, contentShift + addedRowsNumber);
    } catch (ExecutionException | RetryException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    //no-op
  }
}
