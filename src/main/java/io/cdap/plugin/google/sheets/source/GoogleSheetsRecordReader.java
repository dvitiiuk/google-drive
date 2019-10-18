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

import io.cdap.plugin.google.drive.source.GoogleDriveInputFormatProvider;
import io.cdap.plugin.google.sheets.common.Sheet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * RecordReader implementation, which reads {@link Sheet} wrappers from Google Drive using
 * Google Drive API.
 */
public class GoogleSheetsRecordReader extends RecordReader<NullWritable, Sheet> {

  private GoogleSheetsSourceClient googleSheetsSourceClient;
  private String fileId;
  //private String spreadSheetName;
  private String sheetTitle;
  private boolean isFileProcessed;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON);
    GoogleSheetsSourceConfig googleSheetsSourceConfig =
      GoogleDriveInputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSourceConfig.class);
    googleSheetsSourceClient = new GoogleSheetsSourceClient(googleSheetsSourceConfig);

    GoogleSheetsSplit split = (GoogleSheetsSplit) inputSplit;
    this.fileId = split.getFileId();
    //this.spreadSheetName = split.getSpreadSheetName();
    this.sheetTitle = split.getSheetTitle();
    this.isFileProcessed = false;
  }

  @Override
  public boolean nextKeyValue() {
    return !isFileProcessed;
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public Sheet getCurrentValue() throws IOException {
    // read file and content
    isFileProcessed = true;
    return googleSheetsSourceClient.getSheetContent(fileId, sheetTitle);
  }

  @Override
  public float getProgress() {
    // progress is unknown
    return 0.0f;
  }

  @Override
  public void close() {

  }
}
