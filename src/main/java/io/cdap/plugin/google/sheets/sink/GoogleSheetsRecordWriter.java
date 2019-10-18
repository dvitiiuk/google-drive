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

import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.drive.sink.GoogleDriveOutputFormatProvider;
import io.cdap.plugin.google.drive.sink.GoogleDriveSinkClient;
import io.cdap.plugin.google.drive.sink.GoogleDriveSinkConfig;
import io.cdap.plugin.google.sheets.common.Sheet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Writes {@link FileFromFolder} records to Google Drive via {@link GoogleDriveSinkClient}
 */
public class GoogleSheetsRecordWriter extends RecordWriter<NullWritable, Sheet> {

  private GoogleSheetsSinkClient sheetsSinkClient;

  public GoogleSheetsRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveOutputFormatProvider.PROPERTY_CONFIG_JSON);
    GoogleSheetsSinkConfig googleSheetsSinkConfig =
      GoogleSheetsOutputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSinkConfig.class);

    sheetsSinkClient = new GoogleSheetsSinkClient(googleSheetsSinkConfig);
  }

  @Override
  public void write(NullWritable nullWritable, Sheet sheet) throws IOException {
    sheetsSinkClient.createFile(sheet);
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    //no-op
  }
}
