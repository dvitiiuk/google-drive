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
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.drive.sink.GoogleDriveOutputFormatProvider;
import io.cdap.plugin.google.drive.sink.GoogleDriveSinkClient;
import io.cdap.plugin.google.sheets.common.MultipleRowsRecord;
import io.cdap.plugin.google.sheets.common.RowRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.concurrent.ExecutionException;

/**
 * Writes {@link FileFromFolder} records to Google Drive via {@link GoogleDriveSinkClient}
 */
public class GoogleSheetsRecordWriter extends RecordWriter<NullWritable, MultipleRowsRecord> {

  private GoogleSheetsSinkClient sheetsSinkClient;
  private GoogleSheetsSinkConfig googleSheetsSinkConfig;

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
      sheetsSinkClient.createFile(rowsRecord);
    } catch (ExecutionException | RetryException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    //no-op
  }
}
