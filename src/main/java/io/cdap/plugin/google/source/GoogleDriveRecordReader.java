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

package io.cdap.plugin.google.source;

import io.cdap.plugin.google.common.FileFromFolder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * RecordReader implementation, which reads {@link FileFromFolder} wrappers from Google Drive using
 * Google Drive API.
 */
public class GoogleDriveRecordReader extends RecordReader<NullWritable, FileFromFolder> {

  private GoogleDriveSourceClient googleDriveSourceClient;
  private String fileId;
  private Long bytesFrom;
  private Long bytesTo;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON);
    GoogleDriveSourceConfig googleDriveSourceConfig =
      GoogleDriveInputFormatProvider.GSON.fromJson(configJson, GoogleDriveSourceConfig.class);
    googleDriveSourceClient = new GoogleDriveSourceClient(googleDriveSourceConfig);

    GoogleDriveSplit split = (GoogleDriveSplit) inputSplit;
    this.fileId = split.getFileId();
    this.bytesFrom = split.getBytesFrom();
    this.bytesTo = split.getBytesTo();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // read file with filename
    return googleDriveSourceClient.hasFile(fileId);
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public FileFromFolder getCurrentValue() throws IOException, InterruptedException {
    return googleDriveSourceClient.getFilePartition(bytesFrom, bytesTo);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // progress is unknown
    return 0.0f;
  }

  @Override
  public void close() throws IOException {

  }
}
