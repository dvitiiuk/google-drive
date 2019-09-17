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

package io.cdap.plugin.google;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.google.source.GoogleDriveSourceClient;
import io.cdap.plugin.google.source.GoogleDriveSourceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class GoogleDriveRecordReader extends RecordReader<NullWritable, FilesFromFolder> {
  private static final Gson gson = new GsonBuilder().create();

  private FilesFromFolder value;
  private GoogleDriveSourceClient googleDriveSourceClient;
  private int counter = 0;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON);
    GoogleDriveSourceConfig googleDriveSourceConfig = gson.fromJson(configJson, GoogleDriveSourceConfig.class);
    googleDriveSourceClient = new GoogleDriveSourceClient(googleDriveSourceConfig);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (counter < 2) {
      value = googleDriveSourceClient.getFiles();
      counter++;
      return true;
    }
    return false;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public FilesFromFolder getCurrentValue() throws IOException, InterruptedException {
    return value;
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
