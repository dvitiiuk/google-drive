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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Input format class which generates splits for each query.
 */
public class GoogleDriveInputFormat extends InputFormat {
  public static final Gson GSON = new GsonBuilder().create();

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    String configJson = conf.get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON);
    GoogleDriveSourceConfig googleDriveSourceConfig = GSON.fromJson(configJson, GoogleDriveSourceConfig.class);

    GoogleDriveSourceClient client = new GoogleDriveSourceClient(googleDriveSourceConfig);
    /*List<InputSplit> splits = new ArrayList<>();
    for (File file : client.getFiles()) {
      splits.add(new GoogleDriveSplit(file.getId()));
    }*/
    return client.getFiles().stream().map(f -> new GoogleDriveSplit(f.getId())).collect(Collectors.toList());
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    return new GoogleDriveRecordReader();
  }
}
