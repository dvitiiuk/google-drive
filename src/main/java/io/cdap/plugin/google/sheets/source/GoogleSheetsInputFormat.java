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

import com.google.api.services.drive.model.File;
import io.cdap.plugin.google.common.GoogleDriveFilteringClient;
import io.cdap.plugin.google.common.GoogleFilteringSourceConfig;
import io.cdap.plugin.google.drive.source.GoogleDriveInputFormatProvider;
import io.cdap.plugin.google.drive.source.utils.ExportedType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Input format class which generates splits for each query.
 */
public class GoogleSheetsInputFormat extends InputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();
    String configJson = conf.get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON);
    GoogleFilteringSourceConfig googleFilteringSourceConfig =
      GoogleSheetsInputFormatProvider.GSON.fromJson(configJson, GoogleFilteringSourceConfig.class);
    GoogleSheetsSourceConfig googleSheetsSourceConfig =
      GoogleSheetsInputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSourceConfig.class);

    // get all sheets file according to filter
    GoogleDriveFilteringClient driveFilteringClient = new GoogleDriveFilteringClient(googleFilteringSourceConfig);
    List<File> spreadSheetsFiles =
      driveFilteringClient.getFilesSummary(Collections.singletonList(ExportedType.SPREADSHEETS));

    // get all sheets from sheet files and create appropriate splits
    GoogleSheetsSourceClient sheetsSourceClient = new GoogleSheetsSourceClient(googleSheetsSourceConfig);
    return getSplitsFromFiles(sheetsSourceClient, googleSheetsSourceConfig.getSheetsToPull(), spreadSheetsFiles);
  }

  private List<InputSplit> getSplitsFromFiles(GoogleSheetsSourceClient sheetsSourceClient, List<String> requiredSheets,
                                              List<File> files)
    throws IOException {
    List<InputSplit> splits = new ArrayList<>();

    for (File file : files) {
      String spreadSheetId = file.getId();
      if (requiredSheets.contains("all")) {
        splits.addAll(sheetsSourceClient.getSheetsTitles(spreadSheetId).stream()
                        .map(s -> new GoogleSheetsSplit(spreadSheetId, s))
                        .collect(Collectors.toList()));
      }
    }
    return splits;
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    return new GoogleSheetsRecordReader();
  }
}
