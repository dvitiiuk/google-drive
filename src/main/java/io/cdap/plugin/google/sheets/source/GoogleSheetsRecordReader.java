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

import com.google.gson.reflect.TypeToken;
import io.cdap.plugin.google.drive.source.GoogleDriveInputFormatProvider;
import io.cdap.plugin.google.sheets.common.Sheet;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * RecordReader implementation, which reads {@link Sheet} wrappers from Google Drive using
 * Google Drive API.
 */
public class GoogleSheetsRecordReader extends RecordReader<NullWritable, Sheet> {

  private GoogleSheetsSourceClient googleSheetsSourceClient;
  private String fileId;
  private String sheetTitle;
  private int rowNumber;
  private boolean isFileProcessed;
  private GoogleSheetsSourceConfig config;
  private LinkedHashMap<Integer, String> resolvedHeaders;
  private List<MetadataKeyValueAddress> metadataCoordinates;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON);
    config = GoogleDriveInputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSourceConfig.class);
    googleSheetsSourceClient = new GoogleSheetsSourceClient(config);

    GoogleSheetsSplit split = (GoogleSheetsSplit) inputSplit;
    this.fileId = split.getFileId();
    this.sheetTitle = split.getSheetTitle();
    this.rowNumber = split.getRowNumber();
    this.isFileProcessed = false;
    Type headersType = new TypeToken<LinkedHashMap<Integer, String>>() { }.getType();
    this.resolvedHeaders = GoogleDriveInputFormatProvider.GSON.fromJson(split.getHeaders(), headersType);
    Type metadataType = new TypeToken<List<MetadataKeyValueAddress>>() { }.getType();
    this.metadataCoordinates = GoogleDriveInputFormatProvider.GSON.fromJson(split.getMetadates(), metadataType);
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
  public Sheet getCurrentValue() throws IOException, InterruptedException {
    // read file and content
    isFileProcessed = true;
    return googleSheetsSourceClient.getSheetContentWithQuoteBypass(fileId, sheetTitle, rowNumber, config,
        resolvedHeaders, metadataCoordinates);
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
