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

import com.github.rholder.retry.RetryException;
import com.google.gson.reflect.TypeToken;
import io.cdap.plugin.google.drive.source.GoogleDriveInputFormatProvider;
import io.cdap.plugin.google.sheets.common.RowRecord;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * RecordReader implementation, which reads {@link RowRecord} wrappers from Google Drive using
 * Google Drive API.
 */
public class GoogleSheetsRecordReader extends RecordReader<NullWritable, RowRecord> {

  private GoogleSheetsSourceClient googleSheetsSourceClient;
  private String fileId;
  private GoogleSheetsSourceConfig config;
  private LinkedHashMap<Integer, String> resolvedHeaders;
  private List<MetadataKeyValueAddress> metadataCoordinates;

  private Queue<RowTask> rowTaskQueue = new ArrayDeque<>();
  private RowTask currentRowTask;
  private String currentSheetTitle;
  private Map<String, String> sheetMetadata = Collections.EMPTY_MAP;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON);
    config = GoogleDriveInputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSourceConfig.class);
    googleSheetsSourceClient = new GoogleSheetsSourceClient(config);

    GoogleSheetsSplit split = (GoogleSheetsSplit) inputSplit;
    this.fileId = split.getFileId();
    Type headersType = new TypeToken<LinkedHashMap<Integer, String>>() {
    }.getType();
    this.resolvedHeaders = GoogleDriveInputFormatProvider.GSON.fromJson(split.getHeaders(), headersType);
    Type metadataType = new TypeToken<List<MetadataKeyValueAddress>>() {
    }.getType();
    this.metadataCoordinates = GoogleDriveInputFormatProvider.GSON.fromJson(split.getMetadates(), metadataType);

    int firstDataRow = config.getActualFirstDataRow();
    int lastDataRow = config.getActualLastDataRow();

    Stream<Integer> rowsStream = IntStream.range(firstDataRow, lastDataRow + 1).boxed();
    try {
      switch (config.getSheetsToPull()) {
        case ALL:
          List<String> allTitles = googleSheetsSourceClient.getSheetsTitles(fileId);
          rowTaskQueue.addAll(rowsStream.flatMap(i -> allTitles.stream().map(s -> new RowTask(s, i)))
              .collect(Collectors.toList()));
          break;
        case NUMBERS:
          List<Integer> sheetIndexes = config.getSheetsIdentifiers().stream()
              .map(s -> Integer.parseInt(s)).collect(Collectors.toList());
          List<String> sheetTitles = googleSheetsSourceClient.getSheets(fileId).stream()
              .filter(s -> sheetIndexes.contains(s.getProperties().getIndex()))
              .map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
          rowTaskQueue.addAll(rowsStream.flatMap(i -> sheetTitles.stream().map(s -> new RowTask(s, i)))
              .collect(Collectors.toList()));
          break;
        case TITLES:
          rowTaskQueue.addAll(rowsStream.flatMap(i -> config.getSheetsIdentifiers().stream()
              .map(s -> new RowTask(s, i)))
              .collect(Collectors.toList()));
          break;
      }
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean nextKeyValue() {
    currentRowTask = rowTaskQueue.poll();
    return currentRowTask != null;
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public RowRecord getCurrentValue() throws IOException, InterruptedException {
    boolean isNewSheet = !currentRowTask.getSheetTitle().equals(currentSheetTitle);
    currentSheetTitle = currentRowTask.getSheetTitle();
    RowRecord rowRecord = null;
    try {
      rowRecord = googleSheetsSourceClient.getContent(fileId, currentSheetTitle,
          currentRowTask.getRowNumber(), resolvedHeaders, isNewSheet ? metadataCoordinates : null);
    } catch (ExecutionException | RetryException e) {
      throw new InterruptedException(e.getMessage());
    }
    if (isNewSheet) {
      sheetMetadata = rowRecord.getMetadata();
    } else {
      rowRecord.setMetadata(sheetMetadata);
    }
    return rowRecord;
  }

  @Override
  public float getProgress() {
    // progress is unknown
    return 0.0f;
  }

  @Override
  public void close() {

  }

  class RowTask {
    final String sheetTitle;
    final int rowNumber;

    RowTask(String sheetTitle, int rowNumber) {
      this.sheetTitle = sheetTitle;
      this.rowNumber = rowNumber;
    }

    public String getSheetTitle() {
      return sheetTitle;
    }

    public int getRowNumber() {
      return rowNumber;
    }
  }
}
