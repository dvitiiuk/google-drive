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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.plugin.google.common.GoogleDriveFilteringClient;
import io.cdap.plugin.google.common.GoogleFilteringSourceConfig;
import io.cdap.plugin.google.drive.source.GoogleDriveInputFormatProvider;
import io.cdap.plugin.google.drive.source.utils.ExportedType;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import io.cdap.plugin.google.sheets.source.utils.SheetsToPull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Input format class which generates splits for each query.
 */
public class GoogleSheetsInputFormat extends InputFormat {
  private static final AtomicInteger g = new AtomicInteger(0);

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();

    String configJson = conf.get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON);
    String headersJson = conf.get(GoogleSheetsInputFormatProvider.PROPERTY_HEADERS_JSON);

    GoogleFilteringSourceConfig googleFilteringSourceConfig =
        GoogleSheetsInputFormatProvider.GSON.fromJson(configJson, GoogleFilteringSourceConfig.class);
    GoogleSheetsSourceConfig googleSheetsSourceConfig =
        GoogleSheetsInputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSourceConfig.class);

    Type headersType = new TypeToken<LinkedHashMap<Integer, String>>() {
    }.getType();
    LinkedHashMap<Integer, String> resolvedHeaders =
        GoogleSheetsInputFormatProvider.GSON.fromJson(headersJson, headersType);

    // get all sheets files according to filter
    GoogleDriveFilteringClient driveFilteringClient = new GoogleDriveFilteringClient(googleFilteringSourceConfig);
    List<File> spreadSheetsFiles =
        driveFilteringClient.getFilesSummary(Collections.singletonList(ExportedType.SPREADSHEETS));

    // get all sheets from sheet files and create appropriate splits
    GoogleSheetsSourceClient sheetsSourceClient = new GoogleSheetsSourceClient(googleSheetsSourceConfig);
    try {
      return getSplitsFromFiles(sheetsSourceClient, googleSheetsSourceConfig, spreadSheetsFiles, resolvedHeaders);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private List<InputSplit> getSplitsFromFiles(GoogleSheetsSourceClient sheetsSourceClient,
                                              GoogleSheetsSourceConfig googleSheetsSourceConfig,
                                              List<File> files, LinkedHashMap<Integer, String> resolvedHeaders)
      throws IOException, InterruptedException, ExecutionException {
    List<InputSplit> splits = new ArrayList<>();
    String resolvedHeadersJson =
        GoogleSheetsInputFormatProvider.GSON.toJson(resolvedHeaders);

    List<MetadataKeyValueAddress> metadataCoordinates = googleSheetsSourceConfig.getMetadataCoordinates();
    String metadataCoordinatesJson =
        GoogleSheetsInputFormatProvider.GSON.toJson(metadataCoordinates);
    int firstDataRow = googleSheetsSourceConfig.getActualFirstDataRow();
    int lastDataRow = googleSheetsSourceConfig.getActualLastDataRow();


    ConcurrentLinkedQueue<File> filesQueue = new ConcurrentLinkedQueue<>(files);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("sheet-file-split-%d")
        .setPriority(Thread.MAX_PRIORITY)
        .setDaemon(true)
        .build();
    ExecutorService executor = Executors.newFixedThreadPool(10, threadFactory);
    List<Future<List<InputSplit>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(new FileSplitTask(filesQueue, firstDataRow, lastDataRow,
          googleSheetsSourceConfig.getSheetsToPull(), resolvedHeadersJson, metadataCoordinatesJson,
          sheetsSourceClient, googleSheetsSourceConfig)));
    }
    for (Future<List<InputSplit>> future : futures) {
      splits.addAll(future.get());
    }

    for (File file : files) {
      /*String spreadSheetId = file.getId(); j++;
      Stream<Integer> rowsStream = IntStream.range(firstDataRow, lastDataRow + 1).boxed();
      switch (googleSheetsSourceConfig.getSheetsToPull()) {
        case ALL:
          List<String> allTitles = sheetsSourceClient.getSheetsTitlesWithQuoteBypass(spreadSheetId);
          splits.addAll(
              rowsStream.flatMap(i -> allTitles.stream()
                  .map(s -> new GoogleSheetsSplit(spreadSheetId, s, i, resolvedHeadersJson,
                      metadataCoordinatesJson)))
                  .collect(Collectors.toList()));
          break;
        case NUMBERS:
          List<Integer> sheetIndexes = googleSheetsSourceConfig.getSheetsIdentifiers().stream()
              .map(s -> Integer.parseInt(s)).collect(Collectors.toList());
          List<String> sheetTitles = sheetsSourceClient.getSheets(spreadSheetId).stream()
              .filter(s -> sheetIndexes.contains(s.getProperties().getIndex()))
              .map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
          splits.addAll(
              rowsStream.flatMap(i -> sheetTitles.stream()
                  .map(s -> new GoogleSheetsSplit(spreadSheetId, s, i, resolvedHeadersJson,
                      metadataCoordinatesJson)))
                  .collect(Collectors.toList()));
          break;
        case TITLES:
          splits.addAll(
              rowsStream.flatMap(i -> googleSheetsSourceConfig.getSheetsIdentifiers().stream()
                  .map(s -> new GoogleSheetsSplit(spreadSheetId, s, i, resolvedHeadersJson,
                      metadataCoordinatesJson)))
                  .collect(Collectors.toList()));
          break;
      }*/
    }
    return splits;
  }

  private class FileSplitTask implements Callable<List<InputSplit>> {
    final ConcurrentLinkedQueue<File> filesQueue;
    final int firstDataRow;
    final int lastDataRow;
    final SheetsToPull sheetsToPull;
    final String resolvedHeadersJson;
    final String metadataCoordinatesJson;
    final GoogleSheetsSourceClient sheetsSourceClient;
    final GoogleSheetsSourceConfig googleSheetsSourceConfig;

    private FileSplitTask(ConcurrentLinkedQueue<File> filesQueue, int firstDataRow, int lastDataRow,
                          SheetsToPull sheetsToPull, String resolvedHeadersJson, String metadataCoordinatesJson,
                          GoogleSheetsSourceClient sheetsSourceClient,
                          GoogleSheetsSourceConfig googleSheetsSourceConfig) {
      this.filesQueue = filesQueue;
      this.firstDataRow = firstDataRow;
      this.lastDataRow = lastDataRow;
      this.sheetsToPull = sheetsToPull;
      this.resolvedHeadersJson = resolvedHeadersJson;
      this.metadataCoordinatesJson = metadataCoordinatesJson;
      this.sheetsSourceClient = sheetsSourceClient;
      this.googleSheetsSourceConfig = googleSheetsSourceConfig;
    }

    @Override
    public List<InputSplit> call() throws Exception {
      List<InputSplit> splits = new ArrayList<>();
      while (true) {
        File file = filesQueue.poll();
        if (file == null) {
          return splits;
        }
        int h = g.incrementAndGet();
        String spreadSheetId = file.getId();
        Stream<Integer> rowsStream = IntStream.range(firstDataRow, lastDataRow + 1).boxed();
        switch (sheetsToPull) {
          case ALL:
            List<String> allTitles = sheetsSourceClient.getSheetsTitles(spreadSheetId);
            splits.addAll(
                rowsStream.flatMap(i -> allTitles.stream()
                    .map(s -> new GoogleSheetsSplit(spreadSheetId, s, i, resolvedHeadersJson,
                        metadataCoordinatesJson)))
                    .collect(Collectors.toList()));
            break;
          case NUMBERS:
            List<Integer> sheetIndexes = googleSheetsSourceConfig.getSheetsIdentifiers().stream()
                .map(s -> Integer.parseInt(s)).collect(Collectors.toList());
            List<String> sheetTitles = sheetsSourceClient.getSheets(spreadSheetId).stream()
                .filter(s -> sheetIndexes.contains(s.getProperties().getIndex()))
                .map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
            splits.addAll(
                rowsStream.flatMap(i -> sheetTitles.stream()
                    .map(s -> new GoogleSheetsSplit(spreadSheetId, s, i, resolvedHeadersJson,
                        metadataCoordinatesJson)))
                    .collect(Collectors.toList()));
            break;
          case TITLES:
            splits.addAll(
                rowsStream.flatMap(i -> googleSheetsSourceConfig.getSheetsIdentifiers().stream()
                    .map(s -> new GoogleSheetsSplit(spreadSheetId, s, i, resolvedHeadersJson,
                        metadataCoordinatesJson)))
                    .collect(Collectors.toList()));
            break;
        }
      }
    }
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    return new GoogleSheetsRecordReader();
  }
}
