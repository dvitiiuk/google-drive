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
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.drive.sink.GoogleDriveOutputFormatProvider;
import io.cdap.plugin.google.drive.sink.GoogleDriveSinkClient;
import io.cdap.plugin.google.sheets.common.MultipleRowsRecord;
import io.cdap.plugin.google.sheets.sink.utils.FlatternedRecordRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Writes {@link FileFromFolder} records to Google Drive via {@link GoogleDriveSinkClient}
 */
public class GoogleSheetsRecordWriter extends RecordWriter<NullWritable, MultipleRowsRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSheetsRecordWriter.class);

  private static final int DEFAULT_SHEET_LENGTH = 1000;

  private final Map<String, Map<String, Integer>> availableSheets = new HashMap<>();
  private final Map<String, String> availableFiles = new HashMap<>();

  private final Queue<FlatternedRecordRequest> recordsQueue = new ConcurrentLinkedQueue<>();
  private final Map<String, Map<String, Integer>> sheetContentShifts = new HashMap<>();
  private final Map<String, Map<String, Integer>> sheetSize = new HashMap<>();
  private final Semaphore threadsSemaphore;
  private final ExecutorService writeService;
  private final Semaphore queueSemaphore = new Semaphore(1);
  private GoogleSheetsSinkClient sheetsSinkClient;
  private GoogleSheetsSinkConfig googleSheetsSinkConfig;
  private ScheduledExecutorService formerScheduledExecutorService;
  private boolean stopSignal;

  public GoogleSheetsRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveOutputFormatProvider.PROPERTY_CONFIG_JSON);
    googleSheetsSinkConfig =
      GoogleSheetsOutputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSinkConfig.class);

    sheetsSinkClient = new GoogleSheetsSinkClient(googleSheetsSinkConfig);

    writeService = Executors.newFixedThreadPool(googleSheetsSinkConfig.getThreadsNumber());
    threadsSemaphore = new Semaphore(googleSheetsSinkConfig.getThreadsNumber());

    formerScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    formerScheduledExecutorService.schedule(new TasksFormer(true),
      googleSheetsSinkConfig.getFlushExecutionTimeout(), TimeUnit.SECONDS);
  }

  @Override
  public void write(NullWritable nullWritable, MultipleRowsRecord record) throws IOException, InterruptedException {
    try {
      String spreadSheetName = record.getSpreadSheetName();
      String sheetTitle = record.getSheetTitle();
      if (!availableFiles.keySet().contains(spreadSheetName)) {
        // create spreadsheet file with sheet

        Spreadsheet spreadsheet = sheetsSinkClient.createEmptySpreadsheet(spreadSheetName, sheetTitle);
        String spreadSheetId = spreadsheet.getSpreadsheetId();
        Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();

        sheetsSinkClient.moveSpreadsheetToDestinationFolder(spreadSheetId, spreadSheetName, sheetTitle);

        availableFiles.put(spreadSheetName, spreadSheetId);
        availableSheets.put(spreadSheetName, new HashMap<>());
        availableSheets.get(spreadSheetName).put(sheetTitle, sheetId);

        sheetContentShifts.put(spreadSheetName, new ConcurrentHashMap<>());
        sheetContentShifts.get(spreadSheetName).put(sheetTitle, 0);
        sheetSize.put(spreadSheetName, new ConcurrentHashMap<>());
        sheetSize.get(spreadSheetName).put(sheetTitle, DEFAULT_SHEET_LENGTH);
      }
      String spreadSheetId = availableFiles.get(spreadSheetName);
      if (!availableSheets.get(spreadSheetName).keySet().contains(sheetTitle)) {

        // create new sheet
        Integer sheetId = sheetsSinkClient.createSheet(spreadSheetId, spreadSheetName, sheetTitle);
        availableSheets.get(spreadSheetName).put(sheetTitle, sheetId);
        sheetContentShifts.get(spreadSheetName).put(sheetTitle, 0);
        sheetSize.get(spreadSheetName).put(sheetTitle, DEFAULT_SHEET_LENGTH);
      }

      // for each record
      Integer sheetId = availableSheets.get(spreadSheetName).get(sheetTitle);
      int currentShift = sheetContentShifts.get(spreadSheetName).get(sheetTitle);
      // 1. flattern it and
      FlatternedRecordRequest flatternedRecordRequest =
        sheetsSinkClient.prepareFlatternedRequest(sheetId, record, currentShift == 0, currentShift);
      flatternedRecordRequest.setSheetTitle(sheetTitle);
      flatternedRecordRequest.setSpreadSheetName(spreadSheetName);
      flatternedRecordRequest.setSpreadSheetId(spreadSheetId);
      // 2. update shift
      sheetContentShifts.get(spreadSheetName).put(sheetTitle, flatternedRecordRequest.getLastRowIndex());

      // 3. extend dimension
      if (sheetContentShifts.get(spreadSheetName).get(sheetTitle) > sheetSize.get(spreadSheetName).get(sheetTitle)) {
        int minimumAddition = sheetContentShifts.get(spreadSheetName).get(sheetTitle) -
          sheetSize.get(spreadSheetName).get(sheetTitle);
        int extensionSize = Math.max(minimumAddition, googleSheetsSinkConfig.getMinPageExtensionSize());
        sheetsSinkClient.extendDimension(spreadSheetId, spreadSheetName, sheetTitle, sheetId, extensionSize);
        sheetSize.get(spreadSheetName).put(sheetTitle, sheetSize.get(spreadSheetName).get(sheetTitle) + extensionSize);
      }

      queueSemaphore.acquire();
      try {
        recordsQueue.offer(flatternedRecordRequest);
      } finally {
        queueSemaphore.release();
      }

      if (recordsQueue.size() >= googleSheetsSinkConfig.getRecordsQueueLength()) {
        int counter = 0;
        while (recordsQueue.size() >= googleSheetsSinkConfig.getRecordsQueueLength() && !stopSignal) {
          if (counter > 0) {
            Thread.sleep(1000);
          }
          formerScheduledExecutorService.submit(new TasksFormer(false)).get();
          counter++;
        }
      }

    } catch (ExecutionException | RetryException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    stopSignal = true;
    // wait for scheduled task formers will be completed
    formerScheduledExecutorService.shutdown();
    formerScheduledExecutorService.awaitTermination(googleSheetsSinkConfig.getMaxFlushInterval() * 2,
      TimeUnit.SECONDS);

    // we should guarantee at least one task former calling
    try {
      formerScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      formerScheduledExecutorService.submit(new TasksFormer(false)).get();
    } catch (ExecutionException e) {
      throw new InterruptedException(e.getMessage());
    }

    // wait for worker threads
    writeService.shutdown();
    writeService.awaitTermination(googleSheetsSinkConfig.getFlushExecutionTimeout(), TimeUnit.SECONDS);
  }

  /**
   *
   */
  public class TasksFormer implements Callable {
    private final boolean rerun;

    public TasksFormer(boolean rerun) {
      this.rerun = rerun;
    }

    @Override
    public Object call() throws Exception {
      queueSemaphore.acquire();
      try {
        Map<String, Queue<FlatternedRecordRequest>> groupedRequests = new HashMap<>();

        // pick up groups of elements
        FlatternedRecordRequest element;
        while ((element = recordsQueue.poll()) != null) {
          String spreadSheetName = element.getSpreadSheetName();

          if (!groupedRequests.containsKey(spreadSheetName)) {
            groupedRequests.put(spreadSheetName, new ConcurrentLinkedQueue<>());
          }
          groupedRequests.get(spreadSheetName).add(element);
        }

        List<GroupedRecord> recordsToSort = new ArrayList<>();
        for (Map.Entry<String, Queue<FlatternedRecordRequest>> spreadSheetEntry : groupedRequests.entrySet()) {
          String spreadSheetName = spreadSheetEntry.getKey();
          Queue<FlatternedRecordRequest> queue = spreadSheetEntry.getValue();
          List<FlatternedRecordRequest> buffer = new ArrayList<>();

          while (!queue.isEmpty()) {
            buffer.add(queue.poll());
            if (buffer.size() == googleSheetsSinkConfig.getMaxBufferSize()) {
              recordsToSort.add(new GroupedRecord(buffer, spreadSheetName));
              buffer = new ArrayList<>();
            }
          }
          if (!buffer.isEmpty()) {
            recordsToSort.add(new GroupedRecord(buffer, spreadSheetName));
          }
        }

        // sort groups by size of records
        Collections.sort(recordsToSort);

        // send biggest groups to threads
        for (GroupedRecord groupedRecord : recordsToSort) {
          if (stopSignal) {
            if (threadsSemaphore.tryAcquire(googleSheetsSinkConfig.getFlushExecutionTimeout(), TimeUnit.SECONDS)) {
              // create new thread
              writeService.submit(
                new MessagesProcessor(groupedRecord));
            } else {
              throw new InterruptedException("It is not possible write out the record");
            }
          } else {
            if (threadsSemaphore.tryAcquire()) {
              // create new thread
              writeService.submit(new MessagesProcessor(groupedRecord));
            } else {
              groupedRecord.group.stream().forEach(r -> {
                recordsQueue.offer(r);
              });
            }
          }
        }
      } finally {
        queueSemaphore.release();
      }
      if (rerun) {
        if (!stopSignal) {
          formerScheduledExecutorService.schedule(new TasksFormer(rerun),
            googleSheetsSinkConfig.getMaxFlushInterval(), TimeUnit.SECONDS);
        }
      }
      return null;
    }
  }

  /**
   *
   */
  private class GroupedRecord implements Comparable<GroupedRecord> {
    private final List<FlatternedRecordRequest> group;
    private final String spreadSheetName;

    private GroupedRecord(List<FlatternedRecordRequest> group, String spreadSheetName) {
      this.group = group;
      this.spreadSheetName = spreadSheetName;
    }

    public void add(FlatternedRecordRequest record) {
      group.add(record);
    }

    public List<FlatternedRecordRequest> getGroup() {
      return group;
    }

    public String getSpreadSheetName() {
      return spreadSheetName;
    }

    @Override
    public int compareTo(GroupedRecord o) {
      return o.group.size() - this.group.size();
    }
  }

  /**
   *
   */
  private class MessagesProcessor implements Callable {

    private final GroupedRecord groupedRecord;

    private MessagesProcessor(GroupedRecord groupedRecord) {
      this.groupedRecord = groupedRecord;
    }

    @Override
    public Object call() throws Exception {
      try {
        List<FlatternedRecordRequest> recordsToProcess = groupedRecord.getGroup();
        if (recordsToProcess.isEmpty()) {
          return null;
        }
        String spreadSheetName = groupedRecord.getSpreadSheetName();

        List<Request> contentRequests = new ArrayList<>();
        List<Request> mergeRequests = new ArrayList<>();
        List<String> sheetTitles = new ArrayList<>();
        for (FlatternedRecordRequest flatternedRecordRequest : recordsToProcess) {
          contentRequests.add(flatternedRecordRequest.getContentRequest());
          mergeRequests.addAll(flatternedRecordRequest.getMergeRequests());
          sheetTitles.add(flatternedRecordRequest.getSheetTitle());
        }

        String spreadSheetId = availableFiles.get(spreadSheetName);

        sheetsSinkClient.populateCells(spreadSheetId, spreadSheetName, sheetTitles,
          contentRequests, mergeRequests);
      } catch (Exception e) {
        LOG.error(e.toString());
      } finally {
        threadsSemaphore.release();
        return null;
      }
    }
  }
}
