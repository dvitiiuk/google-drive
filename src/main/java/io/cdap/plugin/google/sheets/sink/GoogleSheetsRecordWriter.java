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
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import io.cdap.plugin.google.drive.sink.GoogleDriveOutputFormatProvider;
import io.cdap.plugin.google.drive.sink.GoogleDriveSinkClient;
import io.cdap.plugin.google.sheets.sink.utils.DimensionType;
import io.cdap.plugin.google.sheets.sink.utils.FlatternedRowsRecord;
import io.cdap.plugin.google.sheets.sink.utils.FlatternedRowsRequest;
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
 * Writes {@link FlatternedRowsRecord} records to Google drive via {@link GoogleDriveSinkClient}.
 */
public class GoogleSheetsRecordWriter extends RecordWriter<NullWritable, FlatternedRowsRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSheetsRecordWriter.class);

  private final Map<String, Map<String, Integer>> availableSheets = new HashMap<>();
  private final Map<String, String> availableFiles = new HashMap<>();

  private final Queue<FlatternedRowsRequest> recordsQueue = new ConcurrentLinkedQueue<>();
  private final Map<String, Map<String, Integer>> sheetsContentShift = new HashMap<>();
  private final Map<String, Map<String, Integer>> sheetsRowCount = new HashMap<>();
  private final Map<String, Map<String, Integer>> sheetsColumnCount = new HashMap<>();
  private final Semaphore threadsSemaphore;
  private final ExecutorService writeService;
  private final Semaphore queueSemaphore = new Semaphore(1);
  private final int flushTimeout;
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

    flushTimeout = googleSheetsSinkConfig.getFlushExecutionTimeout();

    formerScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    formerScheduledExecutorService.schedule(new TasksFormer(true),
      flushTimeout, TimeUnit.SECONDS);

  }

  @Override
  public void write(NullWritable nullWritable, FlatternedRowsRecord record) throws IOException, InterruptedException {
    try {
      String spreadSheetName = record.getSpreadSheetName();
      String sheetTitle = record.getSheetTitle();

      // create new spreadsheet file with sheet if needed
      if (!availableFiles.keySet().contains(spreadSheetName)) {
        Spreadsheet spreadsheet = sheetsSinkClient.createEmptySpreadsheet(spreadSheetName, sheetTitle);
        String spreadSheetId = spreadsheet.getSpreadsheetId();
        Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();
        int sheetRowCount = spreadsheet.getSheets().get(0).getProperties().getGridProperties().getRowCount();
        int sheetColumnCount = spreadsheet.getSheets().get(0).getProperties().getGridProperties().getColumnCount();

        sheetsSinkClient.moveSpreadsheetToDestinationFolder(spreadSheetId, spreadSheetName);

        availableFiles.put(spreadSheetName, spreadSheetId);

        availableSheets.put(spreadSheetName, new HashMap<>());
        availableSheets.get(spreadSheetName).put(sheetTitle, sheetId);

        sheetsContentShift.put(spreadSheetName, new ConcurrentHashMap<>());
        sheetsContentShift.get(spreadSheetName).put(sheetTitle, 0);

        sheetsRowCount.put(spreadSheetName, new ConcurrentHashMap<>());
        sheetsRowCount.get(spreadSheetName).put(sheetTitle, sheetRowCount);

        sheetsColumnCount.put(spreadSheetName, new ConcurrentHashMap<>());
        sheetsColumnCount.get(spreadSheetName).put(sheetTitle, sheetColumnCount);
      }
      String spreadSheetId = availableFiles.get(spreadSheetName);

      // create new sheet if needed
      if (!availableSheets.get(spreadSheetName).keySet().contains(sheetTitle)) {
        SheetProperties sheetProperties = sheetsSinkClient.createEmptySheet(spreadSheetId, spreadSheetName, sheetTitle);
        Integer sheetId = sheetProperties.getSheetId();
        int sheetRowCount = sheetProperties.getGridProperties().getRowCount();
        int sheetColumnCount = sheetProperties.getGridProperties().getColumnCount();

        availableSheets.get(spreadSheetName).put(sheetTitle, sheetId);
        sheetsContentShift.get(spreadSheetName).put(sheetTitle, 0);
        sheetsRowCount.get(spreadSheetName).put(sheetTitle, sheetRowCount);
        sheetsColumnCount.get(spreadSheetName).put(sheetTitle, sheetColumnCount);
      }

      // for each flatterned record
      Integer sheetId = availableSheets.get(spreadSheetName).get(sheetTitle);
      int currentShift = sheetsContentShift.get(spreadSheetName).get(sheetTitle);

      // 1. prepare all needed content and merge requests
      FlatternedRowsRequest flatternedRowsRequest =
        sheetsSinkClient.prepareFlatternedRequest(sheetId, record, currentShift);
      flatternedRowsRequest.setSheetTitle(sheetTitle);
      flatternedRowsRequest.setSpreadSheetName(spreadSheetName);

      // 2. update shift
      sheetsContentShift.get(spreadSheetName).put(sheetTitle, flatternedRowsRequest.getLastRowIndex());

      // 3. extend column dimension if needed
      if (record.getHeader().getWidth() > sheetsColumnCount.get(spreadSheetName).get(sheetTitle)) {
        int extensionSize = record.getHeader().getWidth() -
          sheetsRowCount.get(spreadSheetName).get(sheetTitle);
        sheetsSinkClient.extendDimension(spreadSheetId, spreadSheetName, sheetTitle, sheetId, extensionSize,
          DimensionType.COLUMNS);
        sheetsColumnCount.get(spreadSheetName).put(sheetTitle, record.getHeader().getWidth());
      }

      // 4. extend rows dimension if needed
      if (sheetsContentShift.get(spreadSheetName).get(sheetTitle) >
        sheetsRowCount.get(spreadSheetName).get(sheetTitle)) {
        int minimumAddition = sheetsContentShift.get(spreadSheetName).get(sheetTitle) -
          sheetsRowCount.get(spreadSheetName).get(sheetTitle);
        int extensionSize = Math.max(minimumAddition, googleSheetsSinkConfig.getMinPageExtensionSize());
        sheetsSinkClient.extendDimension(spreadSheetId, spreadSheetName, sheetTitle, sheetId, extensionSize,
          DimensionType.ROWS);
        sheetsRowCount.get(spreadSheetName).put(sheetTitle,
          sheetsRowCount.get(spreadSheetName).get(sheetTitle) + extensionSize);
      }

      // 5. offer flatterned requests to queue
      queueSemaphore.acquire();
      try {
        recordsQueue.offer(flatternedRowsRequest);
      } finally {
        queueSemaphore.release();
      }

      // 6. forcibly send requests to execution if the queue is exceeded
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

    // we should guarantee that at least one task former was called finally
    try {
      formerScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      formerScheduledExecutorService.submit(new TasksFormer(false)).get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getMessage());
    }

    // wait for worker threads completion
    writeService.shutdown();
    writeService.awaitTermination(flushTimeout, TimeUnit.SECONDS);
  }

  /**
   * Task that forms batches of requests and schedules tasks for writing the batches.
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
        Map<String, Queue<FlatternedRowsRequest>> groupedRequests = new HashMap<>();

        // pick up groups of elements
        FlatternedRowsRequest element;
        while ((element = recordsQueue.poll()) != null) {
          String spreadSheetName = element.getSpreadSheetName();

          if (!groupedRequests.containsKey(spreadSheetName)) {
            groupedRequests.put(spreadSheetName, new ConcurrentLinkedQueue<>());
          }
          groupedRequests.get(spreadSheetName).add(element);
        }

        // form batches of requests
        List<RecordsBatch> recordsToSort = new ArrayList<>();
        for (Map.Entry<String, Queue<FlatternedRowsRequest>> spreadSheetEntry : groupedRequests.entrySet()) {
          String spreadSheetName = spreadSheetEntry.getKey();
          Queue<FlatternedRowsRequest> queue = spreadSheetEntry.getValue();
          List<FlatternedRowsRequest> buffer = new ArrayList<>();

          while (!queue.isEmpty()) {
            buffer.add(queue.poll());
            if (buffer.size() == googleSheetsSinkConfig.getMaxBufferSize()) {
              recordsToSort.add(new RecordsBatch(buffer, spreadSheetName));
              buffer = new ArrayList<>();
            }
          }
          if (!buffer.isEmpty()) {
            recordsToSort.add(new RecordsBatch(buffer, spreadSheetName));
          }
        }

        // sort groups by size of records
        Collections.sort(recordsToSort);

        // send biggest groups to threads, else back to queue
        for (RecordsBatch recordsBatch : recordsToSort) {
          if (stopSignal) {
            if (threadsSemaphore.tryAcquire(flushTimeout, TimeUnit.SECONDS)) {
              // create new thread
              writeService.submit(
                new MessagesProcessor(recordsBatch));
            } else {
              throw new RuntimeException(
                String.format("It is not possible schedule of records batch during '%d' seconds.", flushTimeout));
            }
          } else {
            if (threadsSemaphore.tryAcquire()) {
              // create new thread
              writeService.submit(new MessagesProcessor(recordsBatch));
            } else {
              recordsBatch.group.stream().forEach(r -> {
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
   * Batch of flatterned requests.
   */
  private class RecordsBatch implements Comparable<RecordsBatch> {
    private final List<FlatternedRowsRequest> group;
    private final String spreadSheetName;

    private RecordsBatch(List<FlatternedRowsRequest> group, String spreadSheetName) {
      this.group = group;
      this.spreadSheetName = spreadSheetName;
    }

    public void add(FlatternedRowsRequest record) {
      group.add(record);
    }

    public List<FlatternedRowsRequest> getGroup() {
      return group;
    }

    public String getSpreadSheetName() {
      return spreadSheetName;
    }

    @Override
    public int compareTo(RecordsBatch o) {
      return o.group.size() - this.group.size();
    }
  }

  /**
   * Task that executes all requests from {@link RecordsBatch} by single Sheets API request.
   */
  private class MessagesProcessor implements Callable {

    private final RecordsBatch recordsBatch;

    private MessagesProcessor(RecordsBatch recordsBatch) {
      this.recordsBatch = recordsBatch;
    }

    @Override
    public Object call() throws Exception {
      try {
        List<FlatternedRowsRequest> recordsToProcess = recordsBatch.getGroup();
        if (recordsToProcess.isEmpty()) {
          return null;
        }
        String spreadSheetName = recordsBatch.getSpreadSheetName();

        List<Request> contentRequests = new ArrayList<>();
        List<Request> mergeRequests = new ArrayList<>();
        List<String> sheetTitles = new ArrayList<>();
        for (FlatternedRowsRequest flatternedRowsRequest : recordsToProcess) {
          contentRequests.add(flatternedRowsRequest.getContentRequest());
          mergeRequests.addAll(flatternedRowsRequest.getMergeRequests());
          sheetTitles.add(flatternedRowsRequest.getSheetTitle());
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
