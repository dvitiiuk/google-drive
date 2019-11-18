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
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Writes {@link FileFromFolder} records to Google Drive via {@link GoogleDriveSinkClient}
 */
public class GoogleSheetsRecordWriter extends RecordWriter<NullWritable, MultipleRowsRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSheetsRecordWriter.class);

  private GoogleSheetsSinkClient sheetsSinkClient;
  private GoogleSheetsSinkConfig googleSheetsSinkConfig;

  private static final int BUFFER_SIZE = 100;
  private static final int THREADS_NUMBER = 5;
  private static final int TASKS_QUEUE_LENGTH = 10;

  private static int g = 0;

  private final Map<String, Map<String, Integer>> availableSheets = new HashMap<>();
  private final Map<String, String> availableFiles = new HashMap<>();


  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Queue<MultipleRowsRecord>>> recordQueues =
    new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> sheetContentShifts =
    new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> sheetSize =
    new ConcurrentHashMap<>();
  private final Semaphore threadsSemaphore = new Semaphore(TASKS_QUEUE_LENGTH);
  private final AtomicInteger recordRowsNumber = new AtomicInteger(0);
  private final AtomicInteger headerRowsNumber = new AtomicInteger(-1);
  private final ExecutorService writeService;

  public GoogleSheetsRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleDriveOutputFormatProvider.PROPERTY_CONFIG_JSON);
    googleSheetsSinkConfig =
      GoogleSheetsOutputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSinkConfig.class);

    sheetsSinkClient = new GoogleSheetsSinkClient(googleSheetsSinkConfig);

    writeService = Executors.newFixedThreadPool(THREADS_NUMBER);
  }

  @Override
  public void write(NullWritable nullWritable, MultipleRowsRecord rowsRecord) throws InterruptedException {
    try {
      String spreadSheetName = rowsRecord.getSpreadSheetName();
      String sheetTitle = rowsRecord.getSheetTitle();
      if (!availableFiles.keySet().contains(spreadSheetName)) {
        // create spreadsheet file with sheet

        Spreadsheet spreadsheet = sheetsSinkClient.createEmptySpreadsheet(spreadSheetName, sheetTitle);
        String spreadSheetId = spreadsheet.getSpreadsheetId();
        Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();
        sheetsSinkClient.startDeleteDimensions(spreadSheetId, sheetId, sheetTitle);

        sheetsSinkClient.moveSpreadsheetToDestinationFolder(spreadSheetId, spreadSheetName, sheetTitle);

        availableFiles.put(spreadSheetName, spreadSheetId);
        availableSheets.put(spreadSheetName, new HashMap<>());
        availableSheets.get(spreadSheetName).put(sheetTitle, sheetId);

        sheetContentShifts.put(spreadSheetName, new ConcurrentHashMap<>());
        sheetContentShifts.get(spreadSheetName).put(sheetTitle, 0);
        sheetSize.put(spreadSheetName, new ConcurrentHashMap<>());
        sheetSize.get(spreadSheetName).put(sheetTitle, 1000);
        recordQueues.put(spreadSheetName, new ConcurrentHashMap<>());
      }
      String spreadSheetId = availableFiles.get(spreadSheetName);
      if (!availableSheets.get(spreadSheetName).keySet().contains(sheetTitle)) {

        // create new sheet
        Integer sheetId = sheetsSinkClient.createSheet(spreadSheetId, spreadSheetName, sheetTitle);
        availableSheets.get(spreadSheetName).put(sheetTitle, sheetId);
        sheetContentShifts.get(spreadSheetName).put(sheetTitle, 0);
        sheetSize.get(spreadSheetName).put(sheetTitle, 1000);
      }

      MutableBoolean createTask = new MutableBoolean(false);
      recordQueues.get(spreadSheetName).compute(sheetTitle, (title, queue) -> {
        if (queue == null) {
          // create new task with this queue
          queue = new ConcurrentLinkedQueue<>();
          createTask.setTrue();
        } else {
          // just add new value to queue
          if (queue.size() == BUFFER_SIZE) {
            createTask.setTrue();
          }
        }
        queue.add(rowsRecord);
        return queue;
      });
      if (createTask.getValue()) {
        if (threadsSemaphore.tryAcquire(600, TimeUnit.SECONDS)) {
          writeService.submit(new MessagesProcessor(recordQueues.get(spreadSheetName), sheetTitle, g++));
        } else {
          throw new InterruptedException("It is not possible write out the record");
        }
      }
    } catch (ExecutionException | RetryException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws InterruptedException {
    //
  }

  /**
   *
   */
  private class MessagesProcessor implements Callable {

    private final ConcurrentHashMap<String, Queue<MultipleRowsRecord>> sheetsQueues;
    private final String sheetTitle;
    private int id;

    private MessagesProcessor(ConcurrentHashMap<String, Queue<MultipleRowsRecord>> sheetsQueues,
                              String sheetTitle, int id) {
      this.sheetsQueues = sheetsQueues;
      this.sheetTitle = sheetTitle;
      this.id = id;
    }

    @Override
    public Object call() throws Exception {
      try {
        while (true) {
          try {
            List<MultipleRowsRecord> recordsToProcess = new ArrayList<>();
            sheetsQueues.computeIfPresent(sheetTitle, (title, queue) -> {
              int counter = 0;
              while (counter < BUFFER_SIZE && queue != null && !queue.isEmpty()) {
                recordsToProcess.add(queue.poll());
                counter++;
              }
              if (queue.isEmpty()) {
                return null;
              }
              return queue;
            });
            if (recordsToProcess.isEmpty()) {
              return null;
            }
            String spreadSheetName = recordsToProcess.get(0).getSpreadSheetName();
            String sheetTitle = recordsToProcess.get(0).getSheetTitle();

            // write messages into spreadsheet via Sheets API
            MutableInt currentShift = new MutableInt(0);

            String spreadSheetId = availableFiles.get(spreadSheetName);
            Integer sheetId = availableSheets.get(spreadSheetName).get(sheetTitle);
            sheetContentShifts.computeIfPresent(spreadSheetName, (name, titles) -> {
              titles.computeIfPresent(sheetTitle, (title, shift) -> {
                currentShift.setValue(shift);
                if (recordRowsNumber.get() > 0 && headerRowsNumber.get() > -1) {
                  if (shift == 0) {
                    return headerRowsNumber.get() + recordRowsNumber.get() * recordsToProcess.size();
                  } else {
                    return shift + recordRowsNumber.get() * recordsToProcess.size();
                  }
                } else {
                  GoogleSheetsSinkClient.ContentRequest contentRequest =
                    sheetsSinkClient.prepareContentRequest(sheetId, recordsToProcess, true, 0);
                  recordRowsNumber.set(contentRequest.getRowsInRequest());
                  headerRowsNumber.set(contentRequest.getRowsInHeader());
                  int newShift = contentRequest.getRowsInHeader() +
                    contentRequest.getRowsInRequest() * contentRequest.getNumberOfRequests();
                  return newShift;
                }
              });
              return titles;
            });
            GoogleSheetsSinkClient.ContentRequest contentRequest =
              sheetsSinkClient.prepareContentRequest(sheetId, recordsToProcess,
                currentShift.getValue() == 0, currentShift.getValue());

            List<Request> mergeRequests = sheetsSinkClient.prepareMergeRequests(sheetId, recordsToProcess,
              currentShift.getValue() == 0, currentShift.getValue());


            sheetsSinkClient.extendDimension(spreadSheetId, spreadSheetName, sheetTitle, sheetId,
                contentRequest.getRowsInRequest() * contentRequest.getNumberOfRequests() +
                  contentRequest.getRowsInHeader());

            sheetsSinkClient.populateCells(spreadSheetId, spreadSheetName, sheetTitle,
              contentRequest.getContentRequest(), mergeRequests, currentShift.getValue());
          } catch (Exception e) {
            LOG.error(e.toString());
            return null;
          }
        }
      } finally {
        threadsSemaphore.release();
      }
    }
  }
}
