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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.google.sheets.common.MultipleRowsRecord;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Batch sink to writing multiple sheets to Google Drive directory.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(GoogleSheetsSink.NAME)
@Description("Sink plugin to save spreadsheets from the pipeline to Google Drive directory.")
public class GoogleSheetsSink extends BatchSink<StructuredRecord, Void, MultipleRowsRecord> {
  public static final String NAME = "GoogleSheets";

  private final GoogleSheetsSinkConfig config;
  private StructuredRecordToRowRecordTransformer transformer;

  public GoogleSheetsSink(GoogleSheetsSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();

    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) {
    Schema inputSchema = batchSinkContext.getInputSchema();

    FailureCollector failureCollector = batchSinkContext.getFailureCollector();
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();

    batchSinkContext.addOutput(Output.of(config.getReferenceName(), new GoogleSheetsOutputFormatProvider(config)));

    LineageRecorder lineageRecorder = new LineageRecorder(batchSinkContext, config.getReferenceName());
    lineageRecorder.createExternalDataset(inputSchema);
    if (inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
      String operationDescription = "Wrote sheet to Google Drive directory";
      lineageRecorder.recordWrite("Write", operationDescription,
                                  inputSchema.getFields().stream()
                                    .map(Schema.Field::getName)
                                    .collect(Collectors.toList()));
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    transformer = new StructuredRecordToRowRecordTransformer(config.getSchemaSpreadSheetNameFieldName(),
                                                         config.getSchemaSheetNameFieldName(),
                                                         config.getSpreadSheetName(),
                                                         config.getSheetName());

  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Void, MultipleRowsRecord>> emitter)
      throws IOException {
    MultipleRowsRecord rowsRecord = transformer.transform(input);
    emitter.emit(new KeyValue<>(null, rowsRecord));
  }
}
