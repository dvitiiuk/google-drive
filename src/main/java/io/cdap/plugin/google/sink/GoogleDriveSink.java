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

package io.cdap.plugin.google.sink;

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
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.google.FileFromFolder;
import io.cdap.plugin.google.FilesFromFolder;

import java.util.Collections;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GoogleDrive")
@Description("Sink plugin to save files from the pipeline to Google Drive directory.")
public class GoogleDriveSink extends BatchSink<StructuredRecord, Void, Void> {

  private final GoogleDriveSinkConfig config;
  private final GoogleDriveSinkClient sinkClient;

  public GoogleDriveSink(GoogleDriveSinkConfig config) {
    this.sinkClient = new GoogleDriveSinkClient(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    validateSchema(failureCollector, pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {
    batchSinkContext.addOutput(Output.of(config.referenceName, new GoogleDriveOutputformatProvider()));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
    sinkClient.createFiles(convertFromMessage(input));
  }

  private FilesFromFolder convertFromMessage(StructuredRecord input) {
    byte[] content = new byte[]{};
    String mimeType = "";
    String name = "";
    for (Schema.Field field : input.getSchema().getFields()) {
      if (field.getName().equals("content")) {
        content = input.get(field.getName());
      }
      if (field.getName().equals("mimeType")) {
        mimeType = input.get(field.getName());
      }
      if (field.getName().equals("name")) {
        name = input.get(field.getName());
      }
    }
    FileFromFolder fileFromFolder = new FileFromFolder(content, mimeType, name);
    FilesFromFolder filesFromFolder = new FilesFromFolder(Collections.singletonList(fileFromFolder));
    return filesFromFolder;
  }

  private void validateSchema(FailureCollector collector, Schema inputSchema) {
    boolean contentPresent = false;
    boolean mimeTypePresent = false;
    boolean namePresent = false;
    for (Schema.Field field : inputSchema.getFields()) {
      if (field.getName().equals("content")) {
        contentPresent = true;
      }
      if (field.getName().equals("mimeType")) {
        mimeTypePresent = true;
      }
      if (field.getName().equals("name")) {
        namePresent = true;
      }
    }
    if (!contentPresent) {
      collector.addFailure("No 'content' field is available in input schema",
                           "Add 'content' field to schema");
    }
    if (!mimeTypePresent) {
      collector.addFailure("No 'mimeType' field is available in input schema",
                           "Add 'mimeType' field to schema");
    }
    if (!namePresent) {
      collector.addFailure("No 'name' field is available in input schema",
                           "Add 'name' field to schema");
    }
  }
}
