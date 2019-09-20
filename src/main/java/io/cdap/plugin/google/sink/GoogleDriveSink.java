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

import com.google.api.services.drive.model.File;
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
import io.cdap.plugin.google.common.FileFromFolder;
import io.cdap.plugin.google.source.SchemaBuilder;

import java.nio.charset.Charset;
import java.util.Random;

/**
 * Batch sink to writing multiple files to Google Drive directory.
 */
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
    batchSinkContext.addOutput(Output.of(config.referenceName, new GoogleDriveOutputFormatProvider()));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
    sinkClient.createFile(convertFromMessage(input));
  }

  private FileFromFolder convertFromMessage(StructuredRecord input) {
    byte[] content = new byte[]{};
    Long offset = 0L;
    File file = new File();
    for (Schema.Field field : input.getSchema().getFields()) {
      String fieldName = field.getName();
      if (fieldName.equals(SchemaBuilder.BODY_FIELD_NAME)) {
        content = input.get(fieldName);
      } else if (fieldName.equals(SchemaBuilder.OFFSET_FIELD_NAME)) {
        offset = input.get(fieldName);
      }
      if (fieldName.equals("name")) {
        String name = input.get(fieldName);
        if (name == null || name.equals("")) {
          name = generateName();
        }
        file.setName(name);
      } else if (!fieldName.contains(".")) {
        file.set(fieldName, input.get(fieldName));
      }
    }
    FileFromFolder fileFromFolder = new FileFromFolder(content, offset, file);
    return fileFromFolder;
  }

  private void validateSchema(FailureCollector collector, Schema inputSchema) {
    boolean bodyPresent = false;
    boolean offsetPresent = false;
    boolean namePresent = false;
    for (Schema.Field field : inputSchema.getFields()) {
      if (field.getName().equals(SchemaBuilder.BODY_FIELD_NAME)) {
        bodyPresent = true;
      }
      if (field.getName().equals(SchemaBuilder.OFFSET_FIELD_NAME)) {
        offsetPresent = true;
      }
      if (field.getName().equals("name")) {
        namePresent = true;
      }
    }
    if (!bodyPresent) {
      collector.addFailure("No 'body' field is available in input schema",
                           "Add 'content' field to schema");
    }
    if (!offsetPresent) {
      collector.addFailure("No 'offset' field is available in input schema",
                           "Add 'offset' field to schema");
    }
    if (!namePresent) {
      collector.addFailure("No 'name' field is available in input schema",
                           "Add 'name' field to schema");
    }
  }

  public String generateName() {
    byte[] array = new byte[7]; // length is bounded by 7
    new Random().nextBytes(array);
    return new String(array, Charset.forName("UTF-8"));
  }
}
