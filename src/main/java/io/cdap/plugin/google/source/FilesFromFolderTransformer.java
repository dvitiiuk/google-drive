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

package io.cdap.plugin.google.source;

import com.google.api.client.json.GenericJson;
import com.google.api.services.drive.model.File;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.common.FileFromFolder;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms {@link FileFromFolder} wrapper to {@link StructuredRecord} instance.
 */
public class FilesFromFolderTransformer {

  // TODO: check for "properties" field
  public static StructuredRecord transform(FileFromFolder fileFromFolder, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    File file = fileFromFolder.getFile();

    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      if (name.equals(SchemaBuilder.BODY_FIELD_NAME)) {
        if (Schema.Type.STRING == field.getSchema().getNonNullable().getType()) {
          builder.set(SchemaBuilder.BODY_FIELD_NAME, new String(fileFromFolder.getContent()));
        } else {
          builder.set(SchemaBuilder.BODY_FIELD_NAME, fileFromFolder.getContent());
        }
      } else if (name.equals(SchemaBuilder.OFFSET_FIELD_NAME)) {
        builder.set(SchemaBuilder.OFFSET_FIELD_NAME, fileFromFolder.getOffset());
      } else if (file != null) {
        if (name.equals(SchemaBuilder.IMAGE_METADATA_FIELD_NAME)) {
          File.ImageMediaMetadata imageMediaMetadata = file.getImageMediaMetadata();
          if (imageMediaMetadata != null) {
            builder.set(field.getName(),
                        parseSubSchema(field.getSchema().getNonNullable(), imageMediaMetadata));
          }
        } else if (name.equals(SchemaBuilder.VIDEO_METADATA_FIELD_NAME)) {
          File.VideoMediaMetadata videoMediaMetadata = file.getVideoMediaMetadata();
          if (videoMediaMetadata != null) {
            builder.set(field.getName(),
                        parseSubSchema(field.getSchema().getNonNullable(), videoMediaMetadata));
          }
        } else if (Schema.LogicalType.TIMESTAMP_MILLIS.equals(field.getSchema().getLogicalType())) {
          builder.setTimestamp(name, ZonedDateTime.parse((CharSequence) file.get(name)));
        } else {
          builder.set(name, file.get(name));
        }
      }
    }
    return builder.build();
  }

  private static Map<String, Object> parseSubSchema(Schema subSchema, GenericJson info) {
    Map<String, Object> fieldsMap = new HashMap<>();
    for (Schema.Field imageFiled : subSchema.getFields()) {
      fieldsMap.put(imageFiled.getName(), info.get(imageFiled.getName()));
    }
    return fieldsMap;
  }
}
