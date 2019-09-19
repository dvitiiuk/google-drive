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

package io.cdap.plugin.google;

import com.google.api.services.drive.model.File;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

/**
 * Transforms {@link FileFromFolder} wrapper to {@link StructuredRecord} instance.
 */
public class FilesFromFolderTransformer {

  public static StructuredRecord transform(FileFromFolder fileFromFolder, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      if (name.equals(SchemaBuilder.CONTENT_FIELD_NAME)) {
        builder.set(SchemaBuilder.CONTENT_FIELD_NAME, fileFromFolder.getContent());
      } else if (!field.getName().contains(".")) {
        File file = fileFromFolder.getFile();
        if (file != null) {
          builder.set(name, file.get(name));
        }
      }
    }
    return builder.build();
  }
}
