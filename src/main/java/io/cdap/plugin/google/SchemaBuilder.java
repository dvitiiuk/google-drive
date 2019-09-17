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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaBuilder {

  public static Schema buildSchema(List<String> fields) {
    List<String> extendedFields = new ArrayList<>(fields);
    extendedFields.add("content");
    return Schema.recordOf(
      "FilesFromFolder",
      extendedFields.stream().map(SchemaBuilder::fromName).collect(Collectors.toList()));
  }

  private static Schema.Field fromName(String name) {
    switch (name) {
      case "content":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
      case "kind":
      case "id":
      case "name":
      case "mimeType":
      case "description":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case "starred":
      case "trashed":
      case "explicitlyTrashed":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
      case "parents":
        return Schema.Field.of(name, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    }
    throw new IllegalArgumentException(String.format("'%s' is not a valid field to select", name));
  }

  public static boolean isValidForFields(String fieldName) {
    switch (fieldName) {
      case "content":
      case "kind":
      case "id":
      case "name":
      case "mimeType":
      case "description":
      case "starred":
      case "trashed":
      case "parents":
        return true;
      default:
        return false;
    }
  }
}
