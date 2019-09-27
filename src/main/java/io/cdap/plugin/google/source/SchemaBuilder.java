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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.source.exceptions.InvalidFilePropertyException;
import io.cdap.plugin.google.source.utils.BodyFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Util class for building pipeline schema.
 */
public class SchemaBuilder {
  public static final String SCHEMA_ROOT_RECORD_NAME = "FileFromFolder";

  public static final String BODY_FIELD_NAME = "body";
  public static final String OFFSET_FIELD_NAME = "offset";
  public static final String IMAGE_METADATA_FIELD_NAME = "imageMediaMetadata";
  public static final String VIDEO_METADATA_FIELD_NAME = "videoMediaMetadata";
  public static final String IMAGE_METADATA_NAME_PREFIX = IMAGE_METADATA_FIELD_NAME + ".";
  public static final String VIDEO_METADATA_NAME_PREFIX = VIDEO_METADATA_FIELD_NAME + ".";
  public static final String LOCATION_FIELD_NAME = "location";
  public static final String IMAGE_METADATA_LOCATION_FIELD_NAME = IMAGE_METADATA_NAME_PREFIX + LOCATION_FIELD_NAME;
  public static final String IMAGE_METADATA_LOCATION_FIELD_NAME_PREFIX = IMAGE_METADATA_LOCATION_FIELD_NAME + ".";

  public static Schema buildSchema(List<String> fields, BodyFormat bodyFormat) {
    List<String> extendedFields = new ArrayList<>(fields);
    extendedFields.add(BODY_FIELD_NAME);
    extendedFields.add(OFFSET_FIELD_NAME);
    List<Schema.Field> generalFields =
      extendedFields.stream().map(f -> SchemaBuilder.fromNameGeneral(f, bodyFormat))
        .filter(f -> f != null).collect(Collectors.toList());
    processImageMediaMetadata(extendedFields, generalFields);
    processVideoMediaMetadata(extendedFields, generalFields);

    return Schema.recordOf(SCHEMA_ROOT_RECORD_NAME, generalFields);
  }

  public static Schema.Field fromNameGeneral(String name, BodyFormat bodyFormat) {
    switch (name) {
      case BODY_FIELD_NAME:
        switch (bodyFormat) {
          case BYTES:
            return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
          case STRING:
            return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
        }
      case OFFSET_FIELD_NAME:
      case "size":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
      case "kind":
      case "id":
      case "name":
      case "mimeType":
      case "description":
      case "driveId":
      case "originalFilename":
      case "fullFileExtension":
      case "md5Checksum":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case "starred":
      case "trashed":
      case "explicitlyTrashed":
      case "isAppAuthorized":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
      case "trashedTime":
      case "createdTime":
      case "modifiedTime":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)));
      case "parents":
      case "spaces":
        return Schema.Field.of(name, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING))));
      case "properties":
      case "appProperties":
        return Schema.Field.of(name, Schema.nullableOf(Schema.arrayOf(
          Schema.recordOf("property",
                          Schema.Field.of("key", Schema.of(Schema.Type.STRING)),
                          Schema.Field.of("value", Schema.of(Schema.Type.STRING))))));
      default:
        if (!name.startsWith(IMAGE_METADATA_NAME_PREFIX) && !name.startsWith(VIDEO_METADATA_NAME_PREFIX)) {
          throw new InvalidFilePropertyException(name);
        }
    }
    return null;
  }

  public static void processImageMediaMetadata(List<String> fields, List<Schema.Field> schemaFields) {
    List<String> imageMediaFields =
      fields.stream().filter(f -> f.startsWith(IMAGE_METADATA_NAME_PREFIX))
        .map(f -> f.substring(IMAGE_METADATA_NAME_PREFIX.length())).collect(Collectors.toList());

    List<String> locationFields =
      fields.stream().filter(f -> f.startsWith(IMAGE_METADATA_LOCATION_FIELD_NAME_PREFIX))
        .map(f -> f.substring(IMAGE_METADATA_LOCATION_FIELD_NAME_PREFIX.length())).collect(Collectors.toList());

    List<Schema.Field> imageMediaFieldsSchemas = imageMediaFields.stream()
      .map(SchemaBuilder::fromImageMediaMetadataName)
      .filter(f -> f != null).collect(Collectors.toList());

    if (!locationFields.isEmpty()) {
      imageMediaFieldsSchemas.add(
        Schema.Field.of(LOCATION_FIELD_NAME,
                        Schema.nullableOf(Schema.recordOf(
                          "location",
                          locationFields.stream().map(SchemaBuilder::fromLocationName).collect(Collectors.toList())))));
    }

    if (!imageMediaFieldsSchemas.isEmpty()) {
      schemaFields.add(Schema.Field.of(IMAGE_METADATA_FIELD_NAME,
                                       Schema.nullableOf(Schema.recordOf("metadata",
                                                       imageMediaFieldsSchemas))));
    }
  }

  public static void processVideoMediaMetadata(List<String> fields, List<Schema.Field> schemaFields) {
    List<String> videoMediaFields =
      fields.stream().filter(f -> f.startsWith(VIDEO_METADATA_NAME_PREFIX))
        .map(f -> f.substring(VIDEO_METADATA_NAME_PREFIX.length())).collect(Collectors.toList());

    List<Schema.Field> videoMediaFieldsSchemas = videoMediaFields.stream()
      .map(SchemaBuilder::fromVideoMediaMetadataName)
      .filter(f -> f != null).collect(Collectors.toList());

    if (!videoMediaFieldsSchemas.isEmpty()) {
      schemaFields.add(Schema.Field.of(VIDEO_METADATA_FIELD_NAME,
                                       Schema.nullableOf(Schema.recordOf("metadata",
                                                       videoMediaFieldsSchemas))));
    }
  }

  public static Schema.Field fromImageMediaMetadataName(String name) {
    switch (name) {
      case "width":
      case "height":
      case "rotation":
      case "isoSpeed":
      case "subjectDistance":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.INT)));
      case "time":
      case "cameraMake":
      case "cameraModel":
      case "meteringMode":
      case "sensor":
      case "exposureMode":
      case "colorSpace":
      case "whiteBalance":
      case "lens":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case "exposureTime":
      case "aperture":
      case "focalLength":
      case "exposureBias":
      case "maxApertureValue":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.FLOAT)));
      case "flashUsed":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
      default:
        if (!name.startsWith("location.")) {
          throw new InvalidFilePropertyException(name);
        }
    }
    return null;
  }

  public static Schema.Field fromVideoMediaMetadataName(String name) {
    switch (name) {
      case "width":
      case "height":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.INT)));
      case "durationMillis":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      default:
        throw new InvalidFilePropertyException(name);
    }
  }

  public static Schema.Field fromLocationName(String name) {
    switch (name) {
      case "latitude":
      case "longitude":
      case "altitude":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
    }
    throw new InvalidFilePropertyException(name);
  }
}
