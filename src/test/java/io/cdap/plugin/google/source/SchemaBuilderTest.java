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
import io.cdap.plugin.google.source.utils.BodyFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

// TODO complete tests
public class SchemaBuilderTest {

  @Test
  public void testGetImageMediaMetadata() {
    List<String> fields = new ArrayList<>();
    fields.add(getFullImageName(SchemaBuilder.IMAGE_WIDTH_FIELD_NAME));
    fields.add(getFullImageName(SchemaBuilder.IMAGE_CAMERA_MODEL_FIELD_NAME));
    fields.add(getFullImageName(SchemaBuilder.IMAGE_APERTURE_FIELD_NAME));
    fields.add(getFullImageName(SchemaBuilder.IMAGE_FLASH_USED_FIELD_NAME));
    fields.add(getFullImageLocationName(SchemaBuilder.IMAGE_LATITUDE_FIELD_NAME));
    fields.add(getFullImageLocationName(SchemaBuilder.IMAGE_LONGITUDE_FIELD_NAME));

    Schema schema = SchemaBuilder.buildSchema(fields, BodyFormat.BYTES);

    // expected fields for body and offset and record for all image metadata fields
    assertEquals(3, schema.getFields().size());

    assertNotNull(schema.getField(SchemaBuilder.IMAGE_METADATA_FIELD_NAME));
    assertEquals(Schema.Type.RECORD, schema.getField(SchemaBuilder.IMAGE_METADATA_FIELD_NAME)
      .getSchema().getNonNullable().getType());

    Schema imageMetadataRecord = schema.getField(SchemaBuilder.IMAGE_METADATA_FIELD_NAME)
      .getSchema().getNonNullable();

    assertEquals(5, imageMetadataRecord.getFields().size());
    assertEquals(Schema.Type.INT, imageMetadataRecord.getField(SchemaBuilder.IMAGE_WIDTH_FIELD_NAME)
      .getSchema().getNonNullable().getType());
    assertTrue(imageMetadataRecord.getField(SchemaBuilder.IMAGE_WIDTH_FIELD_NAME)
      .getSchema().isNullable());

    assertEquals(Schema.Type.STRING, imageMetadataRecord.getField(SchemaBuilder.IMAGE_CAMERA_MODEL_FIELD_NAME)
      .getSchema().getNonNullable().getType());
    assertTrue(imageMetadataRecord.getField(SchemaBuilder.IMAGE_CAMERA_MODEL_FIELD_NAME)
      .getSchema().isNullable());

    assertEquals(Schema.Type.FLOAT, imageMetadataRecord.getField(SchemaBuilder.IMAGE_APERTURE_FIELD_NAME)
      .getSchema().getNonNullable().getType());
    assertTrue(imageMetadataRecord.getField(SchemaBuilder.IMAGE_APERTURE_FIELD_NAME)
      .getSchema().isNullable());

    assertEquals(Schema.Type.BOOLEAN, imageMetadataRecord.getField(SchemaBuilder.IMAGE_FLASH_USED_FIELD_NAME)
      .getSchema().getNonNullable().getType());
    assertTrue(imageMetadataRecord.getField(SchemaBuilder.IMAGE_FLASH_USED_FIELD_NAME)
      .getSchema().isNullable());

  }

  @Test
  public void testEmptyFields() {
    List<String> fields = new ArrayList<>();

    // bytes body
    Schema schema = SchemaBuilder.buildSchema(fields, BodyFormat.BYTES);

    assertEquals(2, schema.getFields().size());
    assertNotNull(schema.getField(SchemaBuilder.BODY_FIELD_NAME));
    assertEquals(Schema.Type.BYTES, schema.getField(SchemaBuilder.BODY_FIELD_NAME)
      .getSchema().getNonNullable().getType());

    assertNotNull(schema.getField(SchemaBuilder.OFFSET_FIELD_NAME));
    assertEquals(Schema.Type.LONG, schema.getField(SchemaBuilder.OFFSET_FIELD_NAME)
      .getSchema().getNonNullable().getType());

    // string body
    schema = SchemaBuilder.buildSchema(fields, BodyFormat.STRING);

    assertEquals(2, schema.getFields().size());
    assertNotNull(schema.getField(SchemaBuilder.BODY_FIELD_NAME));
    assertEquals(Schema.Type.STRING, schema.getField(SchemaBuilder.BODY_FIELD_NAME)
      .getSchema().getNonNullable().getType());

    assertNotNull(schema.getField(SchemaBuilder.OFFSET_FIELD_NAME));
    assertEquals(Schema.Type.LONG, schema.getField(SchemaBuilder.OFFSET_FIELD_NAME)
      .getSchema().getNonNullable().getType());
  }

  private String getFullImageName(String fieldName) {
    return SchemaBuilder.IMAGE_METADATA_FIELD_NAME + "." + fieldName;
  }

  private String getFullImageLocationName(String fieldName) {
    return SchemaBuilder.IMAGE_METADATA_FIELD_NAME + "." + SchemaBuilder.LOCATION_FIELD_NAME + "." + fieldName;
  }
}
