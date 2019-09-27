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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

// TODO complete tests
public class SchemaBuilderTest {

  @Test
  public void testGetImageMediaMetadata() {
    List<String> fields = new ArrayList<>();
    fields.add("imageMediaMetadata.width");
    fields.add("imageMediaMetadata.height");
    fields.add("imageMediaMetadata.location.latitude");

    List<Schema.Field> schemaFields = new ArrayList<>();
    SchemaBuilder.processImageMediaMetadata(fields, schemaFields);
  }

  @Test
  public void testEmptyFields() {
    List<String> fields = new ArrayList<>();

    // bytes body
    Schema schema = SchemaBuilder.buildSchema(fields, BodyFormat.BYTES);

    Assert.assertEquals(2, schema.getFields().size());
    Assert.assertNotNull(schema.getField(SchemaBuilder.BODY_FIELD_NAME));
    Assert.assertEquals(Schema.Type.BYTES, schema.getField(SchemaBuilder.BODY_FIELD_NAME)
      .getSchema().getNonNullable().getType());

    Assert.assertNotNull(schema.getField(SchemaBuilder.OFFSET_FIELD_NAME));
    Assert.assertEquals(Schema.Type.LONG, schema.getField(SchemaBuilder.OFFSET_FIELD_NAME)
      .getSchema().getNonNullable().getType());

    // string body
    schema = SchemaBuilder.buildSchema(fields, BodyFormat.STRING);

    Assert.assertEquals(2, schema.getFields().size());
    Assert.assertNotNull(schema.getField(SchemaBuilder.BODY_FIELD_NAME));
    Assert.assertEquals(Schema.Type.STRING, schema.getField(SchemaBuilder.BODY_FIELD_NAME)
      .getSchema().getNonNullable().getType());

    Assert.assertNotNull(schema.getField(SchemaBuilder.OFFSET_FIELD_NAME));
    Assert.assertEquals(Schema.Type.LONG, schema.getField(SchemaBuilder.OFFSET_FIELD_NAME)
      .getSchema().getNonNullable().getType());
  }
}
