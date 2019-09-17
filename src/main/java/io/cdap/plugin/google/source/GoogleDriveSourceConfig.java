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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.SchemaBuilder;
import io.cdap.plugin.google.common.GoogleDriveBaseConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class GoogleDriveSourceConfig extends GoogleDriveBaseConfig {

  private transient Schema schema = null;

  @Nullable
  @Description("A filter that can be applied to the files in the selected directory. Filters follow the Google Drive Filter Syntax.")
  @Macro
  protected String filter;

  @Nullable
  @Description("In addition to the filter specified above, also filter files to only pull those that were modified between the date range.")
  @Macro
  protected String modificationDateRange;

  @Nullable
  @Description("Metainfos")
  @Macro
  protected String metainfos;

  public GoogleDriveSourceConfig(String referenceName) {
    super(referenceName);
  }

  public Schema getSchema() {
    if (schema == null) {
      /*schema = Schema.recordOf("FilesFromFolder",
          Schema.Field.of("content", Schema.of(Schema.Type.STRING)));*/
      schema = SchemaBuilder.buildSchema(getMetainfos());
    }
    return schema;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);
  }

  @Nullable
  public String getFilter() {
    return filter;
  }

  public void setFilter(@Nullable String filter) {
    this.filter = filter;
  }

  @Nullable
  public String getModificationDateRange() {
    return modificationDateRange;
  }

  public void setModificationDateRange(@Nullable String modificationDateRange) {
    this.modificationDateRange = modificationDateRange;
  }

  List<String> getMetainfos() {
    if (metainfos == null || "".equals(metainfos)) {
      return Collections.emptyList();
    }
    return Arrays.asList(metainfos.split(","));
  }

  public void setMetainfos(@Nullable String metainfos) {
    this.metainfos = metainfos;
  }
}
