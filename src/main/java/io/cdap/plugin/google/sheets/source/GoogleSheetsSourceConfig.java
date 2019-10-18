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

package io.cdap.plugin.google.sheets.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleFilteringSourceConfig;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configurations for Google Sheets Batch Source plugin.
 */
public class GoogleSheetsSourceConfig extends GoogleFilteringSourceConfig {

  public static final String SHEETS_TO_PULL = "sheetsToPull";
  public static final String NAME_SCHEMA = "schema";

  @Nullable
  @Name(SHEETS_TO_PULL)
  @Description("Properties that represent metadata of files. \n" +
    "They will be a part of output structured record.")
  @Macro
  protected String sheetsToPull;

  @Name(NAME_SCHEMA)
  @Macro
  @Nullable
  @Description("The schema of the table to read.")
  private transient Schema schema = null;

  @Nullable
  public List<String> getSheetsToPull() {
    return Arrays.asList(sheetsToPull.split(","));
  }

  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema();
    }
    return schema;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);
  }
}
