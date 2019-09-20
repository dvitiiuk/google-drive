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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleDriveBaseConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Source plugin.
 */
public class GoogleDriveSourceConfig extends GoogleDriveBaseConfig {
  public static final String FILTER = "filter";
  public static final String MODIFICATION_DATE_RANGE = "modificationDateRange";
  public static final String FILE_PROPERTIES = "fileProperties";
  public static final String FILE_TYPES_TO_PULL = "fileTypesToPull";
  public static final String MAX_BODY_SIZE = "maxBodySize";
  public static final String DOCS_EXPORTING_FORMAT = "docsExportingFormat";
  public static final String SHEETS_EXPORTING_FORMAT = "sheetsExportingFormat";
  public static final String DRAWINGS_EXPORTING_FORMAT = "drawingsExportingFormat";
  public static final String PRESENTATIONS_EXPORTING_FORMAT = "presentationsExportingFormat";

  @Nullable
  @Name(FILTER)
  @Description("A filter that can be applied to the files in the selected directory. " +
    "Filters follow the Google Drive Filter Syntax.")
  @Macro
  protected String filter;

  @Nullable
  @Name(MODIFICATION_DATE_RANGE)
  @Description("In addition to the filter specified above, also filter files to only pull those " +
    "that were modified between the date range.")
  @Macro
  protected String modificationDateRange;

  @Nullable
  @Name(FILE_PROPERTIES)
  @Description("Properties which should be get for each file in directory.")
  @Macro
  protected String fileProperties;

  @Name(FILE_TYPES_TO_PULL)
  @Description("Types of files should be pulled from specified directory.")
  @Macro
  protected String fileTypesToPull;

  @Name(MAX_BODY_SIZE)
  @Description("Maximum body size for each partition specified in bytes. Default 0 value means unlimited.")
  @Macro
  protected String maxBodySize;

  @Nullable
  @Name(DOCS_EXPORTING_FORMAT)
  @Description("MIME type for Google Documents.")
  @Macro
  protected String docsExportingFormat;

  @Nullable
  @Name(SHEETS_EXPORTING_FORMAT)
  @Description("MIME type for Google Spreadsheets.")
  @Macro
  protected String sheetsExportingFormat;

  @Nullable
  @Name(DRAWINGS_EXPORTING_FORMAT)
  @Description("MIME type for Google Drawings.")
  @Macro
  protected String drawingsExportingFormat;

  @Nullable
  @Name(PRESENTATIONS_EXPORTING_FORMAT)
  @Description("MIME type for Google Presentations.")
  @Macro
  protected String presentationsExportingFormat;
  private transient Schema schema = null;

  public GoogleDriveSourceConfig(String referenceName) {
    super(referenceName);
  }

  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(getFileProperties());
    }
    return schema;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(FILE_TYPES_TO_PULL)) {
      if (Strings.isNullOrEmpty(fileTypesToPull)) {
        collector.addFailure("fileTypesToPull is empty or macro is not available",
                             "fileTypesToPull must be not empty")
          .withConfigProperty(FILE_TYPES_TO_PULL);
      }
    }
    if (!containsMacro(MAX_BODY_SIZE)) {
      if (Strings.isNullOrEmpty(maxBodySize)) {
        collector.addFailure("maxBodySize is empty or macro is not available",
                             "maxBodySize must be not empty")
          .withConfigProperty(MAX_BODY_SIZE);
      }
    }
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

  List<String> getFileProperties() {
    if (fileProperties == null || "".equals(fileProperties)) {
      return Collections.emptyList();
    }
    return Arrays.asList(fileProperties.split(","));
  }

  public void setFileProperties(@Nullable String fileProperties) {
    this.fileProperties = fileProperties;
  }

  public List<String> getFileTypesToPull() {
    if (fileTypesToPull == null || "".equals(fileTypesToPull)) {
      return Collections.emptyList();
    }
    return Arrays.asList(fileTypesToPull.split(","));
  }

  public void setFileTypesToPull(String fileTypesToPull) {
    this.fileTypesToPull = fileTypesToPull;
  }

  public Long getMaxBodySize() {
    return Long.parseLong(maxBodySize);
  }

  public void setMaxBodySize(String maxBodySize) {
    this.maxBodySize = maxBodySize;
  }

  public String getDocsExportingFormat() {
    return docsExportingFormat;
  }

  public void setDocsExportingFormat(String docsExportingFormat) {
    this.docsExportingFormat = docsExportingFormat;
  }

  public String getSheetsExportingFormat() {
    return sheetsExportingFormat;
  }

  public void setSheetsExportingFormat(String sheetsExportingFormat) {
    this.sheetsExportingFormat = sheetsExportingFormat;
  }

  public String getDrawingsExportingFormat() {
    return drawingsExportingFormat;
  }

  public void setDrawingsExportingFormat(String drawingsExportingFormat) {
    this.drawingsExportingFormat = drawingsExportingFormat;
  }

  public String getPresentationsExportingFormat() {
    return presentationsExportingFormat;
  }

  public void setPresentationsExportingFormat(String presentationsExportingFormat) {
    this.presentationsExportingFormat = presentationsExportingFormat;
  }
}
