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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.SchemaBuilder;
import io.cdap.plugin.google.common.GoogleDriveBaseConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Source plugin.
 */
public class GoogleDriveSourceConfig extends GoogleDriveBaseConfig {
  public static final String FILE_TYPES_TO_PULL = "fileTypesToPull";
  public static final String DOCS_EXPORTING_FORMAT = "docsExportingFormat";
  public static final String SHEETS_EXPORTING_FORMAT = "sheetsExportingFormat";
  public static final String DRAWINGS_EXPORTING_FORMAT = "drawingsExportingFormat";
  public static final String PRESENTATIONS_EXPORTING_FORMAT = "presentationsExportingFormat";
  @Nullable
  @Description("A filter that can be applied to the files in the selected directory. " +
    "Filters follow the Google Drive Filter Syntax.")
  @Macro
  protected String filter;
  @Nullable
  @Description("In addition to the filter specified above, also filter files to only pull those " +
    "that were modified between the date range.")
  @Macro
  protected String modificationDateRange;
  @Nullable
  @Description("Metainfos")
  @Macro
  protected String metainfos;
  @Name(FILE_TYPES_TO_PULL)
  @Description("Types of files should be pulled from specified directory.")
  @Macro
  protected String fileTypesToPull;
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

  public List<String> getFileTypesToPull() {
    if (fileTypesToPull == null || "".equals(fileTypesToPull)) {
      return Collections.emptyList();
    }
    return Arrays.asList(fileTypesToPull.split(","));
  }

  public void setFileTypesToPull(String fileTypesToPull) {
    this.fileTypesToPull = fileTypesToPull;
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
