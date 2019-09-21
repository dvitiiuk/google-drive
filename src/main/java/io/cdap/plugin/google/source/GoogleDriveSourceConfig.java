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
import io.cdap.plugin.google.common.GoogleDriveBaseConfig;
import io.cdap.plugin.google.source.utils.ExportedType;
import io.cdap.plugin.google.source.utils.ModifiedDateRangeType;
import io.cdap.plugin.google.source.utils.ModifiedDateRangeUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Source plugin.
 */
public class GoogleDriveSourceConfig extends GoogleDriveBaseConfig {
  public static final String FILTER = "filter";
  public static final String MODIFICATION_DATE_RANGE = "modificationDateRange";
  public static final String START_DATE = "startDate";
  public static final String END_DATE = "endDate";
  public static final String FILE_PROPERTIES = "fileProperties";
  public static final String FILE_TYPES_TO_PULL = "fileTypesToPull";
  public static final String MAX_PARTITION_SIZE = "maxPartitionSize";
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
  @Name(START_DATE)
  @Description("Accepts start date for modification date range. " +
    "RFC3339 format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.")
  @Macro
  protected String startDate;

  @Nullable
  @Name(END_DATE)
  @Description("Accepts end date for modification date range. " +
    "RFC3339 format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.")
  @Macro
  protected String endDate;

  @Nullable
  @Name(FILE_PROPERTIES)
  @Description("Properties which should be get for each file in directory.")
  @Macro
  protected String fileProperties;

  @Name(FILE_TYPES_TO_PULL)
  @Description("Types of files should be pulled from specified directory.")
  @Macro
  protected String fileTypesToPull;

  @Name(MAX_PARTITION_SIZE)
  @Description("Maximum body size for each partition specified in bytes. Default 0 value means unlimited.")
  @Macro
  protected String maxPartitionSize;

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
    checkPropertyIsSet(collector, fileTypesToPull, FILE_TYPES_TO_PULL);
    checkPropertyIsSet(collector, maxPartitionSize, MAX_PARTITION_SIZE);

    checkPropertyIsValid(collector, getModificationDateRangeType() != null, MODIFICATION_DATE_RANGE);
    if (getModificationDateRangeType().equals(ModifiedDateRangeType.CUSTOM)) {
      checkPropertyIsSet(collector, startDate, START_DATE);
      checkPropertyIsSet(collector, endDate, END_DATE);
      checkPropertyIsValid(collector, ModifiedDateRangeUtils.isValidDateString(startDate), START_DATE);
      checkPropertyIsValid(collector, ModifiedDateRangeUtils.isValidDateString(endDate), END_DATE);
    }

    for (ExportedType exportedType : getFileTypesToPull()) {
      checkPropertyIsValid(collector, exportedType != null, FILE_TYPES_TO_PULL);
    }
  }

  public ModifiedDateRangeType getModificationDateRangeType() {
    return Stream.of(ModifiedDateRangeType.values())
      .filter(keyType -> keyType.getValue().equalsIgnoreCase(modificationDateRange))
      .findAny()
      .orElse(null);
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

  public List<ExportedType> getFileTypesToPull() {
    if (fileTypesToPull == null || "".equals(fileTypesToPull)) {
      return Collections.emptyList();
    }
    return Arrays.stream(fileTypesToPull.split(","))
      .map(type -> ExportedType.fromValue(type)).collect(Collectors.toList());
  }

  public void setFileTypesToPull(String fileTypesToPull) {
    this.fileTypesToPull = fileTypesToPull;
  }

  public Long getMaxPartitionSize() {
    return Long.parseLong(maxPartitionSize);
  }

  public void setMaxPartitionSize(String maxPartitionSize) {
    this.maxPartitionSize = maxPartitionSize;
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

  @Nullable
  public String getStartDate() {
    return startDate;
  }

  public void setStartDate(@Nullable String startDate) {
    this.startDate = startDate;
  }

  @Nullable
  public String getEndDate() {
    return endDate;
  }

  public void setEndDate(@Nullable String endDate) {
    this.endDate = endDate;
  }
}
