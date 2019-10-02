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
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.google.common.GoogleDriveBaseConfig;
import io.cdap.plugin.google.common.GoogleDriveClient;
import io.cdap.plugin.google.source.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.source.utils.BodyFormat;
import io.cdap.plugin.google.source.utils.ExportedType;
import io.cdap.plugin.google.source.utils.ModifiedDateRangeType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Source plugin.
 */
public class GoogleDriveSourceConfig extends GoogleDriveBaseConfig {
  public static final String FILTER = "filter";
  public static final String MODIFICATION_DATE_RANGE = "modificationDateRange";
  public static final String FILE_PROPERTIES = "fileProperties";
  public static final String FILE_TYPES_TO_PULL = "fileTypesToPull";
  public static final String MAX_PARTITION_SIZE = "maxPartitionSize";
  public static final String BODY_FORMAT = "bodyFormat";
  public static final String DOCS_EXPORTING_FORMAT = "docsExportingFormat";
  public static final String SHEETS_EXPORTING_FORMAT = "sheetsExportingFormat";
  public static final String DRAWINGS_EXPORTING_FORMAT = "drawingsExportingFormat";
  public static final String PRESENTATIONS_EXPORTING_FORMAT = "presentationsExportingFormat";

  public static final String MODIFICATION_DATE_RANGE_LABEL = "Modification date range";
  public static final String FILE_PROPERTIES_LABEL = "File properties";
  public static final String FILE_TYPES_TO_PULL_LABEL = "File types to pull";
  public static final String BODY_FORMAT_LABEL = "Body output format";

  private static final String IS_VALID_FAILURE_MESSAGE_PATTERN = "%s has invalid value %s";
  private static final String IS_SET_FAILURE_MESSAGE_PATTERN = "'%s' property is empty or macro is not available";
  private static final String CHECK_CORRECTIVE_MESSAGE_PATTERN = "Enter valid '%s' property";

  @Nullable
  @Name(FILTER)
  @Description("Filter that can be applied to the files in the selected directory. \n" +
    "Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).")
  @Macro
  protected String filter;

  @Name(MODIFICATION_DATE_RANGE)
  @Description("Filter that narrows set of files by modified date range. " +
    "User can select either some preset range or input range manually in the RFC3339 format")
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

  @Name(MAX_PARTITION_SIZE)
  @Description("Maximum body size for each partition specified in bytes. Default 0 value means unlimited.")
  @Macro
  protected String maxPartitionSize;

  @Name(BODY_FORMAT)
  @Description("Output format for body of file. \"Bytes\" and \"String\" values are available. Default is \"Bytes\".")
  @Macro
  protected String bodyFormat;

  @Name(DOCS_EXPORTING_FORMAT)
  @Description("MIME type for exporting Google Documents. Default value is 'text/plain'.")
  @Macro
  protected String docsExportingFormat;

  @Name(SHEETS_EXPORTING_FORMAT)
  @Description("MIME type for exporting Google Spreadsheets. Default value is 'text/csv'.")
  @Macro
  protected String sheetsExportingFormat;

  @Name(DRAWINGS_EXPORTING_FORMAT)
  @Description("MIME type for exporting Google Drawings. Default value is 'image/svg+xml'.")
  @Macro
  protected String drawingsExportingFormat;

  @Name(PRESENTATIONS_EXPORTING_FORMAT)
  @Description("MIME type for exporting Google Presentations. Default value is 'text/plain'.")
  @Macro
  protected String presentationsExportingFormat;
  private transient Schema schema = null;

  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(getFileProperties(), getBodyFormat());
    }
    return schema;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);

    validateFileTypesToPull(collector);

    validateBodyFormat(collector);

    validateModificationDateRange(collector);

    validateFileProperties(collector);
  }

  @Override
  protected GoogleDriveClient getDriveClient() {
    return new GoogleDriveSourceClient(this);
  }

  private void validateFileTypesToPull(FailureCollector collector) {
    if (!containsMacro(FILE_TYPES_TO_PULL)) {
      if (!Strings.isNullOrEmpty(fileProperties)) {
        List<String> exportedTypeStrings = Arrays.asList(fileTypesToPull.split(","));
        exportedTypeStrings.forEach(exportedTypeString -> {
          try {
            ExportedType.fromValue(exportedTypeString);
          } catch (InvalidPropertyTypeException e) {
            collectInvalidProperty(collector, FILE_TYPES_TO_PULL, fileProperties, FILE_TYPES_TO_PULL_LABEL,
                                   e.getStackTrace());
          }
        });
      }
    }
  }

  private void validateBodyFormat(FailureCollector collector) {
    if (checkPropertyIsSet(collector, BODY_FORMAT, bodyFormat, BODY_FORMAT_LABEL)) {
      try {
        getModificationDateRangeType();
      } catch (InvalidPropertyTypeException e) {
        collectInvalidProperty(collector, BODY_FORMAT, bodyFormat,
                               BODY_FORMAT_LABEL, e.getStackTrace());
      }
    }
  }

  private boolean validateModificationDateRange(FailureCollector collector) {
    if (checkPropertyIsSet(collector, MODIFICATION_DATE_RANGE, modificationDateRange, MODIFICATION_DATE_RANGE_LABEL)) {
      try {
        getModificationDateRangeType();
        return true;
      } catch (InvalidPropertyTypeException e) {
        collectInvalidProperty(collector, MODIFICATION_DATE_RANGE, modificationDateRange,
                               MODIFICATION_DATE_RANGE_LABEL, e.getStackTrace());
      }
    }
    return false;
  }

  private void validateFileProperties(FailureCollector collector) {
    if (!containsMacro(FILE_TYPES_TO_PULL)) {
      if (!Strings.isNullOrEmpty(fileProperties)) {
        try {
          SchemaBuilder.buildSchema(getFileProperties(), getBodyFormat());
        } catch (InvalidPropertyTypeException e) {
          collectInvalidProperty(collector, FILE_TYPES_TO_PULL, fileProperties, FILE_PROPERTIES_LABEL,
                                 e.getStackTrace());
        }
      }
    }
  }

  protected boolean checkPropertyIsSet(FailureCollector collector, String propertyValue, String propertyName,
                                       String propertyLabel) {
    if (!containsMacro(propertyName)) {
      if (Strings.isNullOrEmpty(propertyValue)) {
        collector.addFailure(getIsSetValidationFailedMessage(propertyLabel),
                             getValidationFailedCorrectiveAction(propertyLabel))
          .withConfigProperty(propertyName);
      } else {
        return true;
      }
    }
    return false;
  }

  protected void checkPropertyIsValid(FailureCollector collector, boolean isPropertyValid, String propertyName,
                                      String propertyValue, String propertyLabel,
                                      StackTraceElement[] stacktraceElements) {
    if (isPropertyValid) {
      return;
    }
    ValidationFailure failure = collector
      .addFailure(String.format(IS_VALID_FAILURE_MESSAGE_PATTERN, propertyName, propertyValue),
                  getValidationFailedCorrectiveAction(propertyLabel))
      .withConfigProperty(propertyName);
    if (stacktraceElements != null) {
      failure.withStacktrace(stacktraceElements);
    }
  }

  protected void checkPropertyIsValid(FailureCollector collector, boolean isPropertyValid, String propertyName,
                                      String propertyValue, String propertyLabel) {
    checkPropertyIsValid(collector, isPropertyValid, propertyName, propertyValue, propertyLabel, null);
  }

  protected void collectInvalidProperty(FailureCollector collector, String propertyName, String propertyValue,
                                        String propertyLabel, StackTraceElement[] stacktraceElements) {
    checkPropertyIsValid(collector, false, propertyName, propertyValue, propertyLabel,
                         stacktraceElements);
  }

  protected String getIsSetValidationFailedMessage(String propertyLabel) {
    return String.format(IS_SET_FAILURE_MESSAGE_PATTERN, propertyLabel);
  }

  protected String getValidationFailedCorrectiveAction(String propertyLabel) {
    return String.format(CHECK_CORRECTIVE_MESSAGE_PATTERN, propertyLabel);
  }

  public ModifiedDateRangeType getModificationDateRangeType() {
    return ModifiedDateRangeType.fromValue(modificationDateRange);
  }

  @Nullable
  public String getFilter() {
    return filter;
  }

  @Nullable
  public String getModificationDateRange() {
    return modificationDateRange;
  }

  List<String> getFileProperties() {
    if (Strings.isNullOrEmpty(fileProperties)) {
      return Collections.emptyList();
    }
    return Arrays.asList(fileProperties.split(","));
  }

  public List<ExportedType> getFileTypesToPull() {
    if (Strings.isNullOrEmpty(fileTypesToPull)) {
      return Collections.emptyList();
    }
    return Arrays.stream(fileTypesToPull.split(","))
      .map(type -> ExportedType.fromValue(type)).collect(Collectors.toList());
  }

  public BodyFormat getBodyFormat() {
    return BodyFormat.fromValue(bodyFormat);
  }

  public Long getMaxPartitionSize() {
    return Long.parseLong(maxPartitionSize);
  }

  public String getDocsExportingFormat() {
    return docsExportingFormat;
  }

  public String getSheetsExportingFormat() {
    return sheetsExportingFormat;
  }

  public String getDrawingsExportingFormat() {
    return drawingsExportingFormat;
  }

  public String getPresentationsExportingFormat() {
    return presentationsExportingFormat;
  }
}
