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

package io.cdap.plugin.google.source.utils;

import io.cdap.plugin.google.common.GoogleDriveClient;

import java.util.Arrays;

/**
 * An enum which represent a type of exported file.
 */
public enum ExportedType {
  BINARY("binary", GoogleDriveClient.DRIVE_DOCS_MIME_PREFIX),
  DOCUMENTS("documents", GoogleDriveClient.DRIVE_DOCUMENTS_MIME),
  SPREADSHEETS("spreadsheets", GoogleDriveClient.DRIVE_SPREADSHEETS_MIME),
  DRAWINGS("drawings", GoogleDriveClient.DRIVE_DRAWINGS_MIME),
  PRESENTATIONS("presentations", GoogleDriveClient.DRIVE_PRESENTATIONS_MIME),
  APPSCRIPTS("appsScripts", GoogleDriveClient.DRIVE_APPS_SCRIPTS_MIME);

  private final String value;
  private final String relatedMIME;

  ExportedType(String value, String relatedMIME) {
    this.value = value;
    this.relatedMIME = relatedMIME;
  }

  public String getValue() {
    return value;
  }

  public String getRelatedMIME() {
    return relatedMIME;
  }

  public static ExportedType fromValue(String value) {
    return Arrays.stream(ExportedType.values()).filter(exportedType -> exportedType.getValue().equals(value))
      .findAny().orElse(null);
  }
}
