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

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Strings;
import io.cdap.plugin.google.common.FileFromFolder;
import io.cdap.plugin.google.common.GoogleDriveClient;
import io.cdap.plugin.google.source.utils.DateRange;
import io.cdap.plugin.google.source.utils.ExportedType;
import io.cdap.plugin.google.source.utils.ModifiedDateRangeUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Client for getting data via Google Drive API.
 */
public class GoogleDriveSourceClient extends GoogleDriveClient<GoogleDriveSourceConfig> {

  public static final String MODIFIED_TIME_TERM = "modifiedTime";

  public static final String DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder";
  public static final String DRIVE_DOCS_MIME_PREFIX = "application/vnd.google-apps.";
  public static final String DRIVE_DOCUMENTS_MIME = "application/vnd.google-apps.document";
  public static final String DRIVE_SPREADSHEETS_MIME = "application/vnd.google-apps.spreadsheet";
  public static final String DRIVE_DRAWINGS_MIME = "application/vnd.google-apps.drawing";
  public static final String DRIVE_PRESENTATIONS_MIME = "application/vnd.google-apps.presentation";
  public static final String DRIVE_APPS_SCRIPTS_MIME = "application/vnd.google-apps.script";

  public static final String DEFAULT_APPS_SCRIPTS_EXPORT_MIME = "application/vnd.google-apps.script+json";

  private File currentFile;
  private boolean hasFileRetrieved = false;

  private static final String RANGE_PATTERN = "bytes=%d-%d";

  public GoogleDriveSourceClient(GoogleDriveSourceConfig config) {
    super(config);

  }

  @Override
  protected String getRequiredScope() {
    return READONLY_PERMISSIONS_SCOPE;
  }

  public boolean hasFile(String fileId) throws IOException {
    if (hasFileRetrieved) {
      return false;
    }
    Drive.Files.Get request = service.files().get(fileId).setFields("*");
    currentFile = request.execute();
    hasFileRetrieved = true;
    if (currentFile == null) {
      return false;
    }
    return true;
  }

  public FileFromFolder getFile() throws IOException {
    return getFilePartition(null, null);
  }

  public FileFromFolder getFilePartition(Long bytesFrom, Long bytesTo) throws IOException {
    FileFromFolder fileFromFolder;

    String mimeType = currentFile.getMimeType();
    if (!mimeType.startsWith(DRIVE_DOCS_MIME_PREFIX)) {
      long offset = bytesFrom == null ? 0L : bytesFrom;
      OutputStream outputStream = new ByteArrayOutputStream();
      Drive.Files.Get get = service.files().get(currentFile.getId());

      if (bytesFrom != null && bytesTo != null) {
        get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
        get.getRequestHeaders().setRange(String.format(RANGE_PATTERN, bytesFrom, bytesTo));
      }

      get.executeMediaAndDownloadTo(outputStream);
      fileFromFolder =
        new FileFromFolder(((ByteArrayOutputStream) outputStream).toByteArray(), offset, currentFile);
    } else if (mimeType.equals(DRIVE_DOCUMENTS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, config.getDocsExportingFormat());
    } else if (mimeType.equals(DRIVE_SPREADSHEETS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, config.getSheetsExportingFormat());
    } else if (mimeType.equals(DRIVE_DRAWINGS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, config.getDrawingsExportingFormat());
    } else if (mimeType.equals(DRIVE_PRESENTATIONS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, config.getPresentationsExportingFormat());
    } else if (mimeType.equals(DRIVE_APPS_SCRIPTS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, DEFAULT_APPS_SCRIPTS_EXPORT_MIME);
    } else {
      fileFromFolder =
        new FileFromFolder(new byte[]{}, bytesFrom, currentFile);
    }
    return fileFromFolder;
  }

  // We should separate binary and Google Drive formats between two requests
  public List<File> getFiles() throws InterruptedException {
    List<ExportedType> exportedTypes = new ArrayList<>(config.getFileTypesToPull());
    List<List<ExportedType>> exportedTypeGroups = new ArrayList<>();
    if (exportedTypes.contains(ExportedType.BINARY)) {
      exportedTypeGroups.add(Collections.singletonList(ExportedType.BINARY));

      exportedTypes.remove(ExportedType.BINARY);
      exportedTypeGroups.add(exportedTypes);
    }
    List<File> files = new ArrayList<>();
    for (List<ExportedType> group : exportedTypeGroups) {
      if (!group.isEmpty()) {
        files.addAll(getFiles(group));
      }
    }
    return files;
  }

  public List<File> getFiles(List<ExportedType> exportedTypes) throws InterruptedException {
    try {
      List<File> files = new ArrayList<>();
      String nextToken = "";
      Drive.Files.List request = service.files().list()
        .setQ(generateFilter(exportedTypes))
        .setFields("nextPageToken, files(id, size)");
      while (nextToken != null) {
        FileList result = request.execute();
        files.addAll(result.getFiles());
        nextToken = result.getNextPageToken();
        request.setPageToken(nextToken);
      }
      return files;
    } catch (IOException | InterruptedException e) {
      throw new InterruptedException(e.toString());
    }
  }


  // Google Drive API does not support partitioning for exporting Google Docs
  private FileFromFolder exportGoogleDocFile(Drive service, File currentFile, String exportFormat) throws IOException {
    OutputStream outputStream = new ByteArrayOutputStream();
    service.files().export(currentFile.getId(), exportFormat).executeMediaAndDownloadTo(outputStream);
    currentFile.setMimeType(exportFormat);
    return new FileFromFolder(((ByteArrayOutputStream) outputStream).toByteArray(), 0L, currentFile);
  }

  private String generateFilter(List<ExportedType> exportedTypes) throws InterruptedException {
    StringBuilder sb = new StringBuilder();

    // prepare parent
    sb.append("'");
    sb.append(config.getDirectoryIdentifier());
    sb.append("' in parents");

    // prepare query for non folders
    sb.append(" and mimeType != '");
    sb.append(DRIVE_FOLDER_MIME);
    sb.append("'");

    if (!exportedTypes.isEmpty()) {
      sb.append(" and (");
      for (ExportedType exportedType : exportedTypes) {
        if (exportedType.equals(ExportedType.BINARY)) {
          sb.append(" not mimeType contains '");
          sb.append(exportedType.getRelatedMIME());
          sb.append("' or");
        } else {
          sb.append(" mimeType = '");
          sb.append(exportedType.getRelatedMIME());
          sb.append("' or");
        }
      }
      // delete last 'or'
      sb.delete(sb.length() - 3, sb.length());
      sb.append(")");
    }

    String filter = config.getFilter();
    if (!Strings.isNullOrEmpty(filter)) {
      sb.append(" and ");
      sb.append(filter);
    }

    DateRange modifiedDateRange = ModifiedDateRangeUtils.getDataRange(config.getModificationDateRangeType(),
                                                                      config.getStartDate(),
                                                                      config.getEndDate());
    if (modifiedDateRange != null) {
      sb.append(" and ");
      sb.append(ModifiedDateRangeUtils.getFilterValue(modifiedDateRange));
    }

    return sb.toString();
  }
}
