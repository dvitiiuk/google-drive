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
import io.cdap.plugin.google.common.FileFromFolder;
import io.cdap.plugin.google.common.GoogleDriveClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

/**
 * Client for getting data via Google Drive API.
 */
public class GoogleDriveSourceClient extends GoogleDriveClient<GoogleDriveSourceConfig> {
  private File currentFile;
  private boolean hasFileRetrieved = false;

  public GoogleDriveSourceClient(GoogleDriveSourceConfig config) {
    super(config);
  }

  public boolean hasFile(String fileId) throws IOException, InterruptedException {
    if (hasFileRetrieved) {
      return false;
    }
    try {
      initialize();
      Drive.Files.Get request = service.files().get(fileId).setFields("*");
      currentFile = request.execute();
      hasFileRetrieved = true;
      if (currentFile == null) {
        return false;
      }
      return true;
    } catch (GeneralSecurityException e) {
      throw new InterruptedException(e.toString());
    }
  }

  public FileFromFolder getFile() throws IOException, InterruptedException {
    return getFilePartition(null, null);
  }

  public FileFromFolder getFilePartition(Long bytesFrom, Long bytesTo) throws IOException, InterruptedException {
    FileFromFolder fileFromFolder;

    String mimeType = currentFile.getMimeType();
    if (!mimeType.startsWith("application/vnd.google-apps.")) {
      Long offset = bytesFrom == null ? 0L : bytesFrom;
      OutputStream outputStream = new ByteArrayOutputStream();
      Drive.Files.Get get = service.files().get(currentFile.getId());

      if (bytesFrom != null && bytesTo != null) {
        get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
        get.getRequestHeaders().setRange(String.format("bytes=%d-%d", bytesFrom, bytesTo));
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
    } else {
      fileFromFolder =
        new FileFromFolder(new byte[]{}, bytesFrom, currentFile);
    }
    return fileFromFolder;
  }

  public List<File> getFiles() throws InterruptedException {
    try {
      initialize();
      List<File> files = new ArrayList<>();
      String nextToken = "";
      Drive.Files.List request = service.files().list()
        .setQ(generateFilter())
        .setFields("nextPageToken, files(id, size)");
      while (nextToken != null) {
        FileList result = request.execute();
        files.addAll(result.getFiles());
        nextToken = result.getNextPageToken();
        request.setPageToken(nextToken);
      }
      return files;
    } catch (GeneralSecurityException e) {
      throw new InterruptedException(e.toString());
    } catch (IOException e) {
      throw new InterruptedException(e.toString());
    } catch (InterruptedException e) {
      throw new InterruptedException(e.toString());
    }
  }

  // TODO Google Drive API does not support partitioning exported Google Docs, implement external partitioning
  private FileFromFolder exportGoogleDocFile(Drive service, File currentFile, String exportFormat) throws IOException {
    OutputStream outputStream = new ByteArrayOutputStream();
    service.files().export(currentFile.getId(), exportFormat).executeMediaAndDownloadTo(outputStream);
    return new FileFromFolder(((ByteArrayOutputStream) outputStream).toByteArray(), 0L, currentFile);
  }

  private String generateFilter() throws InterruptedException {
    StringBuilder sb = new StringBuilder();

    // prepare parent
    sb.append("'");
    sb.append(config.getDirectoryIdentifier());
    sb.append("' in parents");

    // prepare query for non folders
    sb.append(" and mimeType != '");
    sb.append(DRIVE_FOLDER_MIME);
    sb.append("'");

    List<String> formats = config.getFileTypesToPull();
    if (!formats.isEmpty()) {
      sb.append(" and (");
      for (String format : formats) {
        if (format.equals("binary")) {
          sb.append(" not mimeType contains '");
          sb.append(mimeFromType(format));
          sb.append("' or");
        } else {
          sb.append(" mimeType = '");
          sb.append(mimeFromType(format));
          sb.append("' or");
        }
      }
      // delete last 'or'
      sb.delete(sb.length() - 3, sb.length());
      sb.append(")");
    }

    String filter = config.getFilter();
    if (filter != null && !filter.equals("")) {
      sb.append(" and ");
      sb.append(filter);
    }

    return sb.toString();
  }

  private String mimeFromType(String type) throws InterruptedException {
    switch (type) {
      case "binary":
        return DRIVE_DOCS_MIME_PREFIX;
      case "documents":
        return DRIVE_DOCUMENTS_MIME;
      case "spreadsheets":
        return DRIVE_SPREADSHEETS_MIME;
      case "drawings":
        return DRIVE_DRAWINGS_MIME;
      case "presentations":
        return DRIVE_PRESENTATIONS_MIME;
      case "appsScripts":
        return DRIVE_APPS_SCRIPTS_MIME;
      default:
        throw new InterruptedException("Invalid MIME type");
    }
  }
}
