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
import io.cdap.plugin.google.FileFromFolder;
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

  //private String nextToken = "";
  //private Queue<File> currentQueue = new ArrayDeque<>();
  //private Drive.Files.List currentFilesRequest;
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
        /*.setQ(/*"'" + config.getDirectoryIdentifier() + "' in parents and " +
                  "mimeType != '" + DRIVE_FOLDER_MIME + "' and " +
                  "id = '" + fileId + "'"
          generateFilter(fileId))*/
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


  /*public boolean hasFile() throws IOException, InterruptedException {
    currentFile = currentQueue.poll();
    if (currentFile == null) {
      try {
        initialize();

        if (currentFilesRequest == null) {
          currentFilesRequest = service.files().list()
            .setQ("'" + config.getDirectoryIdentifier() + "' in parents and " +
                    "mimeType != 'application/vnd.google-apps.folder'")
            .setFields("*");
        }

        if (nextToken == null) {
          return false;
        }

        FileList result = currentFilesRequest.execute();
        List<File> files = result.getFiles();
        nextToken = result.getNextPageToken();
        currentFilesRequest.setPageToken(nextToken);

        if (files == null || files.isEmpty()) {
          System.out.println("No files found.");
        } else {
          currentQueue.addAll(files);
          currentFile = currentQueue.poll();
        }
      } catch (GeneralSecurityException e) {
        throw new InterruptedException(e.toString());
      }
    }
    if (currentFile == null) {
      return false;
    }

    return true;
  }*/

  public FileFromFolder getFile() throws IOException, InterruptedException {
    FileFromFolder fileFromFolder;

    String mimeType = currentFile.getMimeType();
    if (!mimeType.startsWith("application/vnd.google-apps.")) {
      OutputStream outputStream = new ByteArrayOutputStream();
      service.files().get(currentFile.getId()).executeMediaAndDownloadTo(outputStream);
      fileFromFolder =
        new FileFromFolder(((ByteArrayOutputStream) outputStream).toByteArray(), currentFile);
    } else if (mimeType.startsWith(DRIVE_DOCUMENTS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, config.getDocsExportingFormat());
    } else if (mimeType.startsWith(DRIVE_SPREADSHEETS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, config.getSheetsExportingFormat());
    } else if (mimeType.startsWith(DRIVE_DRAWINGS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, config.getDrawingsExportingFormat());
    } else if (mimeType.startsWith(DRIVE_PRESENTATIONS_MIME)) {
      fileFromFolder = exportGoogleDocFile(service, currentFile, config.getPresentationsExportingFormat());
    } else {
      fileFromFolder =
        new FileFromFolder(new byte[]{}, currentFile);
    }
    return fileFromFolder;
  }

  public List<File> getFiles() {
    try {
      initialize();
      List<File> files = new ArrayList<>();
      String nextToken = "";
      Drive.Files.List request = service.files().list()
        .setQ(/*"'" + config.getDirectoryIdentifier() + "' in parents and " +
                "mimeType != 'application/vnd.google-apps.folder'"*/
          generateFilter(null))
        .setFields("nextPageToken, files(id)");
      while (nextToken != null) {
        FileList result = request.execute();
        files.addAll(result.getFiles());
        nextToken = result.getNextPageToken();
        request.setPageToken(nextToken);
      }
      return files;
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e.toString());
    } catch (IOException e) {
      throw new RuntimeException(e.toString());
    } catch (InterruptedException e) {
      throw new RuntimeException(e.toString());
    }
  }

  private FileFromFolder exportGoogleDocFile(Drive service, File currentFile, String exportFormat) throws IOException {
    OutputStream outputStream = new ByteArrayOutputStream();
    service.files().export(currentFile.getId(), exportFormat).executeMediaAndDownloadTo(outputStream);
    return new FileFromFolder(((ByteArrayOutputStream) outputStream).toByteArray(), currentFile);
  }

  private String generateFilter(String fileId) throws InterruptedException {
    StringBuilder sb = new StringBuilder();

    // prepare parent
    sb.append("'");
    sb.append(config.getDirectoryIdentifier());
    sb.append("' in parents");

    // prepare query for non folders
    sb.append(" and mimeType != '");
    sb.append(DRIVE_FOLDER_MIME);
    sb.append("'");

    // add fileId if needed
    if (fileId != null) {
      sb.append(" and fileId = '");
      sb.append(fileId);
      sb.append("'");
    }

    List<String> formats = config.getFileTypesToPull();
    if (!formats.isEmpty()) {
      sb.append(" and (");
      for (String format : formats) {
        if (format.equals("binary")) {
          sb.append("not mimeType contains '");
          sb.append(mimeFromType(format));
          sb.append("' or");
        } else {
          sb.append("mimeType = '");
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
        throw new InterruptedException("Invalid MIME");
    }
  }
}
