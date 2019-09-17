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
import io.cdap.plugin.google.FilesFromFolder;
import io.cdap.plugin.google.common.GoogleDriveClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

public class GoogleDriveSourceClient extends GoogleDriveClient<GoogleDriveSourceConfig> {

  public GoogleDriveSourceClient(GoogleDriveSourceConfig config) {
    super(config);
  }

  public FilesFromFolder getFiles() throws IOException, InterruptedException {
    FilesFromFolder filesFromFolder = null;
    try {
      initialize();

      Drive.Files.List filesRequest = service.files().list()
        .setQ("'" + config.getDirectoryIdentifier() + "' in parents");
      String nextToken = "";
      List<FileFromFolder> filesFromFolderList = new ArrayList<>();
      while (nextToken != null) {
        FileList result = filesRequest.execute();
        List<File> files = result.getFiles();
        nextToken = result.getNextPageToken();
        filesRequest.setPageToken(nextToken);
        if (files == null || files.isEmpty()) {
          System.out.println("No files found.");
        } else {
          System.out.printf("                  Sub Files (%s):\n", files.size());
          for (File file : files) {
            String mimeType = file.getMimeType();
            String fileName = file.getName();
            if (!mimeType.startsWith("application/vnd.google-apps.")) {
              OutputStream outputStream = new ByteArrayOutputStream();
              service.files().get(file.getId()).executeMediaAndDownloadTo(outputStream);
              FileFromFolder fileFromFolder =
                new FileFromFolder(((ByteArrayOutputStream) outputStream).toByteArray(),
                                   mimeType, fileName);
              filesFromFolderList.add(fileFromFolder);
            }
          }
        }
      }
      filesFromFolder = new FilesFromFolder(filesFromFolderList);
    } catch (GeneralSecurityException e) {
      throw new InterruptedException(e.toString());
    }
    return filesFromFolder;
  }
}
