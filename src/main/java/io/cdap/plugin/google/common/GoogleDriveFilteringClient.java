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

package io.cdap.plugin.google.common;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Strings;
import io.cdap.plugin.google.drive.source.utils.DateRange;
import io.cdap.plugin.google.drive.source.utils.ExportedType;
import io.cdap.plugin.google.drive.source.utils.ModifiedDateRangeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base Google Drive Class with files search functionality.
 * @param <C>
 */
public class GoogleDriveFilteringClient<C extends GoogleFilteringSourceConfig> extends GoogleDriveClient<C> {
  public static final String DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder";

  public GoogleDriveFilteringClient(C config) {
    super(config);
  }

  public List<File> getFilesSummary(List<ExportedType> exportedTypes) {
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
      throw new RuntimeException("Issue during retrieving summary for files.", e);
    }
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
                                                                      config.getStartDate(), config.getEndDate());
    if (modifiedDateRange != null) {
      sb.append(" and ");
      sb.append(ModifiedDateRangeUtils.getFilterValue(modifiedDateRange));
    }

    return sb.toString();
  }
}
