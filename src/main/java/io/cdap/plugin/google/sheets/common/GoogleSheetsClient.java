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

package io.cdap.plugin.google.sheets.common;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import io.cdap.plugin.google.common.GoogleAuthBaseConfig;
import io.cdap.plugin.google.common.GoogleDriveClient;

/**
 * Base client for working with Google Drive API
 *
 * @param <C> configuration
 */
public abstract class GoogleSheetsClient<C extends GoogleAuthBaseConfig> extends GoogleDriveClient {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  protected static final String FULL_PERMISSIONS_SCOPE = SheetsScopes.SPREADSHEETS;
  protected static final String READONLY_PERMISSIONS_SCOPE = SheetsScopes.SPREADSHEETS_READONLY;
  protected Sheets service;
  protected Drive drive;

  public GoogleSheetsClient(C config) {
    super(config);
    service = new Sheets.Builder(httpTransport, JSON_FACTORY, getCredentials())
      .build();
    drive = new Drive.Builder(httpTransport, JSON_FACTORY, getCredentials())
      .build();
  }

  protected abstract String getRequiredScope();
}
