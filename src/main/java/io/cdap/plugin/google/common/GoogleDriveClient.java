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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.common.io.Resources;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;

/**
 * Base client for working with Google Drive API
 *
 * @param <C> configuration
 */
public class GoogleDriveClient<C extends GoogleDriveBaseConfig> implements Closeable {

  protected static final String DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder";
  protected static final String DRIVE_DOCS_MIME_PREFIX = "application/vnd.google-apps.";
  protected static final String DRIVE_DOCUMENTS_MIME = "application/vnd.google-apps.document";
  protected static final String DRIVE_SPREADSHEETS_MIME = "application/vnd.google-apps.spreadsheet";
  protected static final String DRIVE_DRAWINGS_MIME = "application/vnd.google-apps.drawing";
  protected static final String DRIVE_PRESENTATIONS_MIME = "application/vnd.google-apps.presentation";
  protected static final String DRIVE_APPS_SCRIPTS_MIME = "application/vnd.google-apps.script";

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  protected static Drive service;
  protected final C config;
  private NetHttpTransport httpTransport;

  public GoogleDriveClient(C config) {
    this.config = config;
  }


  protected void initialize() throws GeneralSecurityException, IOException {
    if (httpTransport == null) {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    }
    if (service == null) {
      service = new Drive.Builder(httpTransport, JSON_FACTORY, getCredentials(httpTransport))
        .setApplicationName(config.getAppId())
        .build();
    }
  }

  @Override
  public void close() throws IOException {
    // TODO ??
  }

  private Credential getCredentials(NetHttpTransport httpTransport) throws IOException {

    // workaround for OAuth2
    URL url = Resources.getResource("cred.pass");
    String text = Resources.toString(url, Charset.forName("UTF-8"));
    String[] creds = text.split(";");

    GoogleCredential credential = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(JSON_FACTORY)
      .setClientSecrets(creds[0],
                        creds[1])
      .build();
    credential.setAccessToken(config.getAccessToken());
    credential.setRefreshToken(creds[2]);

    return credential;
  }
}
