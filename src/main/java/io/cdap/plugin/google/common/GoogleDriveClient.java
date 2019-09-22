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

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Base client for working with Google Drive API
 *
 * @param <C> configuration
 */
public class GoogleDriveClient<C extends GoogleDriveBaseConfig> implements Closeable {

  public static final String MODIFIED_TIME_TERM = "modifiedTime";

  public static final String DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder";
  public static final String DRIVE_DOCS_MIME_PREFIX = "application/vnd.google-apps.";
  public static final String DRIVE_DOCUMENTS_MIME = "application/vnd.google-apps.document";
  public static final String DRIVE_SPREADSHEETS_MIME = "application/vnd.google-apps.spreadsheet";
  public static final String DRIVE_DRAWINGS_MIME = "application/vnd.google-apps.drawing";
  public static final String DRIVE_PRESENTATIONS_MIME = "application/vnd.google-apps.presentation";
  public static final String DRIVE_APPS_SCRIPTS_MIME = "application/vnd.google-apps.script";

  public static final String DEFAULT_APPS_SCRIPTS_EXPORT_MIME = "application/vnd.google-apps.script+json";

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  protected static Drive service;
  protected final C config;
  private static NetHttpTransport httpTransport;

  public GoogleDriveClient(C config) {
    this.config = config;
  }


  protected void initialize() throws GeneralSecurityException, IOException, InterruptedException {
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
    // no-op
  }

  private Credential getCredentials(NetHttpTransport httpTransport) throws IOException, InterruptedException {

    // TODO fix authentication after OAuth2 will be provided by cdap
    // So for now we use Access Token property for all needed credentials transmitting in following format:
    // <clientId>;<clientSecret>;<refreshToken>
    // start of workaround
    String credentialsString = config.getAccessToken();
    String[] parts = credentialsString.split(";");
    if (parts.length != 3) {
      throw new InterruptedException("No enough content for accessToken, please populate " +
                                       "<clientId>;<clientSecret>;<refreshToken> there");
    }
    String clientId = parts[0];
    String clientSecret = parts[1];
    String refreshToken = parts[2];
    // end of workaround

    GoogleCredential credential = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(JSON_FACTORY)
      .setClientSecrets(clientId,
                        clientSecret)
      .build();
    credential.setRefreshToken(refreshToken);

    return credential;
  }
}
