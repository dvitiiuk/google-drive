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

public class GoogleDriveClient<C extends GoogleDriveBaseConfig> implements Closeable {

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static NetHttpTransport HTTP_TRANSPORT;
  protected static Drive service;

  protected final C config;

  public GoogleDriveClient(C config) {
    this.config = config;
  }


  protected void initialize() throws GeneralSecurityException, IOException {
    if (HTTP_TRANSPORT == null) {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
    }
    if (service == null) {
      service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
        .setApplicationName(config.getAppId())
        .build();
    }
  }

  @Override
  public void close() throws IOException {

  }

  private Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) {
    GoogleCredential credential = new GoogleCredential.Builder()
        .setTransport(HTTP_TRANSPORT)
        .setJsonFactory(JSON_FACTORY)
        .setClientSecrets("917948624966-312023rn3t26prgla0fund1egj7k9rmn.apps.googleusercontent.com", "FFuA68r-IouEJk7FDJ0G3WGS")
        .build();
    credential.setAccessToken(config.getAccessToken());
    credential.setRefreshToken("1/GpX8kLEM73r4z-0ZtfWioI7dP2YlVEKkwuSs9eTaYTo");

    return credential;
  }
}
