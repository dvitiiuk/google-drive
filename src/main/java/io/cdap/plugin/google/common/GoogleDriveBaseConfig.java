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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;

import java.io.IOException;

/**
 * Base Google Drive batch config. Contains common configuration properties and methods.
 */
public abstract class GoogleDriveBaseConfig extends PluginConfig {
  public static final String REFERENCE_NAME = "referenceName";
  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String REFRESH_TOKEN = "refreshToken";
  public static final String ACCESS_TOKEN = "accessToken";
  public static final String DIRECTORY_IDENTIFIER = "directoryIdentifier";

  private static final String IS_VALID_FAILURE_MESSAGE_PATTERN = "%s has invalid value %s";
  private static final String IS_SET_FAILURE_MESSAGE_PATTERN = "'%s' property is empty or macro is not available";
  private static final String CHECK_CORRECTIVE_MESSAGE_PATTERN = "Enter valid '%s' property";

  @Name(REFERENCE_NAME)
  @Description("Reference Name")
  protected String referenceName;

  // TODO remove these properties after OAuth2 will be provided by cdap
  // start of workaround
  @Name(CLIENT_ID)
  @Description("OAuth2 client id.")
  @Macro
  protected String clientId;

  @Name(CLIENT_SECRET)
  @Description("OAuth2 client secret.")
  @Macro
  protected String clientSecret;

  @Name(REFRESH_TOKEN)
  @Description("OAuth2 refresh token.")
  @Macro
  protected String refreshToken;
  // end of workaround

  @Name(ACCESS_TOKEN)
  @Description("OAuth2 access token.")
  @Macro
  protected String accessToken;

  @Name(DIRECTORY_IDENTIFIER)
  @Description("ID of target directory, the last part of the URL.")
  @Macro
  protected String directoryIdentifier;

  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);

    try {
      GoogleDriveClient client = new GoogleDriveClient(this);

      // validate auth
      validateCredentials(collector, client);

      // validate directory
      validateDirectoryIdentifier(collector, client);

    } catch (Exception e) {
      collector.addFailure(
        String.format("Exception during authentication/directory properties check: %s", e.getMessage()),
        "Check message and reconfigure the plugin")
        .withStacktrace(e.getStackTrace());
    }

  }

  private void validateCredentials(FailureCollector collector, GoogleDriveClient driveClient) throws IOException {
    if (!containsMacro(CLIENT_ID) && !containsMacro(CLIENT_SECRET) && !containsMacro(REFRESH_TOKEN)
      && !containsMacro(ACCESS_TOKEN)) {
      try {
        driveClient.checkRootFolder();
      } catch (GoogleJsonResponseException e) {
        collector.addFailure(e.getMessage(), "Provide valid credentials")
          .withConfigProperty(CLIENT_ID)
          .withConfigProperty(CLIENT_SECRET)
          .withConfigProperty(REFRESH_TOKEN)
          .withConfigProperty(ACCESS_TOKEN)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  private void validateDirectoryIdentifier(FailureCollector collector, GoogleDriveClient driveClient)
    throws IOException {
    if (!containsMacro(DIRECTORY_IDENTIFIER)) {
      try {
        driveClient.isFolderAccessible(directoryIdentifier);
      } catch (GoogleJsonResponseException e) {
        collector.addFailure(e.getMessage(), "Provide an existing folder identifier")
          .withConfigProperty(DIRECTORY_IDENTIFIER)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  protected boolean checkPropertyIsSet(FailureCollector collector, String propertyValue, String propertyName,
                                       String propertyLabel) {
    if (!containsMacro(propertyName)) {
      if (Strings.isNullOrEmpty(propertyValue)) {
        collector.addFailure(getIsSetValidationFailedMessage(propertyLabel),
                             getValidationFailedCorrectiveAction(propertyLabel))
          .withConfigProperty(propertyName);
      } else {
        return true;
      }
    }
    return false;
  }

  protected void checkPropertyIsValid(FailureCollector collector, boolean isPropertyValid, String propertyName,
                                      String propertyValue, String propertyLabel) {
    if (isPropertyValid) {
      return;
    }
    collector.addFailure(String.format(IS_VALID_FAILURE_MESSAGE_PATTERN, propertyName, propertyValue),
                         getValidationFailedCorrectiveAction(propertyLabel))
      .withConfigProperty(propertyName);
  }

  protected void collectInvalidProperty(FailureCollector collector, String propertyName, String propertyValue,
                                        String propertyLabel) {
    checkPropertyIsValid(collector, false, propertyName, propertyValue, propertyLabel);
  }

  protected String getIsSetValidationFailedMessage(String propertyLabel) {
    return String.format(IS_SET_FAILURE_MESSAGE_PATTERN, propertyLabel);
  }

  protected String getValidationFailedCorrectiveAction(String propertyLabel) {
    return String.format(CHECK_CORRECTIVE_MESSAGE_PATTERN, propertyLabel);
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public String getDirectoryIdentifier() {
    return directoryIdentifier;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getRefreshToken() {
    return refreshToken;
  }
}
