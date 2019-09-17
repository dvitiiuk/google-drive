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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

public abstract class GoogleDriveBaseConfig extends ReferencePluginConfig {
  public static final String APP_ID = "appId";
  public static final String ACCESS_TOKEN = "accessToken";
  public static final String DIRECTORY_IDENTIFIER = "directoryIdentifier";

  @Name(APP_ID)
  @Description("Oauth2 app id")
  @Macro
  protected String appId;

  @Name(ACCESS_TOKEN)
  @Description("OAuth2 access token.")
  @Macro
  protected String accessToken;

  @Name(DIRECTORY_IDENTIFIER)
  @Description("ID is the last part of the URL, such as https://drive.google.com/drive/folders/0B2kqcwp2ycGZanhSR3JmREw5VTV.")
  @Macro
  protected String directoryIdentifier;

  public GoogleDriveBaseConfig(String referenceName) {
    super(referenceName);
  }

  public void validate(FailureCollector collector) {
    if (!containsMacro(APP_ID)) {
      if (Strings.isNullOrEmpty(appId)) {
        collector.addFailure("appId  is empty or macro is not available",
                             "appId must be not empty")
          .withConfigProperty(APP_ID);
      }
    }
    if (!containsMacro(ACCESS_TOKEN)) {
      if (Strings.isNullOrEmpty(accessToken)) {
        collector.addFailure("accessToken is empty or macro is not available",
                             "accessToken must be not empty")
          .withConfigProperty(ACCESS_TOKEN);
      }
    }
    if (!containsMacro(DIRECTORY_IDENTIFIER)) {
      if (Strings.isNullOrEmpty(directoryIdentifier)) {
        collector.addFailure("directoryIdentifier is empty or macro is not available",
                             "directoryIdentifier must be not empty")
          .withConfigProperty(DIRECTORY_IDENTIFIER);
      }
    }
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public String getDirectoryIdentifier() {
    return directoryIdentifier;
  }

  public void setDirectoryIdentifier(String directoryIdentifier) {
    this.directoryIdentifier = directoryIdentifier;
  }
}
