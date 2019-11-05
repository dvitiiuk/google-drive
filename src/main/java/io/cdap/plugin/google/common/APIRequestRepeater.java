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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 * @param <I> dfg
 * @param <O> fdg
 */
public abstract class APIRequestRepeater<I extends APIRequest<O>, O> {
  private static final Logger LOG = LoggerFactory.getLogger(APIRequestRepeater.class);

  public O doRepeatable(I apiRequest) throws InterruptedException, IOException {
    int counter = 0;
    long waitTimeout = 1;
    while (counter < 10) {
      try {
        O result = apiRequest.doRequest();
        return result;
      } catch (GoogleJsonResponseException e) {
        if ((e.getDetails().getCode() == 429 && ("Too Many Requests".equals(e.getStatusMessage())
            || "Rate Limit Exceeded".equals(e.getStatusMessage()))) ||
            (e.getDetails().getCode() == 403 && "Rate Limit Exceeded".equals(e.getStatusMessage()))
            || (e.getDetails().getCode() == 500)
            || (e.getDetails().getCode() == 503)) {
          LOG.warn(String.format("Error: '%d', message: '%s'. Resources limit exhausted, " +
                  "will wait for '%d' seconds before next attempt " + apiRequest.getLog(),
              e.getDetails().getCode(),
              e.getStatusMessage(),
              waitTimeout));
          TimeUnit.SECONDS.sleep(waitTimeout);
          waitTimeout *= 2;
          counter++;
        } else {
          throw e;
        }
      }
    }
    throw new IllegalStateException(String.format("Resources limit exhausted after '%d' attempts", counter));
  }
}
