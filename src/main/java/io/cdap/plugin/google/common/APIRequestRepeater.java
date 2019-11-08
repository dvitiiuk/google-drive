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

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class APIRequestRepeater {
  private static final Logger LOG = LoggerFactory.getLogger(APIRequestRepeater.class);

  public static <T> Retryer<T> getRetryer(GoogleRetryingConfig config, String operationDescription) {
    RetryListener listener = new RetryListener() {
      @Override
      public <V> void onRetry(Attempt<V> attempt) {
        if (attempt.hasException()) {
          if (attempt.getExceptionCause() instanceof GoogleJsonResponseException) {
            GoogleJsonResponseException e = (GoogleJsonResponseException) attempt.getExceptionCause();
            LOG.warn(String.format(
                "Error code: '%d', message: '%s'. Attempt: '%d'. Delay since first: '%d'. Description: " +
                    operationDescription,
                e.getDetails().getCode(),
                e.getStatusMessage(),
                attempt.getAttemptNumber(),
                attempt.getDelaySinceFirstAttempt()));
          }
        }
      }
    };
    return RetryerBuilder.<T>newBuilder()
        .retryIfException(APIRequestRepeater::checkThrowable)
        .retryIfExceptionOfType(SocketTimeoutException.class)
        .withWaitStrategy(WaitStrategies.join(
            new TrueExponentialWaitStrategy(1000, TimeUnit.SECONDS.toMillis(config.getMaxRetryWait())),
            WaitStrategies.randomWait(config.getMaxRetryJitterWait(), TimeUnit.MILLISECONDS)))
        .withStopStrategy(StopStrategies.stopAfterAttempt(config.getMaxRetryCount()))
        .withRetryListener(listener)
        .build();
  }

  private static boolean checkThrowable(Throwable t) {
    if (t instanceof GoogleJsonResponseException) {
      GoogleJsonResponseException e = (GoogleJsonResponseException) t;
      return isTooManyRequestsError(e) || isRateLimitError(e)
          || isBackendError(e) || isServiceUnavailableError(e);
    }
    return false;
  }

  private static boolean isTooManyRequestsError(GoogleJsonResponseException e) {
    List<String> possibleMessages = Arrays.asList("Too Many Requests", "Rate Limit Exceeded");
    int code = 429;
    return e.getDetails().getCode() == code && possibleMessages.contains(e.getStatusMessage());
  }

  private static boolean isRateLimitError(GoogleJsonResponseException e) {
    List<String> possibleMessages = Arrays.asList("Rate Limit Exceeded");
    int code = 403;
    return e.getDetails().getCode() == code && possibleMessages.contains(e.getStatusMessage());
  }

  private static boolean isBackendError(GoogleJsonResponseException e) {
    int code = 500;
    return e.getDetails().getCode() == code;
  }

  private static boolean isServiceUnavailableError(GoogleJsonResponseException e) {
    int code = 503;
    return e.getDetails().getCode() == code;
  }

  private static class TrueExponentialWaitStrategy implements WaitStrategy {

    private final long multiplier;
    private final long maximumWait;

    TrueExponentialWaitStrategy(long multiplier,
                                   long maximumWait) {
      Preconditions.checkArgument(multiplier > 0L,
          "multiplier must be > 0 but is %d", multiplier);
      Preconditions.checkArgument(maximumWait >= 0L,
          "maximumWait must be >= 0 but is %d", maximumWait);
      Preconditions.checkArgument(multiplier < maximumWait,
          "multiplier must be < maximumWait but is %d", multiplier);
      this.multiplier = multiplier;
      this.maximumWait = maximumWait;
    }

    @Override
    public long computeSleepTime(Attempt failedAttempt) {
      double exp = Math.pow(2, failedAttempt.getAttemptNumber() - 1);
      long result = Math.round(multiplier * exp);
      if (result > maximumWait) {
        result = maximumWait;
      }
      return result >= 0L ? result : 0L;
    }
  }
}
