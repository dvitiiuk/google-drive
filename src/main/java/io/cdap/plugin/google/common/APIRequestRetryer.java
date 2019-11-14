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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class APIRequestRetryer {
  private static final Logger LOG = LoggerFactory.getLogger(APIRequestRetryer.class);
  protected static final int TOO_MANY_REQUESTS_CODE = 429;
  protected static final int LIMIT_RATE_EXCEEDED_CODE = 403;
  protected static final int BACKEND_ERROR_CODE = 500;
  protected static final int SERVICE_UNAVAILABLE_CODE = 503;
  protected static final String TOO_MANY_REQUESTS_MESSAGE = "Too Many Requests";
  protected static final String LIMIT_RATE_EXCEEDED_MESSAGE = "Rate Limit Exceeded";

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
        .retryIfException(APIRequestRetryer::checkThrowable)
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
    List<String> possibleMessages = Arrays.asList(TOO_MANY_REQUESTS_MESSAGE, LIMIT_RATE_EXCEEDED_MESSAGE);
    return e.getDetails().getCode() == TOO_MANY_REQUESTS_CODE && possibleMessages.contains(e.getStatusMessage());
  }

  private static boolean isRateLimitError(GoogleJsonResponseException e) {
    List<String> possibleMessages = Collections.singletonList(LIMIT_RATE_EXCEEDED_MESSAGE);
    return e.getDetails().getCode() == LIMIT_RATE_EXCEEDED_CODE && possibleMessages.contains(e.getStatusMessage());
  }

  private static boolean isBackendError(GoogleJsonResponseException e) {
    return e.getDetails().getCode() == BACKEND_ERROR_CODE;
  }

  private static boolean isServiceUnavailableError(GoogleJsonResponseException e) {
    return e.getDetails().getCode() == SERVICE_UNAVAILABLE_CODE;
  }

  /**
   * Default exponential strategy {@link com.github.rholder.retry.WaitStrategies.ExceptionWaitStrategy} starts
   * from multiplier 2 instead of 1.
   */
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
