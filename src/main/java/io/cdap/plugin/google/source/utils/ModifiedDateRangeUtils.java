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

package io.cdap.plugin.google.source.utils;

import io.cdap.plugin.google.common.GoogleDriveClient;
import io.cdap.plugin.google.source.GoogleDriveSourceConfig;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Builds data range from {@link io.cdap.plugin.google.source.GoogleDriveSourceConfig} instance
 */
public class ModifiedDateRangeUtils {

  // TODO cover with tests
  private static final Pattern DATE_PATTERN =
    // RFC 3339 regex : year-month-dayT part
    Pattern.compile("^([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]" +
      // hour:minute:second part
                      "([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)" +
      // .microseconds or partial-time
                      "(\\.[0-9]+)?(([Zz])|([\\+|\\-]([01][0-9]|2[0-3]):[0-5][0-9]))$\n");

  // TODO cover with tests
  public static DateRange getDataRange(GoogleDriveSourceConfig config) throws InterruptedException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    LocalDateTime now = LocalDateTime.now();
    switch (config.getModificationDateRangeType()) {
      case NONE:
        return null;
      case LAST_7_DAYS:
        LocalDateTime weekBefore = now.minusWeeks(1);
        return convertFromLocalDateTimes(dateFormat, weekBefore, now);
      case LAST_30_DAYS:
        LocalDateTime monthBefore = now.minusDays(30);
        return convertFromLocalDateTimes(dateFormat, monthBefore, now);
      case PREVIOUS_QUARTER:
        // TODO complete this
      case CURRENT_QUARTER:
        // TODO complete this
      case LAST_YEAR:
        LocalDateTime startOfPreviousYear = now.minusYears(1).with(TemporalAdjusters.firstDayOfYear())
          .toLocalDate().atTime(LocalTime.MIDNIGHT);
        LocalDateTime endOfPreviousYear = now.minusYears(1).with(TemporalAdjusters.lastDayOfYear())
          .toLocalDate().atTime(LocalTime.MIDNIGHT);
        return convertFromLocalDateTimes(dateFormat, startOfPreviousYear, endOfPreviousYear);
      case CURRENT_YEAR:
        LocalDateTime startOfYear = now.with(TemporalAdjusters.firstDayOfYear())
          .toLocalDate().atTime(LocalTime.MIDNIGHT);
        return convertFromLocalDateTimes(dateFormat, startOfYear, now);
      case CUSTOM:
        return new DateRange(config.getStartDate(), config.getEndDate());
    }
    throw new InterruptedException("No valid modified date range was selected");
  }

  private static DateRange convertFromLocalDateTimes(SimpleDateFormat dateFormat, LocalDateTime fromDateTime,
                                                     LocalDateTime toDateTime) {
    return new DateRange(dateFormat.format(Date.from(fromDateTime.atZone(ZoneId.systemDefault())
                                                             .toInstant())),
                         dateFormat.format(Date.from(toDateTime.atZone(ZoneId.systemDefault())
                                                             .toInstant())));
  }

  public static boolean isValidDateString(String dateString) {
    return DATE_PATTERN.matcher(dateString).matches();
  }

  public static String getFilterValue(DateRange dateRange) {
    if (dateRange == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(GoogleDriveClient.MODIFIED_TIME_TERM);
    sb.append(">='");
    sb.append(dateRange.getStartDate());
    sb.append("' and ");
    sb.append(GoogleDriveClient.MODIFIED_TIME_TERM);
    sb.append("<='");
    sb.append(dateRange.getEndDate());
    sb.append("'");
    return sb.toString();
  }
}
