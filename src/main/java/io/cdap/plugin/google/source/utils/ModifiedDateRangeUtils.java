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

import io.cdap.plugin.google.source.GoogleDriveSourceClient;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.regex.Pattern;

/**
 * Builds data range from {@link io.cdap.plugin.google.source.GoogleDriveSourceConfig} instance
 */
public class ModifiedDateRangeUtils {

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS");
  private static final Pattern DATE_PATTERN =
    // RFC 3339 regex : year-month-dayT part
    Pattern.compile("^([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])" +
      // hour:minute:second part
                      "([Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)" +
      // .microseconds or partial-time
                      "(\\.[0-9]+)?(([Zz])|([\\+|\\-]([01][0-9]|2[0-3]):[0-5][0-9]))?)?$");

  public static DateRange getDataRange(ModifiedDateRangeType modifiedDateRangeType, String startDate, String endDate)
    throws InterruptedException {
    LocalDateTime now = LocalDateTime.now();
    switch (modifiedDateRangeType) {
      case NONE:
        return null;
      case LAST_7_DAYS:
        LocalDateTime weekBefore = now.minusWeeks(1);
        return convertFromLocalDateTimes(weekBefore, now);
      case LAST_30_DAYS:
        LocalDateTime monthBefore = now.minusDays(30);
        return convertFromLocalDateTimes(monthBefore, now);
      case PREVIOUS_QUARTER:
        LocalDateTime currentBeforeQuarter = now.minusMonths(3);
        LocalDateTime startOfPreviousQuarter = currentBeforeQuarter
          .with(currentBeforeQuarter.getMonth().firstMonthOfQuarter())
          .with(TemporalAdjusters.firstDayOfMonth())
          .toLocalDate().atTime(LocalTime.MIDNIGHT);
        LocalDateTime endOfPreviousQuarter = startOfPreviousQuarter.plusMonths(2)
          .with(TemporalAdjusters.lastDayOfMonth())
          .toLocalDate().atTime(LocalTime.MAX);
        return convertFromLocalDateTimes(startOfPreviousQuarter, endOfPreviousQuarter);
      case CURRENT_QUARTER:
        LocalDateTime startOfCurrentQuarter = now.with(now.getMonth().firstMonthOfQuarter())
          .with(TemporalAdjusters.firstDayOfMonth())
          .toLocalDate().atTime(LocalTime.MIDNIGHT);
        return convertFromLocalDateTimes(startOfCurrentQuarter, now);
      case LAST_YEAR:
        LocalDateTime startOfPreviousYear = now.minusYears(1).with(TemporalAdjusters.firstDayOfYear())
          .toLocalDate().atTime(LocalTime.MIDNIGHT);
        LocalDateTime endOfPreviousYear = now.minusYears(1).with(TemporalAdjusters.lastDayOfYear())
          .toLocalDate().atTime(LocalTime.MAX);
        return convertFromLocalDateTimes(startOfPreviousYear, endOfPreviousYear);
      case CURRENT_YEAR:
        LocalDateTime startOfYear = now.with(TemporalAdjusters.firstDayOfYear())
          .toLocalDate().atTime(LocalTime.MIDNIGHT);
        return convertFromLocalDateTimes(startOfYear, now);
      case CUSTOM:
        return new DateRange(startDate, endDate);
    }
    throw new InterruptedException("No valid modified date range was selected");
  }

  private static DateRange convertFromLocalDateTimes(LocalDateTime fromDateTime,
                                                     LocalDateTime toDateTime) {
    return new DateRange(fromDateTime.format(DATE_TIME_FORMATTER),
                         toDateTime.format(DATE_TIME_FORMATTER));
  }

  public static boolean isValidDateString(String dateString) {
    return DATE_PATTERN.matcher(dateString).matches();
  }

  public static String getFilterValue(DateRange dateRange) {
    if (dateRange == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(GoogleDriveSourceClient.MODIFIED_TIME_TERM);
    sb.append(">='");
    sb.append(dateRange.getStartDate());
    sb.append("' and ");
    sb.append(GoogleDriveSourceClient.MODIFIED_TIME_TERM);
    sb.append("<='");
    sb.append(dateRange.getEndDate());
    sb.append("'");
    return sb.toString();
  }
}
