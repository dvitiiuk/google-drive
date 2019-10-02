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

package io.cdap.plugin.google.source.io.cdap.plugin.google.utils;

import io.cdap.plugin.google.source.utils.DateRange;
import io.cdap.plugin.google.source.utils.ModifiedDateRangeType;
import io.cdap.plugin.google.source.utils.ModifiedDateRangeUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.LocalDateTime;
import java.time.Month;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LocalDateTime.class, ModifiedDateRangeUtils.class})
public class ModifiedDateRangeUtilsTest {

  @Test
  public void testDateRangeFormatValidation() {
    String dateString0 = "2012-06-04";
    assertTrue(ModifiedDateRangeUtils.isValidDateString(dateString0));

    String dateString1 = "2012-06-04T19:45:45";
    assertTrue(ModifiedDateRangeUtils.isValidDateString(dateString1));

    String dateString2 = "2012-06-04T19:45:45.456";
    assertTrue(ModifiedDateRangeUtils.isValidDateString(dateString2));

    String dateString3 = "2012-06-04T12:00:00-08:00";
    assertTrue(ModifiedDateRangeUtils.isValidDateString(dateString3));
  }

  @Test
  public void testGetDateRange() throws Exception {
    LocalDateTime testCurrentDateTime =
      LocalDateTime.of(2019, Month.SEPTEMBER, 19, 19, 52, 13, 456000000);
    PowerMockito.mockStatic(LocalDateTime.class);
    PowerMockito.when(LocalDateTime.now()).thenReturn(testCurrentDateTime);

    DateRange dateRange =
      ModifiedDateRangeUtils.getDataRange(ModifiedDateRangeType.NONE, "", "");

    assertNull(dateRange);

    dateRange =
      ModifiedDateRangeUtils.getDataRange(ModifiedDateRangeType.LAST_7_DAYS, "", "");

    assertEquals("2019-09-12T19:52:13.456", dateRange.getStartDate());
    assertEquals("2019-09-19T19:52:13.456", dateRange.getEndDate());

    dateRange =
      ModifiedDateRangeUtils.getDataRange(ModifiedDateRangeType.LAST_30_DAYS, "", "");

    assertEquals("2019-08-20T19:52:13.456", dateRange.getStartDate());
    assertEquals("2019-09-19T19:52:13.456", dateRange.getEndDate());

    dateRange =
      ModifiedDateRangeUtils.getDataRange(ModifiedDateRangeType.PREVIOUS_QUARTER, "", "");

    assertEquals("2019-04-01T00:00:00.000", dateRange.getStartDate());
    assertEquals("2019-06-30T23:59:59.999", dateRange.getEndDate());

    dateRange =
      ModifiedDateRangeUtils.getDataRange(ModifiedDateRangeType.CURRENT_QUARTER, "", "");

    assertEquals("2019-07-01T00:00:00.000", dateRange.getStartDate());
    assertEquals("2019-09-19T19:52:13.456", dateRange.getEndDate());

    dateRange =
      ModifiedDateRangeUtils.getDataRange(ModifiedDateRangeType.LAST_YEAR, "", "");

    assertEquals("2018-01-01T00:00:00.000", dateRange.getStartDate());
    assertEquals("2018-12-31T23:59:59.999", dateRange.getEndDate());

    dateRange =
      ModifiedDateRangeUtils.getDataRange(ModifiedDateRangeType.CURRENT_YEAR, "", "");

    assertEquals("2019-01-01T00:00:00.000", dateRange.getStartDate());
    assertEquals("2019-09-19T19:52:13.456", dateRange.getEndDate());

    String customStartDate = "2018-01-01T01:00:00.045";
    String customEndDate = "2018-01-01T05:45:013.070";
    dateRange =
      ModifiedDateRangeUtils.getDataRange(ModifiedDateRangeType.CUSTOM, customStartDate, customEndDate);

    assertEquals(customStartDate, dateRange.getStartDate());
    assertEquals(customEndDate, dateRange.getEndDate());
  }
}
