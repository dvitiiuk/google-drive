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

package io.cdap.plugin.google.sheets.sink;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZonedDateTime;

public class StructuredRecordToSheetTransformerTest {

  @Test
  public void testToSheetsDate() {
    LocalDate testDate = LocalDate.of(2020, 01, 11);
    StructuredRecordToSheetTransformer transformer = new StructuredRecordToSheetTransformer("", "", "", null);
    Double expected = transformer.toSheetsDate(testDate);
    Assert.assertEquals(Double.valueOf(43841.0), expected);
  }

  @Test
  public void testToSheetsDateTime() {
    ZonedDateTime testZonedDateTime = ZonedDateTime.of(1991, 03, 8, 13, 54, 20, 0,
        StructuredRecordToSheetTransformer.UTC_ZONE_ID);
    StructuredRecordToSheetTransformer transformer = new StructuredRecordToSheetTransformer("", "", "", null);
    Double expected = transformer.toSheetsDateTime(testZonedDateTime);
    Assert.assertEquals(Double.valueOf(33305.57939814815), expected, Math.pow(10, -11));
  }
}
