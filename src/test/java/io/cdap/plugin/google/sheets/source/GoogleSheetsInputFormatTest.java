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

package io.cdap.plugin.google.sheets.source;

import io.cdap.plugin.google.sheets.source.utils.SheetsToPull;
import org.easymock.EasyMock;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;

/**
 *
 */
public class GoogleSheetsInputFormatTest {
  private GoogleSheetsInputFormat googleSheetsInputFormat = new GoogleSheetsInputFormat();

  @Test
  public void testGetSplitsFromFilesAllSheets() throws NoSuchMethodException {
    Method getSplitsFromFiles = GoogleSheetsInputFormat.class.getDeclaredMethod("getSplitsFromFiles",
        GoogleSheetsSourceConfig.class, List.class, LinkedHashMap.class);
    getSplitsFromFiles.setAccessible(true);

    final int firstDataRow = 2;
    final int lastDataRow = 2;

    GoogleSheetsSourceConfig sheetsSourceConfig = EasyMock.createMock(GoogleSheetsSourceConfig.class);

    EasyMock.expect(sheetsSourceConfig.getActualFirstDataRow()).andReturn(firstDataRow);
    EasyMock.expect(sheetsSourceConfig.getActualLastDataRow()).andReturn(lastDataRow);
    EasyMock.expect(sheetsSourceConfig.getSheetsToPull()).andReturn(SheetsToPull.ALL);


  }
}
