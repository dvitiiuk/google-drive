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

import io.cdap.plugin.google.common.AuthType;
import io.cdap.plugin.google.sheets.common.MultipleRowsRecord;
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GoogleSheetsSinkClientTest {

  @Test
  public void testPrepareContentFlatHeaders() throws IOException {
    List<String> headerNames = Arrays.asList("h0", "h1", "h2", "h3");
    GoogleSheetsSinkConfig sinkConfig = EasyMock.createMock(GoogleSheetsSinkConfig.class);
    EasyMock.expect(sinkConfig.getAuthType()).andReturn(AuthType.OAUTH2).anyTimes();
    EasyMock.expect(sinkConfig.getRefreshToken()).andReturn("dsfdsfdsfdsf").anyTimes();
    EasyMock.expect(sinkConfig.getClientSecret()).andReturn("dsfdsfdsfdsf").anyTimes();
    EasyMock.expect(sinkConfig.getClientId()).andReturn("dsdsrfegvrb").anyTimes();
    EasyMock.expect(sinkConfig.isWriteSchema()).andReturn(true).anyTimes();

    EasyMock.replay(sinkConfig);
    GoogleSheetsSinkClient sinkClient = new GoogleSheetsSinkClient(sinkConfig);

    ComplexHeader complexHeader = new ComplexHeader("root");
    headerNames.stream().forEach(h -> complexHeader.addHeader(new ComplexHeader(h)));
    MultipleRowsRecord rowsRecord = new MultipleRowsRecord("spreadsheetName", "sheetTitle",
      complexHeader, Collections.emptyList(), Collections.emptyList());

    /*Method prepareContentRequestMethod = GoogleSheetsSinkClient.class.getDeclaredMethod("prepareContentRequest",
      Integer.class, MultipleRowsRecord.class, boolean.class);
    prepareContentRequestMethod.setAccessible(true);

    Request request = (Request) prepareContentRequestMethod.invoke(sinkClient, 0, rowsRecord, true);

    List<RowData> rows = request.getAppendCells().getRows();
    Assert.assertEquals(1, rows.size());

    RowData headersRow = rows.get(0);
    Assert.assertEquals(headerNames.size(), headersRow.getValues().size());

    for (int i = 0; i < headerNames.size(); i++) {
      Assert.assertEquals(headerNames.get(i), headersRow.getValues().get(i).getUserEnteredValue().getStringValue());
    }*/
  }
}
