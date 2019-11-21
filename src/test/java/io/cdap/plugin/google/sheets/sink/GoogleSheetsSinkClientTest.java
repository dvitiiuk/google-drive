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

import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.MergeCellsRequest;
import io.cdap.plugin.google.common.AuthType;
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;
import io.cdap.plugin.google.sheets.sink.utils.FlatternedRowsRecord;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GoogleSheetsSinkClientTest {

  private static GoogleSheetsSinkClient sinkClient;
  private static GoogleSheetsSinkConfig sinkConfig;

  @BeforeClass
  public static void setupClient() throws IOException {
    sinkConfig = EasyMock.createMock(GoogleSheetsSinkConfig.class);
    EasyMock.expect(sinkConfig.getAuthType()).andReturn(AuthType.OAUTH2).anyTimes();
    EasyMock.expect(sinkConfig.getRefreshToken()).andReturn("dsfdsfdsfdsf").anyTimes();
    EasyMock.expect(sinkConfig.getClientSecret()).andReturn("dsfdsfdsfdsf").anyTimes();
    EasyMock.expect(sinkConfig.getClientId()).andReturn("dsdsrfegvrb").anyTimes();

    EasyMock.replay(sinkConfig);
    sinkClient = new GoogleSheetsSinkClient(sinkConfig);

    EasyMock.verify(sinkConfig);
  }

  @Test
  public void testPrepareContentFlatHeaders() throws IOException, NoSuchMethodException,
    InvocationTargetException, IllegalAccessException {
    List<String> headerNames = Arrays.asList("h0", "h1", "h2", "h3");

    EasyMock.reset(sinkConfig);
    EasyMock.expect(sinkConfig.isWriteSchema()).andReturn(true).anyTimes();
    EasyMock.replay(sinkConfig);

    ComplexHeader complexHeader = new ComplexHeader("root");
    headerNames.stream().forEach(h -> complexHeader.addHeader(new ComplexHeader(h)));
    FlatternedRowsRecord rowsRecord = new FlatternedRowsRecord("spreadsheetName", "sheetTitle",
      complexHeader, Collections.emptyList(), Collections.emptyList());

    Method prepareContentRequestMethod = GoogleSheetsSinkClient.class.getDeclaredMethod("prepareContentRequest",
      Integer.class, FlatternedRowsRecord.class, boolean.class);
    prepareContentRequestMethod.setAccessible(true);

    /*Request request = (Request) prepareContentRequestMethod.invoke(sinkClient, 0, rowsRecord, true);

    List<RowData> rows = request.getAppendCells().getRows();
    Assert.assertEquals(1, rows.size());

    RowData headersRow = rows.get(0);
    Assert.assertEquals(headerNames.size(), headersRow.getValues().size());

    for (int i = 0; i < headerNames.size(); i++) {
      Assert.assertEquals(headerNames.get(i), headersRow.getValues().get(i).getUserEnteredValue().getStringValue());
    }*/
  }

  @Test
  public void testShiftAndNameMergeRequests() throws IOException, NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    List<GridRange> testMergeRanges = new ArrayList<>();
    Integer testSheetId = 42534;
    int rowsShift = 66;

    // add some single-cell merges that should be filtered out
    testMergeRanges.add(new GridRange().setStartRowIndex(0).setEndRowIndex(1)
      .setStartColumnIndex(0).setEndColumnIndex(1));
    testMergeRanges.add(new GridRange().setStartRowIndex(10).setEndRowIndex(11)
      .setStartColumnIndex(2).setEndColumnIndex(3));

    // invalid merges with no differences in coordinates
    testMergeRanges.add(new GridRange().setStartRowIndex(0).setEndRowIndex(0)
      .setStartColumnIndex(0).setEndColumnIndex(10));
    testMergeRanges.add(new GridRange().setStartRowIndex(10).setEndRowIndex(10)
      .setStartColumnIndex(20).setEndColumnIndex(30));

    // valid merges
    GridRange validRange1 = new GridRange().setStartRowIndex(0).setEndRowIndex(4)
      .setStartColumnIndex(0).setEndColumnIndex(1);
    GridRange validRange2 = new GridRange().setStartRowIndex(0).setEndRowIndex(1)
      .setStartColumnIndex(2).setEndColumnIndex(4);
    testMergeRanges.add(validRange1);
    testMergeRanges.add(validRange2);

    GridRange expectedRange1 = new GridRange().setStartRowIndex(validRange1.getStartRowIndex() + rowsShift)
      .setEndRowIndex(validRange1.getEndRowIndex() + rowsShift)
      .setStartColumnIndex(validRange1.getStartColumnIndex())
      .setEndColumnIndex(validRange1.getEndColumnIndex())
      .setSheetId(testSheetId);

    GridRange expectedRange2 = new GridRange().setStartRowIndex(validRange2.getStartRowIndex() + rowsShift)
      .setEndRowIndex(validRange2.getEndRowIndex() + rowsShift)
      .setStartColumnIndex(validRange2.getStartColumnIndex())
      .setEndColumnIndex(validRange2.getEndColumnIndex())
      .setSheetId(testSheetId);

    Method prepareMergeRequestsMethod =
      GoogleSheetsSinkClient.class.getDeclaredMethod("shiftAndNameMergeRequests",
        List.class, int.class, int.class);
    prepareMergeRequestsMethod.setAccessible(true);

    List<MergeCellsRequest> mergeRequests = (List<MergeCellsRequest>) prepareMergeRequestsMethod
      .invoke(sinkClient, testMergeRanges, testSheetId, rowsShift);

    Assert.assertEquals(2, mergeRequests.size());

    MergeCellsRequest request1 = mergeRequests.get(0);
    MergeCellsRequest request2 = mergeRequests.get(1);

    Assert.assertEquals(GoogleSheetsSinkClient.MERGE_ALL_MERGE_TYPE, request1.getMergeType());
    Assert.assertEquals(GoogleSheetsSinkClient.MERGE_ALL_MERGE_TYPE, request2.getMergeType());

    Assert.assertEquals(expectedRange1, request1.getRange());
    Assert.assertEquals(expectedRange2, request2.getRange());
  }

  @Test
  public void testCalcHeaderMergesHorizontal() throws NoSuchMethodException,
    InvocationTargetException, IllegalAccessException {
    List<String> headerNames = Arrays.asList("h0", "h1", "h2", "h3");

    ComplexHeader complexHeader = new ComplexHeader("h");
    headerNames.stream().forEach(h -> complexHeader.addHeader(new ComplexHeader(h)));

    Method calcHeaderMergesMethod = GoogleSheetsSinkClient.class.getDeclaredMethod("calcHeaderMerges",
      ComplexHeader.class, List.class, int.class, int.class, int.class);
    calcHeaderMergesMethod.setAccessible(true);

    List<GridRange> headerMergeRanges = new ArrayList<>();
    calcHeaderMergesMethod.invoke(sinkClient, complexHeader, headerMergeRanges, 0, complexHeader.getDepth(), 0);

    Assert.assertEquals(1, headerMergeRanges.size());

    GridRange horizontalMerge = headerMergeRanges.get(0);
    GridRange expectedMerge = new GridRange().setStartRowIndex(0).setEndRowIndex(1)
      .setStartColumnIndex(0).setEndColumnIndex(headerNames.size());
    Assert.assertEquals(expectedMerge, horizontalMerge);
  }

  @Test
  public void testCalcHeaderMergesVertical() throws NoSuchMethodException,
    InvocationTargetException, IllegalAccessException {

    ComplexHeader complexHeader = new ComplexHeader("h");

    Method calcHeaderMergesMethod = GoogleSheetsSinkClient.class.getDeclaredMethod("calcHeaderMerges",
      ComplexHeader.class, List.class, int.class, int.class, int.class);
    calcHeaderMergesMethod.setAccessible(true);

    List<GridRange> headerMergeRanges = new ArrayList<>();
    calcHeaderMergesMethod.invoke(sinkClient, complexHeader, headerMergeRanges, 0, 2, 0);

    Assert.assertEquals(1, headerMergeRanges.size());

    GridRange verticalMerge = headerMergeRanges.get(0);
    GridRange expectedMerge = new GridRange().setStartRowIndex(0).setEndRowIndex(2)
      .setStartColumnIndex(0).setEndColumnIndex(1);
    Assert.assertEquals(expectedMerge, verticalMerge);
  }

  /*
  | h                    |
  | h0 | h1 | h2         |
  |    |    | h20 |  h21 |
   */
  @Test
  public void testCalcHeaderMergesComplex() throws NoSuchMethodException,
    InvocationTargetException, IllegalAccessException {

    List<String> subSubHeaderNames = Arrays.asList("h20", "h21");

    ComplexHeader complexSubHeader = new ComplexHeader("h2");
    subSubHeaderNames.stream().forEach(sh -> complexSubHeader.addHeader(new ComplexHeader(sh)));

    ComplexHeader complexHeader = new ComplexHeader("h");
    complexHeader.addHeader(new ComplexHeader("h0"));
    complexHeader.addHeader(new ComplexHeader("h1"));
    complexHeader.addHeader(complexSubHeader);

    Method calcHeaderMergesMethod = GoogleSheetsSinkClient.class.getDeclaredMethod("calcHeaderMerges",
      ComplexHeader.class, List.class, int.class, int.class, int.class);
    calcHeaderMergesMethod.setAccessible(true);

    List<GridRange> headerMergeRanges = new ArrayList<>();
    calcHeaderMergesMethod.invoke(sinkClient, complexHeader, headerMergeRanges, 0, complexHeader.getDepth(), 0);

    Assert.assertEquals(4, headerMergeRanges.size());

    Assert.assertEquals(
      new GridRange().setStartRowIndex(0).setEndRowIndex(1).setStartColumnIndex(0).setEndColumnIndex(4),
      headerMergeRanges.get(0));

    Assert.assertEquals(
      new GridRange().setStartRowIndex(1).setEndRowIndex(3).setStartColumnIndex(0).setEndColumnIndex(1),
      headerMergeRanges.get(1));

    Assert.assertEquals(
      new GridRange().setStartRowIndex(1).setEndRowIndex(3).setStartColumnIndex(1).setEndColumnIndex(2),
      headerMergeRanges.get(2));

    Assert.assertEquals(
      new GridRange().setStartRowIndex(1).setEndRowIndex(2).setStartColumnIndex(2).setEndColumnIndex(4),
      headerMergeRanges.get(3));
  }

  @Test
  public void testPopulateHeaderRows() {

  }

  @Test
  public void testPrepareMergeRequests() {

  }

  @Test
  public void testPrepareFlatternedRequest() {

  }
}
