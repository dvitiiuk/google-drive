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

import io.cdap.plugin.google.common.exceptions.InvalidModifiedDateRangeException;

import java.util.stream.Stream;

/**
 * An enum which represent a type of dare range of file modification.
 */
public enum ModifiedDateRangeType {
  NONE("None"),
  LAST_7_DAYS("Last 7 Days"),
  LAST_30_DAYS("Last 30 days"),
  PREVIOUS_QUARTER("Previous Quarter"),
  CURRENT_QUARTER("Current Quarter"),
  LAST_YEAR("Last Year"),
  CURRENT_YEAR("Current Year"),
  CUSTOM("Custom");

  private final String value;

  ModifiedDateRangeType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public static ModifiedDateRangeType fromValue(String value) {
    return Stream.of(ModifiedDateRangeType.values())
      .filter(keyType -> keyType.getValue().equalsIgnoreCase(value))
      .findAny()
      .orElseThrow(() -> new InvalidModifiedDateRangeException(value));
  }
}
