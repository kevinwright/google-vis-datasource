// Copyright 2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.visualization.datasource.query

/**
 * Options definition for a query.
 * Holds options for the query that fall under the options clause.
 *
 * @author Yonatan B.Y.
 *
 * @param noValues
 *   Should be set to indicate not to return the values, but only the column
 *   labels and types (table description).
 *
 * @param noFormat
 *   Should be set to indicate not to format the values, but to return only the
 *   raw data. By default returns both raw and formatted values.
 */
case class QueryOptions(var noValues: Boolean, var noFormat: Boolean) {
  /**
   * Constructs query options with all boolean options set to false.
   */
  def this() = this (false, false)

  def isNoValues: Boolean =  noValues
  def setNoValues(noValues: Boolean): Unit = { this.noValues = noValues }

  def isNoFormat: Boolean = noFormat
  def setNoFormat(noFormat: Boolean): Unit = { this.noFormat = noFormat }

  def isDefault: Boolean = { !noFormat && !noValues }

  /**
   * Returns a string that when fed to the query parser should return a QueryOptions equal to this
   * one. Used mainly for debugging purposes. The string returned does not contain the
   * OPTIONS keyword.
   *
   * @return The query string.
   */
  def toQueryString: String = {
    (if (noValues) "NO_VALUES" else "") + (if (noFormat) "NO_FORMAT" else "")
  }

}

