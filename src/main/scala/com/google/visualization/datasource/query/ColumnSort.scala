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
package com.google.visualization.datasource
package query

/**
 * A sort definition for a single column.
 * This class is immutable.
 *
 * @author Yoah B.D.
 */
class ColumnSort(column: AbstractColumn, order: SortOrder) {

  def getColumn = column
  def getOrder = order

  /**
   * Creates a string that when fed to the query parser should return a ColumnSort equal to this
   * one. Used mainly for debugging purposes.
   * @return A query string.
   */
  def toQueryString =
    column.toQueryString + (if (order == SortOrder.DESCENDING) " DESC" else "")

}

