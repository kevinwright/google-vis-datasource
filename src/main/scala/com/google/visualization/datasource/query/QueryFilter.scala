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

import datatable.{DataTable, TableRow}
import java.util.{List, Set}

/**
 * A query filter.
 * Any class that implements this interface can act as a filter, i.e., be the
 * part of a query that decides for a given TableRow if that row is part of the
 * result set.
 *
 * @author Yonatan B.Y.
 */
abstract class QueryFilter {
  /**
   * Checks if this row should be part of the result set.
   *
   * @param table The table containing this row.
   * @param row The row to check.
   *
   * @return true if this row should be part of the result set, false otherwise.
   */
  def isMatch(table: DataTable, row: TableRow): Boolean

  /**
   * Returns all the columnIds this filter uses.
   *
   * @return All the columnIds this filter uses.
   */
  def getAllColumnIds: Set[String]

  /**
   * Returns a list of all scalarFunctionColumns this filter uses.
   *
   * @return A list of all scalarFunctionColumns this filter uses.
   */
  def getScalarFunctionColumns: List[ScalarFunctionColumn]

  /**
   * Returns a list of all aggregation columns this filter uses. This is kept for future use, as
   * currently filters are not allowed to have aggregation columns. This is still used currently
   * for validation purposes.
   *
   * @return A list of all aggregation columns this filter uses.
   */
  protected[query] def getAggregationColumns: List[AggregationColumn]

  /**
   * Returns a string that, when parsed by the query parser, should return an
   * identical filter. The string returned does not contain the WHERE keyword.
   *
   * @return A string form of this filter.
   */
  def toQueryString: String
}

