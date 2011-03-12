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

import datatable.DataTable
import datatable.TableRow
import java.util.{List, Set}

/**
 * A negation filter.
 * Holds a sub-filter and negates its result, acting like the NOT operator.
 *
 * @author Yonatan B.Y.
 */
case class NegationFilter(val subFilter: QueryFilter) extends QueryFilter {

  /**
   * Implements isMatch (from the QueryFilter interface) by recursively calling
   * isMatch on the sub-filter and negating the result.
   *
   * @param table The table containing this row.
   * @param row The row to check.
   *
   * @return true if this row should be part of the result set, false otherwise.
   */
  def isMatch(table: DataTable, row: TableRow): Boolean =
    !subFilter.isMatch(table, row)

  /**
   * Returns all the columnIds this filter uses, in this case exactly all the
   * columnIds that the sub-filter uses.
   *
   * @return All the columnIds this filter uses.
   */
  def getAllColumnIds: Set[String] = subFilter.getAllColumnIds

  /**
   * Returns a list of all scalarFunctionColumns this filter uses, in this case
   * the scalarFunctionColumns its sub-filter uses.
   *
   * @return A list of all scalarFunctionColumns this filter uses.
   */
  def getScalarFunctionColumns: List[ScalarFunctionColumn] =
    subFilter.getScalarFunctionColumns

  /**
   * {@InheritDoc}
   */
  protected[query] def getAggregationColumns: List[AggregationColumn] =
    subFilter.getAggregationColumns

  /**
   * Returns the sub-filter associated with this NegationFilter.
   *
   * @return The sub-filter associated with this NegationFilter.
   */
  def getSubFilter = subFilter

  /**
   * {@inheritDoc}
   */
  def toQueryString = "NOT (" + subFilter.toQueryString + ")"
}

