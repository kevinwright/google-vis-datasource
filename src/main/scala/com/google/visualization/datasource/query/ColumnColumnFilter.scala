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

import com.google.common.collect.{Lists, Sets}
import datatable.{DataTable, TableRow}
import com.google.visualization.datasource.datatable.value.Value
import java.util.{List, Set}

/**
 * A filter that compares two column values.
 *
 * @author Yonatan B.Y.
 */
case class ColumnColumnFilter(firstColumn: AbstractColumn, secondColumn: AbstractColumn, operator0: ComparisonFilterOperator)
extends ComparisonFilter(operator0) {

  /**
   * Implements isMatch from the QueryFilter interface. This implementation
   * retrieves from the row, the data in the required columns, and compares
   * the first against the second, using the operator given in the
   * constructor.
   *
   * @param table The table containing this row.
   * @param row The row to check.
   *
   * @return true if this row should be part of the result set, i.e., if the
   *     operator is true when run on the values in the two columns; false
   *     otherwise.
   */
  def isMatch(table: DataTable, row: TableRow): Boolean = {
    val lookup = new DataTableColumnLookup(table)
    val firstValue = firstColumn.getValue(lookup, row)
    val secondValue = secondColumn.getValue(lookup, row)
    isOperatorMatch(firstValue, secondValue)
  }

  /**
   * Returns all the simple column IDs this filter uses, in this case
   * the simple column IDs of firstColumn and secondColumn.
   *
   * @return All the column IDs this filter uses.
   */
  override def getAllColumnIds: Set[String] = {
    val columnIds: Set[String] = Sets.newHashSet(firstColumn.getAllSimpleColumnIds)
    columnIds.addAll(secondColumn.getAllSimpleColumnIds)
    columnIds
  }

  /**
   * Returns a list of all scalarFunctionColumns this filter uses, in this case
   * the scalarFunctionColumns in the first and second columns (e.g, in the
   * filter year(a) = year(date(b)) it will return year(a), year(date(b)) and
   * date(b)).
   *
   * @return A list of all scalarFunctionColumns this filter uses.
   */
  override def getScalarFunctionColumns: List[ScalarFunctionColumn] = {
    val scalarFunctionColumns: List[ScalarFunctionColumn] = Lists.newArrayList[ScalarFunctionColumn]
    scalarFunctionColumns addAll firstColumn.getAllScalarFunctionColumns
    scalarFunctionColumns addAll secondColumn.getAllScalarFunctionColumns
    scalarFunctionColumns
  }

  /**
   * {@InheritDoc}
   */
  protected[query] override def getAggregationColumns: List[AggregationColumn] = {
    val aggregationColumns: List[AggregationColumn] = Lists.newArrayList[AggregationColumn]
    aggregationColumns addAll firstColumn.getAllAggregationColumns
    aggregationColumns addAll secondColumn.getAllAggregationColumns
    aggregationColumns
  }

  /**
   * Returns the first column associated with this ColumnColumnFilter.
   *
   * @return The first column associated with this ColumnColumnFilter.
   */
  def getFirstColumn = firstColumn

  /**
   * Returns the second column associated with this ColumnColumnFilter.
   *
   * @return The second column associated with this ColumnColumnFilter.
   */
  def getSecondColumn = secondColumn

  /**
   * {@inheritDoc}
   */
  override def toQueryString =
    firstColumn.toQueryString + " " + operator.toQueryString + " " + secondColumn.toQueryString

}

