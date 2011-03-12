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

import com.google.common.collect.Sets
import datatable.{DataTable, TableRow}
import datatable.value.Value
import java.util.{List, Set}

/**
 * A filter that compares a column value to a constant value.
 *
 * @author Yonatan B.Y.
 *
 * @param isComparisonOrderReversed
 * Specifies whether or not this operator's comparison order is reversed. This
 * means whether the column and the value should be reversed, i.e., instead of
 * column op value, this will be value op column.
 *
 * @return Whether or not the operator's comparison order is reversed.
 */
case class ColumnValueFilter(column: AbstractColumn, value: Value, operator0: ComparisonFilterOperator, isComparisonOrderReversed: Boolean)
extends ComparisonFilter(operator0) {

  /**
   * Constructs a new ColumnValueFilter on a given column, constant value and
   * operator.
   *
   * @param column The column to compare.
   * @param value The constant value the column value is compared to.
   * @param operator The comparison operator to use.
   */
  def this(column: AbstractColumn, value: Value, operator: ComparisonFilterOperator) =
    this (column, value, operator, false)

  /**
   * Implements isMatch from the QueryFilter interface. This implementation
   * retrieves from the row, the data in the required column, and compares
   * it against the constant value, using the operator given in the
   * constructor.
   *
   * @param table The table containing this row.
   * @param row The row to check.
   *
   * @return true if this row should be part of the result set, i.e., if the
   *     operator returns true when run on the value in the column, and the
   *     constant value; false otherwise.
   */
  def isMatch(table: DataTable, row: TableRow): Boolean = {
    val lookup = new DataTableColumnLookup(table)
    val columnValue = column.getValue(lookup, row)
    if (isComparisonOrderReversed) isOperatorMatch(value, columnValue)
    else isOperatorMatch(columnValue, value)
  }

  /**
   * Returns all the columnIds this filter uses, in this case the simple column
   * IDs of the filter's column.
   *
   * @return All the columnIds this filter uses.
   */
  override def getAllColumnIds: Set[String] =
    Sets.newHashSet(column.getAllSimpleColumnIds)

  /**
   * Returns a list of all scalarFunctionColumns this filter uses, in this case
   * the scalarFunctionColumns in the filter's column (e.g, in thefilter
   * 'year(a)=2008' it will return year(a).
   *
   * @return A list of all scalarFunctionColumns this filter uses.
   */
  override def getScalarFunctionColumns: List[ScalarFunctionColumn] =
    column.getAllScalarFunctionColumns

  /**
   * {@inheritDoc}
   */
  protected[query] override def getAggregationColumns: List[AggregationColumn] =
    column.getAllAggregationColumns

  /**
   * Returns the column ID associated with this ColumnValueFilter.
   *
   * @return The column ID associated with this ColumnValueFilter.
   */
  def getColumn = column

  /**
   * Returns the value associated with this ColumnValueFilter.
   *
   * @return The value associated with this ColumnValueFilter.
   */
  def getValue = value


  /**
   * {@inheritDoc}
   */
  override def toQueryString: String = {
    if (isComparisonOrderReversed)
      value.toQueryString + " " + operator.toQueryString + " " + column.toQueryString
    else
      column.toQueryString + " " + operator.toQueryString + " " + value.toQueryString
  }
}

