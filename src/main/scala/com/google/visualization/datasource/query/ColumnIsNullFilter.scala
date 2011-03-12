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
import java.util.{List, Set}

/**
 * A filter that matches null values. Its isMatch function returns true if
 * the value at the column is a null value (as defined by TableCell.isNull()).
 *
 * @author Yonatan B.Y.
 */
case class ColumnIsNullFilter(column: AbstractColumn) extends QueryFilter {

  /**
   * Returns the column that should be null.
   *
   * @return The column that should be null.
   */
  def getColumn = column

  /**
   * {@inheritDoc}
   */
  override def getAllColumnIds: Set[String] =
    Sets.newHashSet(column.getAllSimpleColumnIds)

  /**
   * Returns a list of all scalarFunctionColumns this filter uses, in this case
   * the scalarFunctionColumns in column (e.g, in the filter 'year(a) is null'
   * it will return year(a).
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
   * {@inheritDoc}
   */
  def isMatch(table: DataTable, row: TableRow): Boolean = {
    val lookup = new DataTableColumnLookup(table)
    column.getValue(lookup, row).isNull
  }

  /**
   * {@inheritDoc}
   */
  override def toQueryString = column.toQueryString + " IS NULL"

}

