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

import base.InvalidQueryException
import datatable.{DataTable, TableCell, TableRow}
import datatable.value.{Value, ValueType}
import java.util.List

/**
 * A column.
 * This can be either a column by ID, or an aggregation column such as min(c1),
 * etc.
 *
 * @author Yonatan B.Y.
 */
abstract class AbstractColumn {
  /**
   * Returns a string ID for this column. It can be already existing in the
   * instance, or generated upon request, depending on the specific class
   * implementation.
   *
   * @return The string ID for this column.
   */
  def getId: String

  /**
   * Returns a list of all simple (primitive) column IDs included in this
   * AbstractColumn. This is a list to support calculated columns in the future.
   * For example, a simple column would just return a list containing its own
   * ID. An aggregation column would just return a list containing the ID of
   * its aggregated column. In future, when calculated columns are
   * introduced, a calculated column will return a list with more than one
   * element.
   *
   * @return A list of all simple column IDs included in this AbstractColumn.
   */
  def getAllSimpleColumnIds: List[String]

  /**
   * Returns the value of the column in the given row.
   *
   * @param row The given row.
   * @param lookup The column lookup.
   *
   * @return The value of the column in the given row.
   */
  def getValue(lookup: ColumnLookup, row: TableRow) =
    getCell(lookup, row).getValue

  /**
   * Returns the cell of the column in the given row.
   *
   * @param row The given row.
   * @param lookup The column lookup.
   *
   * @return The cell of the column in the given row.
   */
  def getCell(lookup: ColumnLookup, row: TableRow): TableCell = {
    val columnIndex = lookup getColumnIndex this
    row.getCells get columnIndex
  }

  /**
   * Returns a list of all simple columns included in this abstract column.
   *
   * @return A list of all simple columns included in this abstract column.
   */
  def getAllSimpleColumns: List[SimpleColumn]

  /**
   * Returns a list of all aggregation columns included in this abstract
   * column.
   *
   * @return A list of all aggregation columns included in this abstract
   *     column.
   */
  def getAllAggregationColumns: List[AggregationColumn]

  /**
   * Returns a list of all scalar function columns included in this abstract
   * column.
   *
   * @return A list of all scalar function columns included in this abstract
   *     column.
   */
  def getAllScalarFunctionColumns: List[ScalarFunctionColumn]

  /**
   * Checks whether the column is valid. Aggregation columns and
   * scalar function columns are valid if the aggregation or scalar function
   * respectively matches its arguments (inner columns).
   *
   * @param dataTable The data table.
   *
   * @throws InvalidQueryException Thrown if the column is not valid.
   */
  def validateColumn(dataTable: DataTable): Unit

  /**
   * Returns the value type of the column. For a simple column, returns the
   * value type of the column itself. For an aggregation or scalar function
   * column, returns the value type of the column after evaluating the function.
   * For example, the value type of year(a1) is NUMBER.
   *
   * @param dataTable The data table.
   *
   * @return The value type of the column.
   */
  def getValueType(dataTable: DataTable): ValueType

  /**
   * Returns a string that when parsed by the query parser, should return an
   * identical column.
   *
   * @return A string form of this column.
   */
  def toQueryString: String
}

