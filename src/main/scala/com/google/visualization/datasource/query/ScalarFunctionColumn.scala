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

import com.google.common.collect.Lists
import base.InvalidQueryException
import datatable.DataTable
import datatable.TableCell
import datatable.TableRow
import datatable.value.Value
import datatable.value.ValueType
import query.scalarfunction.ScalarFunction
import org.apache.commons.lang.text.StrBuilder
import java.util.List

import collection.JavaConverters._

/**
 * A scalar function column (e.g. year(Date1)). The values in a scalar function column are the
 * result of executing the function on the columns that are given as parameters, so for example
 * year(Date1) column values will have the year of the corresponding value in Date1 column.
 *
 * @author Liron L.
 */
case class ScalarFunctionColumn(columns: List[AbstractColumn], scalarFunction: ScalarFunction) extends AbstractColumn {

  /**
   * A separator between function type and the columns on which it operates.
   * Used for creating the column ID.
   */
  final val COLUMN_FUNCTION_TYPE_SEPARATOR = "_"
  /**
   * When creating the ID of the column, this is used as a separator between the columns
   * on which the function is performed.
   */
  final val COLUMN_COLUMN_SEPARATOR = ","

  /**
   * Returns the ID of the scalar function column. THe ID is constructed from the
   * function's name and the IDs of the inner columns on which the function is
   * performed.
   *
   * @return The ID of the scalar function column.
   */
  def getId: String = {
    var colIds: List[String] = Lists.newArrayList[String]
    for (col <- columns.asScala) {
      colIds add col.getId
    }
    new StrBuilder(scalarFunction.getFunctionName).append(COLUMN_FUNCTION_TYPE_SEPARATOR).appendWithSeparators(colIds, COLUMN_COLUMN_SEPARATOR).toString
  }

  /**
   * Returns a list of the inner simple column IDs of the scalar function
   * column (i.e., the columns on which the function is performed).
   *
   * @return A list of the inner simple column IDs.
   */
  def getAllSimpleColumnIds: List[String] = {
    var columnIds: List[String] = Lists.newArrayList[String]
    for (column <- columns.asScala) {
      columnIds addAll column.getAllSimpleColumnIds
    }
    columnIds
  }

  /**
   * Returns the function of the scalar function column.
   *
   * @return The function of the scalar function column.
   */
  def getFunction = scalarFunction

  /**
   * Returns a list of the columns on which the function is performed.
   *
   * @return A list of the columns on which the function is performed.
   */
  def getColumns: List[AbstractColumn] = columns

  /**
   * Returns the cell of the column in the given row. If the given column
   * lookup contains this column, returns the cell in the given row using the
   * lookup. Otherwise, recursively gets the inner column values and uses them to
   * evaluate the value and create a cell based on that value. The base of the
   * recursion is a simple column, aggregation column or another scalar function
   * column that exists in the column lookup (i.e., its value was already calculated).
   *
   * @param row The given row.
   * @param lookup The column lookup.
   *
   * @return The cell of the column in the given row.
   */
  override def getCell(lookup: ColumnLookup, row: TableRow): TableCell = {
    if (lookup containsColumn this) {
      val columnIndex = lookup getColumnIndex this
      row.getCells get columnIndex
    } else {
      var functionParameters: List[Value] = Lists.newArrayListWithCapacity[Value](columns.size)
      for (column <- columns.asScala) {
        functionParameters add column.getValue(lookup, row)
      }
      new TableCell(scalarFunction.evaluate(functionParameters))
    }
  }

  /**
   * Returns a list of all simple columns. This includes simple columns that are
   * inside scalar function columns (e.g, year(a1)), but does not include simple
   * columns that are inside aggregation columns (e.g., sum(a1)).
   *
   * @return A list of all simple columns.
   */
  def getAllSimpleColumns: List[SimpleColumn] = {
    val simpleColumns: List[SimpleColumn] = Lists.newArrayListWithCapacity[SimpleColumn](columns.size)
    for (column <- columns.asScala) {
      simpleColumns addAll column.getAllSimpleColumns
    }
    simpleColumns
  }

  /**
   * Returns a list of all aggregation columns including the columns that are
   * inside scalar function columns (e.g., the column year(date(max(d1))) will
   * return max(d1)).
   *
   * @return A list of all aggregation columns.
   */
  def getAllAggregationColumns: List[AggregationColumn] = {
    val aggregationColumns: List[AggregationColumn] = Lists.newArrayListWithCapacity[AggregationColumn](columns.size)
    for (column <- columns.asScala) {
      aggregationColumns addAll column.getAllAggregationColumns
    }
    aggregationColumns
  }

  /**
   * Returns a list of all scalar function columns. Returns itself and
   * other inner scalar function columns (if there are any).
   * e.g., the column max(year(a1), year(a2)) will return the 3 columns:
   * max(year(a1), year(a2)), year(a1), year(a2).
   *
   * @return A list of all scalar function columns.
   */
  def getAllScalarFunctionColumns: List[ScalarFunctionColumn] = {
    val scalarFunctionColumns: List[ScalarFunctionColumn] = Lists.newArrayList[ScalarFunctionColumn](this)
    for (column <- columns.asScala) {
      scalarFunctionColumns addAll column.getAllScalarFunctionColumns
    }
    scalarFunctionColumns
  }

  /**
   * Checks that the column is valid. Checks the scalar function matches 
   * its arguments (inner columns) and all its inner columns are valid too. 
   * Throws a ColumnException if the scalar function has invalid arguments.
   *
   * @param dataTable The table description.
   *
   * @throws InvalidQueryException Thrown when the column is invalid.
   */
  def validateColumn(dataTable: DataTable): Unit = {
    val types: List[ValueType] = Lists.newArrayListWithCapacity[ValueType](columns.size)
    for (column <- columns.asScala) {
      column validateColumn dataTable
      types add (column getValueType dataTable)
    }
    scalarFunction validateParameters types
  }

  /**
   * Returns the value type of the column after evaluating the scalar function.
   * e.g., the value type of year(date1) is NUMBER.
   *
   * @param dataTable The table description.
   *
   * @return the value type of the column.
   */
  def getValueType(dataTable: DataTable): ValueType = {
    if (dataTable containsColumn this.getId) {
      return (dataTable getColumnDescription this.getId).getType
    }
    var types: List[ValueType] = Lists.newArrayListWithCapacity[ValueType](columns.size)
    for (column <- columns.asScala) {
      types add (column getValueType dataTable)
    }
    scalarFunction getReturnType types
  }

  /**
   * {@inheritDoc}
   */
  def toQueryString: String = {
    var columnQueryStrings: List[String] = Lists.newArrayList[String]
    for (column <- columns.asScala) {
      columnQueryStrings add column.toQueryString
    }
    scalarFunction toQueryString columnQueryStrings
  }

  /**
   * This is for debug and error messages, not for ID generation.
   *
   * @return A string describing this AggregationColumn.
   */
  override def toString: String = {
    var colNames: List[String] = Lists.newArrayList[String]
    for (col <- columns.asScala) {
      colNames add col.toString
    }
    new StrBuilder(scalarFunction.getFunctionName).append("(").appendWithSeparators(colNames, COLUMN_COLUMN_SEPARATOR).append(")").toString
  }
}

