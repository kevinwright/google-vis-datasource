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
package engine

import datatable.{ColumnDescription, DataTable}
import datatable.value.{Value, ValueType}
import org.apache.commons.lang.text.StrBuilder
import java.{util => ju}

object ScalarFunctionColumnTitle {
  /**
   * Returns the label of the column description.
   *
   * @param originalTable The original table, from which the original column description of the
   *     column is taken.
   *
   * @return The label of the column description.
   */
  def getColumnDescriptionLabel(originalTable: DataTable, column: AbstractColumn): String = {
    if (originalTable containsColumn column.getId) {
      originalTable.getColumnDescription(column.getId).getLabel
    } else {
      column match {
        case aggColumn: AggregationColumn =>
          aggColumn.getAggregationType.getCode +
          " " +
          originalTable.getColumnDescription(aggColumn.getAggregatedColumn.getId).getLabel)
        case scalarFunctionColumn: ScalarFunctionColumn =>
          val columns = scalarFunctionColumn.getColumns
          val labels = columns map { getColumnDescriptionLabel(originalTable, _) }
          labels.mkString("(", "", ")")
        case _ => error("unexpected")
      }
    }

  }

  /**
   * When creating the id of the column, this is used as a separator between the values
   * in the pivot-by columns.
   */
  val PIVOT_COLUMNS_SEPARATOR = ","
  /**
   * When creating the id of the column, this is used as a separator between the list of
   * values and the ScalarFunctionColumn.
   */
  val PIVOT_SCALAR_FUNCTION_SEPARATOR = " "
}

/**
 * A "title" (identification) of a scalar function column in a pivoted and/or grouped table.
 * What identifies a column is the list of values, one value from each of the
 * pivot columns, and a ScalarFunctionColumn denoting the original column (of the original table
 * columns).
 *
 * @author Liron L.
 */
class ScalarFunctionColumnTitle {
  /**
   * Creates a new instance of this class, with the given values and
   * ScalarFunctionColumn.
   *
   * @param values The list of values.
   * @param column The ScalarFunctionColumn.
   */
  def this(values: List[Value], column: ScalarFunctionColumn) {
    this ()
    this.values = values
    this.scalarFunctionColumn = column
  }

  /**
   * Returns the values in this ColumnTitle.
   *
   * @return The values in this ColumnTitle.
   */
  def getValues: List[Value] = {
    return values
  }

  /**
   * Creates a ColumnDescription for this column.
   *
   * @param originalTable The original table, from which the original column description
   *     of the column is taken.
   *
   * @return The ColumnDescription for this column.
   */
  def createColumnDescription(originalTable: DataTable): ColumnDescription = {
    var columnId: String = createIdPivotPrefix + scalarFunctionColumn.getId
    var `type` : ValueType = scalarFunctionColumn.getValueType(originalTable)
    var label: String = createLabelPivotPart + " " + getColumnDescriptionLabel(originalTable, scalarFunctionColumn)
    var result: ColumnDescription = new ColumnDescription(columnId, `type`, label)
    return result
  }

  /**
   * Creates a prefix for a pivoted column id, containing all the values of the
   * pivoted columns. Returns the empty string if no pivoting was used.
   *
   * @return A prefix for the pivoted column id.
   */
  private def createIdPivotPrefix: String = {
    if (!isPivot) {
      return ""
    }
    return new StrBuilder().appendWithSeparators(values, PIVOT_COLUMNS_SEPARATOR).append(PIVOT_SCALAR_FUNCTION_SEPARATOR).toString
  }

  /**
   * Creates a prefix for a pivoted column label, containing all the values of
   * the pivoted columns. Returns an empty string if no pivoting was used.
   *
   * @return A prefix for the pivoted column label.
   */
  private def createLabelPivotPart: String = {
    if (!isPivot) {
      return ""
    }
    return new StrBuilder().appendWithSeparators(values, PIVOT_COLUMNS_SEPARATOR).toString
  }

  /**
   * Returns true if pivoting was used.
   *
   * @return True if pivoting was used.
   */
  private def isPivot: Boolean = {
    return (!values.isEmpty)
  }

  override def equals(o: AnyRef): Boolean = {
    if (o.isInstanceOf[ScalarFunctionColumnTitle]) {
      var other: ScalarFunctionColumnTitle = o.asInstanceOf[ScalarFunctionColumnTitle]
      return (values.equals(other.values) && scalarFunctionColumn.equals(other.scalarFunctionColumn))
    }
    return false
  }

  override def hashCode: Int = {
    var result: Int = 31
    if (scalarFunctionColumn != null) {
      result += scalarFunctionColumn.hashCode
    }
    result *= 31
    if (values != null) {
      result += values.hashCode
    }
    return result
  }

  /**
   * The list of values from the pivot-by columns.
   */
  private var values: List[Value] = null
  /**
   * The scalar function column denoting this column.
   */
  var scalarFunctionColumn: ScalarFunctionColumn = null
}

