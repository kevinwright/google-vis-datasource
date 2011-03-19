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

import collection.JavaConverters._

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
          originalTable.getColumnDescription(aggColumn.getAggregatedColumn.getId).getLabel
        case scalarFunctionColumn: ScalarFunctionColumn =>
          val columns = scalarFunctionColumn.getColumns
          val labels = columns.asScala map { getColumnDescriptionLabel(originalTable, _) }
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
class ScalarFunctionColumnTitle(jvalues: ju.List[Value], val column: ScalarFunctionColumn) {
  import ScalarFunctionColumnTitle._

  val values = jvalues.asScala.toList
  def getValues = values.asJava

  /**
   * Creates a ColumnDescription for this column.
   *
   * @param originalTable The original table, from which the original column description
   *     of the column is taken.
   *
   * @return The ColumnDescription for this column.
   */
  def createColumnDescription(originalTable: DataTable) =
    new ColumnDescription(
      createIdPivotPrefix + column.getId,
      column.getValueType(originalTable),
      createLabelPivotPart + " " + getColumnDescriptionLabel(originalTable, column)
    )

  /**
   * Creates a prefix for a pivoted column id, containing all the values of the
   * pivoted columns. Returns the empty string if no pivoting was used.
   *
   * @return A prefix for the pivoted column id.
   */
  private def createIdPivotPrefix: String = {
    if (!isPivot) ""
    else values.mkString("", PIVOT_COLUMNS_SEPARATOR, PIVOT_SCALAR_FUNCTION_SEPARATOR)
  }

  /**
   * Creates a prefix for a pivoted column label, containing all the values of
   * the pivoted columns. Returns an empty string if no pivoting was used.
   *
   * @return A prefix for the pivoted column label.
   */
  private def createLabelPivotPart: String = {
    if (!isPivot) ""
    else values.mkString(PIVOT_COLUMNS_SEPARATOR)
  }

  private def isPivot: Boolean = !values.isEmpty

  override def equals(o: Any): Boolean = {
    if (o.isInstanceOf[ScalarFunctionColumnTitle]) {
      val other = o.asInstanceOf[ScalarFunctionColumnTitle]
      (values == other.values) && (column == other.column)
    } else false
  }

  override def hashCode: Int = {
    var result: Int = 31
    if (column != null) {
      result += column.hashCode
    }
    result *= 31
    if (values != null) {
      result += values.hashCode
    }
    result
  }
}

