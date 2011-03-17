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
import query.{AggregationColumn, AggregationType}
import org.apache.commons.lang.text.StrBuilder
import java.{util => ju}

import collection.JavaConverters._

object ColumnTitle {
  /**
   * When creating the id of the column, this is used as a separator between the values
   * in the pivot columns.
   */
  val PIVOT_COLUMNS_SEPARATOR: String = ","
  /**
   * When creating the id of the column, this is used as a separator between the list of
   * values and the column aggregation.
   */
  val PIVOT_AGGREGATION_SEPARATOR: String = " "
}

/**
 * A "title" (identification) of a column in a pivoted and/or grouped table.
 * What identifies a column is the list of values, one value from each of the pivot columns,
 * and a ColumnAggregation denoting on which column (of the original table columns) the aggregation
 * is performed and what type of aggregation (min, max, average, ...) is performed.
 *
 * @param values The list of values from the pivot-by columns.
 * @param aggregation The aggregation column.
 * @param isMultiAggregationQuery Whether this table comes from a
 *     query with more than one aggregation.
 *
 * @author Yonatan B.Y.
 */
class ColumnTitle(jvalues: ju.List[Value], val aggregation: AggregationColumn, isMultiAggregationQuery: Boolean) {
  import ColumnTitle._

  val values = jvalues.asScala.toList

  def getValues: ju.List[Value] = values.toBuffer.asJava
  def getAggregation = aggregation

  /**
   * Creates a ColumnDescription for this column. The id is created by
   * concatenating the values, and the column aggregation, using the separators
   * PIVOT_COLUMNS_SEPARATOR, PIVOT_AGGREGATION_SEPARATOR,
   * COLUMN_AGGRGATION_TYPE_SEPARATOR.
   *
   * @param originalTable The original table, from which the original column description
   *     of the column under aggregation is taken.
   *
   * @return The ColumnDescription for this column.
   */
  def createColumnDescription(originalTable: DataTable): ColumnDescription = {
    val colDesc = originalTable.getColumnDescription(aggregation.getAggregatedColumn.getId)
    createAggregationColumnDescription(colDesc)
  }

  /**
   * Creates a prefix for a pivoted column id, containing all the values of the
   * pivoted columns. Returns an empty string if no pivoting was used.
   *
   * @return A prefix for the pivoted column id.
   */
  private def createIdPivotPrefix: String = {
    if (!isPivot) ""
    else values.mkString("", PIVOT_COLUMNS_SEPARATOR, PIVOT_AGGREGATION_SEPARATOR)
  }

  /**
   * Creates a prefix for a pivoted column label, containing all the values of
   * the pivoted columns. Returns an empty string if no pivoting was used.
   *
   * @return A prefix for the pivoted column label.
   */
  private def createLabelPivotPart: String = {
    if (!isPivot) ""
    else values mkString PIVOT_COLUMNS_SEPARATOR
  }

  /**
   * Returns true if pivoting was used.
   *
   * @return True if pivoting was used.
   */
  private def isPivot: Boolean = (!values.isEmpty)

  override def equals(o: Any): Boolean = {
    if (this == o) true
    else if (!(o.isInstanceOf[ColumnTitle])) false
    else {
      val other = o.asInstanceOf[ColumnTitle]
      (values == other.values) && (aggregation == other.aggregation)
    }
  }

  override def hashCode: Int = {
    var hash: Int = 1279
    hash = (hash * 17) + values.hashCode
    hash = (hash * 17) + aggregation.hashCode
    hash
  }

  /**
   * Creates an aggregation column description.
   *
   * @param originalColumnDescription The original column description.
   *
   * @return A column description for the aggregation column.
   */
  private[engine]
  def createAggregationColumnDescription(originalColumnDescription: ColumnDescription): ColumnDescription = {
    val aggregationType = aggregation.getAggregationType
    val columnId = createIdPivotPrefix + aggregation.getId
    val valType = originalColumnDescription.getType
    val aggregationLabelPart = aggregation.getAggregationType.getCode + " " + originalColumnDescription.getLabel
    val pivotLabelPart = createLabelPivotPart

    var label =
      if (isPivot) {
        if (isMultiAggregationQuery) { pivotLabelPart + " " + aggregationLabelPart }
        else pivotLabelPart
      }
      else aggregationLabelPart

    if (canUseSameTypeForAggregation(valType, aggregationType)) {
      new ColumnDescription(columnId, valType, label)
    } else {
      new ColumnDescription(columnId, ValueType.NUMBER, label)
    }
  }

  /**
   * Checks whether the aggregation values and column values are of the same type.
   *
   * @param valueType The type of values in the aggregated column.
   * @param aggregationType The aggregation type.
   *
   * @return True if the aggregation values and column values are of the same
   *     type.
   */
  private def canUseSameTypeForAggregation(valueType: ValueType, aggregationType: AggregationType): Boolean = {
    import AggregationType._
    if (valueType == ValueType.NUMBER) true
    else {
      aggregationType match {
        case MIN | MAX => true
        case SUM | AVG | COUNT => false
        case _ => throw new IllegalArgumentException
      }
    }
  }
}

