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
import base.{InvalidQueryException, MessagesEnum}
import datatable.DataTable
import datatable.value.ValueType
import com.ibm.icu.util.ULocale
import java.util.List

/**
 * A column that is referred to by an aggregation, for example, min(c1).
 *
 * @author Yonatan B.Y.
 *
 * @param aggregatedColumn
 *   The simple column on which the aggregation is performed, e.g., c1 in
 *   min(c1).
 *
 * @param aggregationType
 *   The type of aggregation that is performed, e.g., min in min(c1).
 */
case class AggregationColumn(aggregatedColumn: SimpleColumn, aggregationType: AggregationType)
extends AbstractColumn {
  val COLUMN_AGGRGATION_TYPE_SEPARATOR = "-"


  /**
   * Creates a string to act as ID for this column. Constructed from the
   * aggregation type and the column ID, separated by a separator.
   *
   * @return A string to act as ID for this column.
   */
  override def getId: String =
    aggregationType.getCode + COLUMN_AGGRGATION_TYPE_SEPARATOR + aggregatedColumn.getId

  /**
   * Returns the column to aggregate.
   *
   * @return The column to aggregate.
   */
  def getAggregatedColumn = aggregatedColumn

  override def getAllSimpleColumnIds: List[String] =
    Lists.newArrayList(aggregatedColumn.getId)

  /**
   * Returns a list of all simple columns. In this case, returns an empty list.
   *
   * @return A list of all simple columns.
   */
  override def getAllSimpleColumns: List[SimpleColumn] =
    Lists.newArrayList[SimpleColumn]

  /**
   * Returns a list of all aggregation columns. In this case, returns only
   * itself.
   *
   * @return A list of all aggregation columns.
   */
  override def getAllAggregationColumns: List[AggregationColumn] =
    Lists.newArrayList(this)

  /**
   * Returns a list of all scalar function columns. In this case, returns an
   * empty list.
   *
   * @return A list of all scalar function columns.
   */
  override def getAllScalarFunctionColumns: List[ScalarFunctionColumn] =
    Lists.newArrayList[ScalarFunctionColumn]

  /**
   * Returns the requested aggregation type.
   *
   * @return The requested aggregation type.
   */
  def getAggregationType: AggregationType = aggregationType

  /**
   * Checks whether it makes sense to have the aggregation type on
   * the aggregated column. The type of the column is taken from the given
   * table description. Throws a column exception if the column is invalid.
   *
   * @param dataTable The data table.
   *
   * @throws InvalidQueryException Thrown if the column is invalid.
   */
  def validateColumn(dataTable: DataTable): Unit = {
    val valueType = dataTable.getColumnDescription(aggregatedColumn.getId).getType
    val userLocale = dataTable.getLocaleForUserMessages
    import AggregationType._
    aggregationType match {
      case COUNT | MAX | MIN => ()
      case AVG | SUM =>
        if (valueType != ValueType.NUMBER) {
          throw new InvalidQueryException(MessagesEnum.AVG_SUM_ONLY_NUMERIC getMessage userLocale)
        }
      case _ =>
        throw new RuntimeException(MessagesEnum.INVALID_AGG_TYPE.getMessageWithArgs(userLocale, aggregationType.toString))
    }
  }

  /**
   * Returns the value type of the column. In this case returns the value type
   * of the column after evaluating the aggregation function.
   *
   * @param dataTable The data table.
   *
   * @return The value type of the column.
   */
  def getValueType(dataTable: DataTable): ValueType = {
    val originalValueType = dataTable.getColumnDescription(aggregatedColumn.getId).getType
    import AggregationType._
    aggregationType match {
      case COUNT => ValueType.NUMBER
      case AVG | SUM | MAX | MIN => originalValueType
      case _ =>
        throw new RuntimeException(MessagesEnum.INVALID_AGG_TYPE.getMessageWithArgs(dataTable.getLocaleForUserMessages, aggregationType.toString))
    }
  }

  /**
   * This is for debug and error messages, not for ID generation.
   *
   * @return A string describing this AggregationColumn.
   */
  override def toString = aggregationType.getCode + "(" + aggregatedColumn.getId + ")"

  /**
   * {@inheritDoc}
   */
  override def toQueryString =
    aggregationType.getCode.toUpperCase + "(" + aggregatedColumn.toQueryString + ")"
}

