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
package com.google.visualization.datasource.query

import com.google.common.collect.Lists
import com.google.visualization.datasource.datatable.DataTable
import com.google.visualization.datasource.datatable.value.ValueType
import java.util.List

/**
 * A column referred to by an explicit string ID.
 *
 * @author Yonatan B.Y.
 */
case class SimpleColumn(columnId: String) extends AbstractColumn {

  if (columnId.contains("`")) {
    throw new RuntimeException("Column ID cannot contain backtick (`)")
  }

  def getColumnId = columnId
  def getId = columnId

  def getAllSimpleColumnIds: List[String] = {
    Lists.newArrayList(columnId)
  }

  override def toString = columnId

  /**
   * Returns a list of all simple columns. In this case, returns only itself.
   *
   * @return A list of all simple columns.
   */
  def getAllSimpleColumns: List[SimpleColumn] = Lists.newArrayList(this)

  /**
   * Returns a list of all aggregation columns. In this case, returns an empty
   * list.
   *
   * @return A list of all aggregation columns.
   */
  def getAllAggregationColumns: List[AggregationColumn] =
    Lists.newArrayList[AggregationColumn]

  /**
   * Returns a list of all scalar function columns. In this case, returns an
   * empty list.
   *
   * @return A list of all scalar function columns.
   */
  def getAllScalarFunctionColumns: List[ScalarFunctionColumn] =
    return Lists.newArrayList[ScalarFunctionColumn]

  /**
   * Checks if the column is valid. In this case always does nothing.
   *
   * @param dataTable The data table.
   */
  def validateColumn(dataTable: DataTable): Unit = { }

  /**
   * Returns the value type of the column. In this case returns the value type
   * of the column itself.
   *
   * @param dataTable The data table.
   *
   * @return the value type of the column.
   */
  def getValueType(dataTable: DataTable): ValueType =
    dataTable.getColumnDescription(columnId).getType

  def toQueryString = "`" + columnId + "`"
}

