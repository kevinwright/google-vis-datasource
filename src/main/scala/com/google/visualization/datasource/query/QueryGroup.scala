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

import com.google.common.collect.{ImmutableList, Lists}
import java.{util => ju, lang => jl}
import ju.List
import collection.JavaConverters._

/**
 * Grouping definition for a query.
 * Grouping is defined as a list of column IDs to group by.
 *
 * @author Yoav G.
 * @author Yonatan B.Y.
 * @author Liron L.
 */
case class QueryGroup(columns: List[AbstractColumn]) {
  def this() = this(Lists.newArrayList[AbstractColumn])

  def addColumn(column: AbstractColumn): Unit = columns add column

  def getColumnIds: List[String] = {
    val columnIds: List[String] = Lists.newArrayList[String]
    for (col <- columns.asScala) {
      columnIds add col.getId
    }
    ImmutableList.copyOf(columnIds: jl.Iterable[String])
  }

  /**
   * Returns a list of all simple columns' IDs in this group.
   *
   * @return A list of all simple columns' IDs in this group.
   */
  def getSimpleColumnIds: List[String] = {
    val columnIds: List[String] = Lists.newArrayList[String]
    for (col <- columns.asScala) {
      columnIds addAll col.getAllSimpleColumnIds
    }
    columnIds
  }

  /**
   * Returns the list of group-by columns. This list is immutable.
   *
   * @return The list of group-by columns. This list is immutable.
   */
  def getColumns: List[AbstractColumn] =
    ImmutableList.copyOf(columns: jl.Iterable[AbstractColumn])

  /**
   * Returns the list of simple columns included in the group-by section.
   *
   * @return The list of simple columns included in the group-by section.
   */
  def getSimpleColumns: List[SimpleColumn] = {
    val simpleColumns: List[SimpleColumn] = Lists.newArrayList[SimpleColumn]
    for (col <- columns.asScala) {
      simpleColumns addAll col.getAllSimpleColumns
    }
    simpleColumns
  }

  /**
   * Returns the list of scalar function columns included in the group-by
   * section.
   *
   * @return The list of scalar function columns included in the group-by
   *     section
   */
  def getScalarFunctionColumns: List[ScalarFunctionColumn] = {
    val scalarFunctionColumns: List[ScalarFunctionColumn] = Lists.newArrayList[ScalarFunctionColumn]
    for (col <- columns.asScala) {
      scalarFunctionColumns addAll col.getAllScalarFunctionColumns
    }
    scalarFunctionColumns
  }

  /**
   * Returns a string that when fed to the query parser would produce an equal QueryGroup.
   * The string is returned without the GROUP BY keywords.
   *
   * @return The query string.
   */
  def toQueryString = Query columnListToQueryString columns


}

