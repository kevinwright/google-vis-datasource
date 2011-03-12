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

import com.google.common.collect.{ImmutableList, Lists}
import java.{util => ju, lang => jl}
import ju.List

import collection.JavaConverters._

/**
 * Pivoting definition for a query.
 * Pivoting is defined as a list of column IDs to pivot.
 *
 * @author Yoav G.
 * @author Yonatan B.Y.
 */
case class QueryPivot(columns: List[AbstractColumn]) {

  def this() = this(Lists.newArrayList[AbstractColumn])

  /**
   * Adds a column to pivot.
   *
   * @param column The column to add.
   */
  def addColumn(column: AbstractColumn): Unit =
    columns add column

  /**
   * Returns the list of pivot column IDs. This list is immutable.
   *
   * @return The list of pivot column IDs. This list is immutable.
   */
  def getColumnIds: List[String] = {
    val columnIds = Lists.newArrayList[String]
    for (col <- columns.asScala) {
      columnIds add col.getId
    }
    ImmutableList.copyOf(columnIds: jl.Iterable[String])
  }

  /**
   * Returns a list of all simple columns' IDs in this pivot.
   *
   * @return A list of all simple columns' IDs in this pivot.
   */
  def getSimpleColumnIds: List[String] = {
    val columnIds = Lists.newArrayList[String]
    for (col <- columns.asScala) {
      columnIds addAll col.getAllSimpleColumnIds
    }
    columnIds
  }

  /**
   * Returns the list of pivot columns. This list is immutable.
   *
   * @return The list of pivot columns. This list is immutable.
   */
  def getColumns: List[AbstractColumn] =
    ImmutableList.copyOf(columns: jl.Iterable[AbstractColumn])
  

  /**
   * Returns the list of pivot simple columns.
   *
   * @return The list of pivot simple columns.
   */
  def getSimpleColumns: List[SimpleColumn] = {
    val simpleColumns = Lists.newArrayList[SimpleColumn]
    for (col <- columns.asScala) {
      simpleColumns addAll col.getAllSimpleColumns
    }
    simpleColumns
  }

  /**
   * Returns the list of pivot scalar function columns.
   *
   * @return The list of pivot scalar function columns.
   */
  def getScalarFunctionColumns: List[ScalarFunctionColumn] = {
    var scalarFunctionColumns = Lists.newArrayList[ScalarFunctionColumn]
    for (col <- columns.asScala) {
      scalarFunctionColumns addAll col.getAllScalarFunctionColumns
    }
    scalarFunctionColumns
  }

  /**
   * Returns a string that when fed to the query parser would produce an equal QueryPivot.
   * The string is returned without the PIVOT keywors.
   *
   * @return The query string.
   */
  def toQueryString = Query columnListToQueryString columns
}

