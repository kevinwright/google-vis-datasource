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

import collection.JavaConverters._

/**
 * Pivoting definition for a query.
 * Pivoting is defined as a list of column IDs to pivot.
 *
 * @author Yoav G.
 * @author Yonatan B.Y.
 */
case class QueryPivot(var columns: Seq[AbstractColumn]) {

  def this() = this(Seq.empty[AbstractColumn])

  def getColumns: ju.List[AbstractColumn] = columns.toBuffer.asJava
  
  def addColumn(column: AbstractColumn): Unit = columns :+= column

  def columnIds = columns map { _.getId }
  def getColumnIds: ju.List[String] = columnIds.toBuffer.asJava

  def simpleColumnIds = columns flatMap {_.getAllSimpleColumnIds.asScala}
  def getSimpleColumnIds: ju.List[String] = simpleColumnIds.toBuffer.asJava 

  def simpleColumns = columns flatMap {_.getAllSimpleColumns.asScala }
  def getSimpleColumns: ju.List[SimpleColumn] = simpleColumns.toBuffer.asJava

  def scalarFunctionColumns = columns flatMap { _.getAllScalarFunctionColumns.asScala }   
  def getScalarFunctionColumns: ju.List[ScalarFunctionColumn] = scalarFunctionColumns.toBuffer.asJava

  /**
   * Returns a string that when fed to the query parser would produce an equal QueryPivot.
   * The string is returned without the PIVOT keywors.
   *
   * @return The query string.
   */
  def toQueryString = Query columnListToQueryString getColumns
}

