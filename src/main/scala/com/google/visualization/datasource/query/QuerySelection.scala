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
 * Selection definition for a query.
 * Selection is defined as a list of column IDs. It can also include aggregations for
 * grouping/pivoting and scalar functions.
 *
 *
 * @author Itai R.
 */
case class QuerySelection(columns: List[AbstractColumn]) {

  def this() = this(Lists.newArrayList[AbstractColumn])
  def this(source: QuerySelection) = this(Lists.newArrayList(source.columns))

  def isEmpty = columns.isEmpty

  def addColumn(column: AbstractColumn): Unit = columns add column

  def getColumns: List[AbstractColumn] =
    ImmutableList.copyOf(columns: jl.Iterable[AbstractColumn])

  /**
   * Returns all the columns that are AggregationColumns including aggregation
   * columns that are inside scalar function columns (e.g., year(min(a1))).
   *
   * @return All the columns that are AggregationColumns.
   */
  def getAggregationColumns: List[AggregationColumn] = {
    val result: List[AggregationColumn] = Lists.newArrayList[AggregationColumn]
    for (col <- columns.asScala) {
      result addAll col.getAllAggregationColumns
    }
    result
  }

  /**
   * Returns all the columns that are SimpleColumns including those inside
   * scalar function columns (e.g, year(a1)). Does not return simple columns
   * inside aggregation columns (e.g., sum(a1)).
   *
   * @return All the columns that are SimpleColumns.
   */
  def getSimpleColumns: List[SimpleColumn] = {
    val result: List[SimpleColumn] = Lists.newArrayList[SimpleColumn]
    for (col <- columns.asScala) {
      result addAll col.getAllSimpleColumns
    }
    result
  }

  /**
   * Returns all the columns that are ScalarFunctionColumns including scalar
   * functions columns that are inside other scalar function columns
   * (e.g., sum(year(a), year(b))).
   *
   * @return all the columns that are ScalarFunctionColumns.
   */
  def getScalarFunctionColumns: List[ScalarFunctionColumn] = {
    val result: List[ScalarFunctionColumn] = Lists.newArrayList[ScalarFunctionColumn]
    for (col <- columns.asScala) {
      result addAll col.getAllScalarFunctionColumns
    }
    result
  }

  /**
   * Returns a string that when fed to the query parser would produce an equal QuerySelection.
   * The string is returned without the SELECT keyword.
   *
   * @return The query string.
   */
  def toQueryString: String = Query.columnListToQueryString(columns)
}

