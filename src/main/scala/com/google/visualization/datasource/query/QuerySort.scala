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
import org.apache.commons.lang.text.StrBuilder
import java.{util => ju, lang => jl}
import ju.List

import collection.JavaConverters._

/**
 * Sorting definition for a query.
 * Sort is defined as a list of column sorts where the first is the primary sort order,
 * the second is the secondary sort order, etc.
 *
 * @author Yoah B.D.
 */
case class QuerySort(sortColumns: List[ColumnSort]) {

  /**
   * Constructs an empty sort list.
   */
  def this() = this ( Lists.newArrayList[ColumnSort] )

  /**
   * Returns true if the sort list is empty.
   *
   * @return True if the sort list is empty.
   */
  def isEmpty: Boolean = sortColumns.isEmpty

  /**
   * Adds a column sort condition.
   * Validates that the column ID is not already specified in the sort.
   *
   * @param columnSort The column sort condition.
   */
  def addSort(columnSort: ColumnSort): Unit =  { sortColumns add columnSort }

  /**
   * Adds a column sort condition.
   * Validates that the column ID is not already specified in the sort.
   *
   * @param column The column to sort by.
   * @param order The requested ordering.
   */
  def addSort(column: AbstractColumn, order: SortOrder): Unit =
    addSort(new ColumnSort(column, order))

  /**
   * Returns the list of sort columns. This list is immutable.
   *
   * @return The list of sort columns. This list is immutable.
   */
  def getSortColumns: List[ColumnSort] =
    ImmutableList.copyOf(sortColumns: jl.Iterable[ColumnSort])

  /**
   * Returns a list of columns held by this query sort.
   *
   * @return A list of columns held by this query sort.
   */
  def getColumns: List[AbstractColumn] = {
    val result: List[AbstractColumn] = Lists.newArrayListWithExpectedSize[AbstractColumn](sortColumns.size)
    for (columnSort <- sortColumns.asScala) {
      result add columnSort.getColumn
    }
    result
  }

  /**
   * Returns all the columns that are AggregationColumns including aggregation
   * columns that are inside scalar function columns (e.g., year(min(a1))).
   *
   * @return All the columns that are AggregationColumns.
   */
  def getAggregationColumns: List[AggregationColumn] = {
    val result: List[AggregationColumn] = Lists.newArrayList[AggregationColumn]
    for (columnSort <- sortColumns.asScala) {
      var col: AbstractColumn = columnSort.getColumn
      for (innerCol <- col.getAllAggregationColumns.asScala) {
        if (!result.contains(innerCol)) {
          result add innerCol
        }
      }
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
    for (columnSort <- sortColumns.asScala) {
      val col: AbstractColumn = columnSort.getColumn
      for (innerCol <- col.getAllScalarFunctionColumns.asScala) {
        if (!result.contains(innerCol)) {
          result add innerCol
        }
      }
    }
    result
  }

  /**
   * Returns a string that when fed to the query parser would produce an equal QuerySort.
   * The string returned does not contain the ORDER BY keywords.
   *
   * @return The query string.
   */
  def toQueryString: String = {
    val builder = new StrBuilder
    val stringList: List[String] = Lists.newArrayList[String]
    for (colSort <- sortColumns.asScala) {
      stringList add colSort.toQueryString
    }
    builder.appendWithSeparators(stringList, ", ")
    builder.toString
  }

}

