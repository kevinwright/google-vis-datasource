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

import com.google.common.collect.{ImmutableSet, Lists, Maps}
import base.InvalidQueryException
import org.apache.commons.lang.text.StrBuilder
import org.apache.commons.logging.{Log, LogFactory}
import java.{util => ju, lang => jl}
import ju.{List, Map, Set}

import collection.JavaConverters._

/**
 * Describes the formatting options of a Query, as passed on the query
 * in the "format" clause.
 *
 * Format can be specified for each column, but does not have to be.
 * If not specified, each column type has a default format.
 *
 * Each format is a string pattern, as used in the ICU UFormat class.
 * @see com.ibm.icu.text.UFormat
 *
 * @author Itai R.
 *
 * @param columnPatterns
 *   A map of all of the columns that require non-default patterns, and the
 *   pattern specified for them.
 *   Columns with default patterns are not in the map.
 */
case class QueryFormat(columnPatterns: Map[AbstractColumn, String]) {

  private val log = LogFactory getLog this.getClass

  def this() = this(Maps.newHashMap[AbstractColumn, String])

  /**
   * Adds a column pattern.
   * Validates that the column ID is not already specified.
   *
   * @param column The column to which the pattern is assigned.
   * @param pattern The assigned pattern.
   *
   * @throws InvalidQueryException Thrown if the column is already specified.
   */
  def addPattern(column: AbstractColumn, pattern: String): Unit = {
    if (columnPatterns.keySet contains column) {
      val message = "Column [" + column.toString + "] is " + "specified more than once in FORMAT."
      log error message
      throw new InvalidQueryException(message)
    }
    columnPatterns.put(column, pattern)
  }

  /**
   * Returns the pattern of the specified column, or null if no pattern was
   * specified.
   *
   * @param column The column for which the pattern is required.
   *
   * @return The pattern, or null if no pattern was specified for this column.
   */
  def getPattern(column: AbstractColumn): String =
    columnPatterns get column

  /**
   * Returns an immutable set of the column IDs for which a pattern was
   * specified.
   *
   * @return An immutable set of the column IDs for which a pattern was
   *     specified.
   */
  def getColumns: Set[AbstractColumn] =
    ImmutableSet.copyOf(columnPatterns.keySet: jl.Iterable[AbstractColumn])

  /**
   * Returns all the columns that are ScalarFunctionColumns including scalar
   * functions columns that are inside other scalar function columns
   * (e.g., sum(year(a), year(b))).
   *
   * @return all the columns that are ScalarFunctionColumns.
   */
  def getScalarFunctionColumns: List[ScalarFunctionColumn] = {
    var result: List[ScalarFunctionColumn] = Lists.newArrayList[ScalarFunctionColumn]
    for (col <- columnPatterns.keySet.asScala) {
      for (innerCol <- col.getAllScalarFunctionColumns.asScala) {
        if (!result.contains(innerCol)) {
          result add innerCol
        }
      }
    }
    result
  }

  /**
   * Returns all the columns that are AggregationColumns.
   *
   * @return All the columns that are AggregationColumns.
   */
  def getAggregationColumns: List[AggregationColumn] = {
    var result: List[AggregationColumn] = Lists.newArrayList[AggregationColumn]
    for (col <- columnPatterns.keySet.asScala) {
      result addAll col.getAllAggregationColumns
    }
    result
  }

  /**
   * Returns a string that when fed into the query parser, produces a QueryFormat equal to this one.
   * The string returned does not contain the FORMAT keyword.
   *
   * @return The query string.
   */
  def toQueryString: String = {
    val builder = new StrBuilder
    val stringList: List[String] = Lists.newArrayList[String]
    for (col <- columnPatterns.keySet.asScala) {
      val pattern = columnPatterns get col
      stringList.add(col.toQueryString + " " + Query.stringToQueryStringLiteral(pattern))
    }
    builder.appendWithSeparators(stringList, ", ")
    builder.toString
  }

}

