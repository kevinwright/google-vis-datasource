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
import java.{util=> ju, lang=> jl}
import ju.{List, Map, Set}

import collection.JavaConverters._

/**
 * Describes the labels of a Query, as passed in the query under
 * the "label" clause.
 *
 * A label can optionally be specified for each column.
 * If not specified, each column type has a default label.
 *
 * @author Itai R.
 */
case class QueryLabels(columnLabels: Map[AbstractColumn, String]) {
  private final val log = LogFactory.getLog(classOf[QueryLabels])

  /**
   * Default empty constructor, with no labels. Labels can be added later.
   */
  def this() = this (Maps.newHashMap[AbstractColumn, String])

  /**
   * Adds a column label.
   * Validates that the column ID is not already specified.
   *
   * @param column The column to which the label is assigned.
   * @param label The assigned label.
   *
   * @throws InvalidQueryException Thrown if the column is already specified.
   */
  def addLabel(column: AbstractColumn, label: String): Unit = {
    if (columnLabels.keySet contains column) {
      val messageToLogAndUser = "Column [" + column.toString + "] is " + "specified more than once in LABEL."
      log error messageToLogAndUser
      throw new InvalidQueryException(messageToLogAndUser)
    }
    columnLabels.put(column, label)
  }

  /**
   * Returns the label of the specified column, or null if no label was
   * specified.
   *
   * @param column The column for which the label is required.
   *
   * @return The label, or null if no label was specified for this column.
   */
  def getLabel(column: AbstractColumn) = columnLabels get column

  /**
   * Returns an immutable set of the columns for which a label was specified.
   *
   * @return An immutable set of the columns for which a label was specified.
   */
  def getColumns: Set[AbstractColumn] = ImmutableSet copyOf (columnLabels.keySet: jl.Iterable[AbstractColumn])

  /**
   * Returns all the columns that are ScalarFunctionColumns including scalar
   * functions columns that are inside other scalar function columns
   * (e.g., sum(year(a), year(b))).
   *
   * @return all the columns that are ScalarFunctionColumns.
   */
  def getScalarFunctionColumns: List[ScalarFunctionColumn] = {
    val result: List[ScalarFunctionColumn] = Lists.newArrayList[ScalarFunctionColumn]
    for (col <- columnLabels.keySet.asScala) {
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
    val result: List[AggregationColumn] = Lists.newArrayList[AggregationColumn]
    for (col <- columnLabels.keySet.asScala) {
      result addAll col.getAllAggregationColumns
    }
    result
  }

  /**
   * Returns a string that when fed into the query parser, produces a QueryLabels equal to this one.
   * The string returned does not contain the LABEL keyword.
   *
   * @return The query string.
   */
  def toQueryString: String = {
    val builder = new StrBuilder
    val stringList: List[String] = Lists.newArrayList[String]
    for (col <- columnLabels.keySet.asScala) {
      var label = columnLabels get col
      stringList add (col.toQueryString + " " + (Query stringToQueryStringLiteral label))
    }
    builder.appendWithSeparators(stringList, ", ")
    builder.toString
  }
}

