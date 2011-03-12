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

import com.google.common.collect.{ImmutableList, Lists, Sets}
import datatable.{DataTable, TableRow}
import org.apache.commons.lang.text.StrBuilder
import java.{util => ju, lang => jl}
import ju.{List, Set}

import collection.JavaConverters._

/**
 * A compound filter.
 * This filter is a logical aggregation of other filters. Currently, 
 * union (OR) and intersection (AND) are supported. An OR complex filter matches if
 * any of its sub-filters match. An AND complex filter matches if all of
 * its sub-filters match.
 *
 * @author Yonatan B.Y.
 */
class CompoundFilter(operator: CompoundFilterLogicalOperator, subFilters: List[QueryFilter]) extends QueryFilter {

  import CompoundFilterLogicalOperator._
  /**
   * Implements isMatch (from the QueryFilter interface) by recursively calling
   * isMatch on each of the sub-filters, and using the compound filter type to
   * determine the result.
   * Note:
   * 1. This uses short-circuit evaluation, i.e., an OR filter decides on a true
   *    result the moment it finds a true result among its sub-filters, without
   *    continuing to check the rest of the sub-filters; and dually for an AND
   *    filter.
   * 2. The sub-filters are evaluated in order.
   * 3. If the collection of sub-filters is empty, a RuntimeException is
   *    thrown.
   *
   * @param table The table containing this row.
   * @param row The row to check.
   *
   * @return true if this row should be part of the result set, false otherwise.
   */
  def isMatch(table: DataTable, row: TableRow): Boolean = {
    if (subFilters.isEmpty) {
      throw new RuntimeException("Compound filter with empty subFilters " + "list")
    }
    for (subFilter <- subFilters.asScala) {
      val result = subFilter.isMatch(table, row)
      if (
        ((operator == AND) && !result) ||
        ((operator == OR) && result))
      {
        return result
      }
    }
    operator == AND
  }

  /**
   * Returns all the columnIds this filter uses, in this case the union of all
   * the results of getAllColumnIds() of all its subfilters.
   *
   * @return All the columnIds this filter uses.
   */
  override def getAllColumnIds: Set[String] = {
    val result: Set[String] = Sets.newHashSet[String]
    for (subFilter <- subFilters.asScala) {
      result addAll subFilter.getAllColumnIds
    }
    result
  }

  /**
   * Returns a list of all scalarFunctionColumns this filter uses, in this case
   * the union of all the results of getScalarFunctionColumns() of all its
   * sub-filters.
   *
   * @return A list of all scalarFunctionColumns this filter uses.
   */
  override def getScalarFunctionColumns: List[ScalarFunctionColumn] = {
    val result: List[ScalarFunctionColumn] = Lists.newArrayList[ScalarFunctionColumn]
    for (subFilter <- subFilters.asScala) {
      result addAll subFilter.getScalarFunctionColumns
    }
    result
  }

  /**
   * {@InheritDoc}
   */
  protected[query] override def getAggregationColumns: List[AggregationColumn] = {
    val result: List[AggregationColumn] = Lists.newArrayList[AggregationColumn]
    for (subFilter <- subFilters.asScala) {
      result addAll subFilter.getAggregationColumns
    }
    result
  }

  /**
   * Returns the list of sub-filters associated with this CompoundFilter.
   *
   * @return The list of sub-filters associated with this CompoundFilter.
   */
  def getSubFilters: List[QueryFilter] =
    ImmutableList.copyOf(subFilters: jl.Iterable[QueryFilter])

  /**
   * Returns the logical operator associated with this CompoundFilter.
   *
   * @return The logical operator associated with this CompoundFilter.
   */
  def getOperator: CompoundFilterLogicalOperator = operator

  /**
   * {@inheritDoc}
   */
  override def toQueryString: String = {
    val subFilterStrings: List[String] = Lists.newArrayList[String]
    for (filter <- subFilters.asScala) {
      subFilterStrings add ("(" + filter.toQueryString + ")")
    }
    new StrBuilder().appendWithSeparators(subFilterStrings, " " + operator.name + " ").toString
  }

}

