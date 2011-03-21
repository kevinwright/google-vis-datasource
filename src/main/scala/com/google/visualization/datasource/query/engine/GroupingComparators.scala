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
package engine

import com.google.common.collect.{Ordering => GoogOrdering}
import datatable.value.Value
import query.AggregationColumn
import java.{util => ju}
import ju.Comparator

import collection.JavaConverters._

/**
 * Contains several comparators that are used by the grouping and pivoting
 * mechanism.
 *
 * @author Yonatan B.Y.
 */
object GroupingComparators {
  /**
   * Return a comparator that compares ColumnTitles by first comparing their
   * lists of (pivot) values "lexicographically" (by using
   * VALUE_LIST_COMPARATOR) and if these are equal, compares the
   * ColumnAggregation within the title, by simply finding their index in the
   * given list, and comparing the indices, i.e., two ColumnTitles that have
   * the same pivot values, will compare according to which ColumnTitle's
   * ColumnAggregation comes first in the given list. If a ColumnTitle's
   * ColumnAggregation isn't in the given list, then it shouldn't be
   * compared using this comparator and a runtime exception will be thrown.
   *
   * @param columnAggregations The list that orders the ColumnAggregations.
   *
   * @return The comparator.
   */
  def getColumnTitleDynamicComparator(columnAggregations: ju.List[AggregationColumn]): Ordering[ColumnTitle] =
    new ColumnTitleDynamicComparator(columnAggregations)

  /**
   * Compares a list of values "lexicographically", i.e., if l1 = (x1...xn) and
   * l2 = (y1...ym) then compare(l1, l2) will be compare(xi,yi) when i =
   * argmin_k(compare(xk, yk) != 0). The result is 0 if no such k exists and
   * both lists are of the same size. If the lists are not of the same size and no
   * such k exists, then the longer list is considered to be greater.
   */
  val VALUE_LIST_COMPARATOR = new Ordering[ju.List[Value]] {
    def compare(a: ju.List[Value], b: ju.List[Value]) =
      ValueSeqOrdering.compare(a.asScala, b.asScala)
  }
  
  object ValueSeqOrdering extends Ordering[Seq[Value]] {
    def compare(a: Seq[Value], b: Seq[Value]): Int = {
      val i = (a zip b) indexWhere { case (a,b) => (a compareTo b) != 0 }
      if (i < a.size) 1 else if (i < b.size) -1 else 0
    }
  }
  
  object RowTitleOrdering extends Ordering[RowTitle] {
    def compare(col1: RowTitle, col2: RowTitle): Int = {
      ValueSeqOrdering.compare(col1.values, col2.values)
    }
  }
  /**
   * Compares RowTitles by comparing their list of values "lexicographically",
   * i.e. by using VALUE_LIST_COMPARATOR on them.
   */
  val ROW_TITLE_COMPARATOR = RowTitleOrdering

  /**
   * A comparator that compares {@link ColumnTitle}s and is parameterized by a
   * list of {@link com.google.visualization.datasource.query.AggregationColumn}s.
   * <p>
   * It compares ColumnTitles by first comparing their
   * lists of (pivot) values "lexicographically" (by using
   * {@code VALUE_LIST_COMPARATOR}) and if these are equal, compares the
   * ColumnAggregation within the title, by simply finding their index
   * in the given list, and comparing the indices, i.e., two ColumnTitles that
   * have the same pivot values, will compare according to which ColumnTitle's
   * ColumnAggregation comes first in the given list. If a ColumnTitle's
   * ColumnAggregation isn't in the given list, then it shouldn't be
   * compared using this comparator and a runtime exception will be thrown.
   */
  private class ColumnTitleDynamicComparator(aggregations: ju.List[AggregationColumn]) extends Ordering[ColumnTitle] {
    val aggregationsComparator = GoogOrdering explicit aggregations

    /**
     * Compares the ColumnTitles according to the logic described in the
     * description of this class.
     *
     * @param col1 The first ColumnTitle
     * @param col2 The second ColumnTitle
     *
     * @return The compare result.
     */
    def compare(col1: ColumnTitle, col2: ColumnTitle): Int = {
      val listCompare: Int = VALUE_LIST_COMPARATOR.compare(col1.getValues, col2.getValues)
      if (listCompare != 0) listCompare
      else aggregationsComparator.compare(col1.aggregation, col2.aggregation)
    }

  }

}
