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

import datatable.TableRow
import datatable.value.Value
import com.ibm.icu.util.ULocale
import java.{util => ju}
import ju.Comparator
import ju.List

import collection.JavaConverters._

/**
 * A comparator comparing two {@link TableRow}s according to the query's ORDER BY, i.e.,
 * {@link QuerySort}.
 *
 * @author Yoah B.D.
 */
class TableRowComparator(sort: QuerySort, locale: ULocale, columnLookup: ColumnLookup)
extends Comparator[TableRow] {

  private val columns = sort.getSortColumns.asScala.toArray

  /**
   * The columns to order by, in sequence of importance.
   */
  private var sortColumns = columns map (_.getColumn)

  /**
   * The sort order of the columns to order by, in sequence of importance. Will
   * be of same size as sortColumnIndex.
   */
  private var sortColumnOrder = columns map (_.getOrder)
  /**
   * A value comparator.
   */
  private val valueComparator = Value getLocalizedComparator locale

  /**
   * Compares two arguments for order. Returns a negative integer, zero, or
   * a positive integer if the first argument is less than, equal to, or greater
   * than the second.
   *
   * @param r1 the first row to be compared.
   * @param r2 the second row to be compared.
   *
   * @return a negative integer, zero, or a positive integer as the first
   *     argument is less than, equal to, or greater than the second.
   */
  def compare(r1: TableRow, r2: TableRow): Int = {
    val ccCols = sortColumns.iterator map { col =>
      valueComparator.compare(
        col.getValue(columnLookup, r1),
        col.getValue(columnLookup, r2))
    }

    val c = (ccCols zip sortColumnOrder.iterator) find (_._1 != 0) map {
      case(cc, ord) => if (ord == SortOrder.ASCENDING) cc else -cc
    }

    c getOrElse 0
  }
}

