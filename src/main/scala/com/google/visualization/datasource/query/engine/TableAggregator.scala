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

import datatable.{DataTable, TableRow}
import datatable.value.Value
import java.{util => ju}

import collection.JavaConverters._

/**
 * Aggregates a DataTable according to specific row groups. The groups are defined by an ordered
 * list of group-by columns. For instance, if the list is {"Name", "Revenue"} then each unique
 * pair of values of these columns (e.g., {"John", 300}, {"John", 19}, {"Sarah", 2222}) defines a
 * group, where the group includes all of the rows that share the values defined by the pair (for
 * example all the rows where value("Name") = "John" and value("Cost") = 300). In addition there is
 * a group for each unique name ({"John"}, {"Sarah"}) and an additional group that contains all the
 * rows in the table.
 * The groups described above are kept in an aggregation tree. The root of the tree contains
 * aggregation information for the group of all rows. Each level of the tree (except for the root
 * level) is associated with an aggregation column. Each node of the tree is associated with a
 * value (of some cell in the column defining the node's level). A path in the tree from the root
 * to a node is represented by an ordered list of values, and is associated with the group of data
 * rows identified by this list. In our example the tree contains one path of length 0 ({}), two
 * paths of length 1 ({"John"}, {"Sarah"}), and three paths of length two ({"John", 300},
 * {"John", 19}, {"Sarah", 2222}).
 *
 * The aggregation data stored is all aggregation data possible for the columns to aggregate (also
 * called aggregation columns): the minimum, maximum, count, average, and sum, each of these
 * where applicable.
 *
 * @author Yoav G.
 */
class TableAggregator(jgroupByColumns: ju.List[String], jaggregateColumns: ju.Set[String], table: DataTable) {
  /**
   * An ordered list of columns to group by.
   */
  private val groupByColumns = jgroupByColumns.asScala.toSeq
  /**
   * A set of columns to aggregate.
   */
  private var aggregateColumns = jaggregateColumns.asScala.toSet
  /**
   * An aggregation tree is the logical data structure to use for grouping.
   */
  private val tree = new AggregationTree(aggregateColumns.asJava, table)
  table.getRows.asScala foreach { row =>
    tree.aggregate(
      getRowPath(
        row,
        table,
        groupByColumns.size - 1),
      getValuesToAggregate(row, table))
  }

  /**
   * Creates a path for the aggregation tree defined by a table row.
   *
   * @param row The table row.
   * @param table The table.
   * @param depth The depth of the desired path.
   *
   * @return A path for the aggregation tree defined by the table row.
   */
  def getRowPath(row: TableRow, table: DataTable, depth: Int): AggregationPath = {
    val result = new AggregationPath
    for(i <- 0 until depth) {
      val columnId = groupByColumns(i)
      var curValue = row.getCell(table getColumnIndex columnId).getValue
      result add curValue
    }
    result
  }

  /**
   * Returns a set containing the paths to all the leaves in the tree.
   *
   * @return A set containing the paths to all the leaves in the tree.
   */
  def getPathsToLeaves: ju.Set[AggregationPath] = tree.getPathsToLeaves.asJava

  /**
   * Creates a map from column id to value according to the aggregation columns.
   *
   * @param row The table row.
   * @param table The table.
   *
   * @return A map from column id to value according to the aggregation columns.
   */
  private def getValuesToAggregate(row: TableRow, table: DataTable): Map[String, Value] = {
    val values = aggregateColumns map {colId => row.getCell(table getColumnIndex colId).getValue}
    (aggregateColumns zip values).toMap
  }

  /**
   * Returns the aggregation value of a specific column and type.
   *
   * @param path The aggregation path.
   * @param columnId The requested column id.
   * @param type The requested aggregation type.
   *
   * @return The aggregation values of a specific column.
   */
  def getAggregationValue(path: AggregationPath, columnId: String, aggType: AggregationType): Value =
    (tree getNode path).getAggregationValue(columnId, aggType)

}

