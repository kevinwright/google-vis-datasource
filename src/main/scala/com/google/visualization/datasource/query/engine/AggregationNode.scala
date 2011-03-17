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

import com.google.common.collect.Maps
import datatable.DataTable
import datatable.value.Value
import query.AggregationType

import java.{util => ju}

import collection.JavaConverters._

/**
 * An aggregation node is a node in an aggregation tree. This node holds a value, equal to the
 * value in the corresponding group-by column. It also holds: value aggregators (one for each
 * aggregation column), a reference to its parent in the tree, and a set of references to its
 * children. Each child is associated with a unique value, which corresponds to the value held
 * in the child node. See {@link AggregationTree} for more details.
 *
 * @param columnsToAggregate
 *   A set of ids of the columns to aggregate (aggregation columns).
 * @param table
 *   The table.
 * @param parent
 *   The parent of this node in the aggregation tree.
 * @param value
 *   The value of this node. This value is unique among the siblings of this
 *   node (in the aggregation tree), and is used for navigation. Note that the value is the same
 *   as the value used as a key to point to this AggregationNode in the parent's {@link #children}
 *   map.
 *
 * @author Yoav G.
 */
class AggregationNode private (
  columnsToAggregate: Set[String],
  table: DataTable,
  parent: AggregationNode,
  value: Value)
{

  def this(columnsToAggregate: Set[String], table: DataTable) =
    this(columnsToAggregate, table, null, null)

  /**
   * Maps a column id to its aggregator. The column id should belong to the list of aggregation
   * columns.
   */
  private val columnAggregators: Map[String, ValueAggregator] =
    (columnsToAggregate map {columnId =>
      columnId -> new ValueAggregator(table.getColumnDescription(columnId).getType)
    }).toMap

  /**
   * Maps a value to a child of this node (which is also an aggregation node). ‎The value is the
   * same as the {@link #value} that will be stored in the child, i.e.,
   * <code>children.get(X).getValue()</code> should equal <code>X</code>.
   */
  private var children = Map.empty[Value, AggregationNode]

  /**
   * Aggregates values using the value aggregators of this node.
   *
   * @param valuesByColumn Maps a column id to the value that needs be aggregated (for that column).
   */
  def aggregate(valuesByColumn: Map[String, Value]): Unit = {
    for (columnId <- valuesByColumn.keySet) {
      columnAggregators(columnId) aggregate valuesByColumn(columnId)
    }
  }

  /**
   * Returns the aggregation value of a specific column and type.
   *
   * @param columnId The requested column id.
   * @param type The requested aggregation type.
   *
   * @return The aggregation values of a specific column.
   */
  def getAggregationValue(columnId: String, aggType: AggregationType): Value =
    (columnAggregators get columnId) map {
      _ getValue aggType
    } getOrElse {
      throw new IllegalArgumentException("Column " + columnId + " is not aggregated")
    }

  /**
   * Returns the child of this node defined by a specific value.
   *
   * @param v The value.
   *
   * @return The child of this node defined by a specific value.
   */
  def getChild(v: Value): AggregationNode =
    (children get v) getOrElse {
      throw new NoSuchElementException("Value " + v + " is not a child.")
    }

  /**
   * Returns true if a node contains a child (identified by value) and false
   * otherwise.
   *
   * @param v The value of the child.
   *
   * @return True if this node contains a child (identified by value) and false
   * otherwise.
   */
  def containsChild(v: Value): Boolean = children contains v

  /**
   * Adds a new child.
   *
   * @param key The value defining the new child.
   * @param columnsToAggregate The ids of the columns to aggregate.
   * @param table The table.
   */
  def addChild(key: Value, columnsToAggregate: Set[String], table: DataTable): Unit = {
    if (children contains key) {
      throw new IllegalArgumentException("A child with key: " + key + " already exists.")
    }
    val node = new AggregationNode(columnsToAggregate, table, this, key)
    children += (key -> node)
  }

  def getChildren: Map[Value, AggregationNode] = children

  /**
   * Returns the value of this node. This is also the key of this node in the
   * children set of this parent.
   *
   * @return The value of this node.
   */
  def getValue = value

  protected[engine] def getParent = parent

}

