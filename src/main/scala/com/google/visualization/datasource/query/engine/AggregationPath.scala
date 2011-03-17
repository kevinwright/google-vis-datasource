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

import com.google.common.collect.{ImmutableList, Lists}
import datatable.value.Value
import java.{util => ju, lang => jl}

/**
 * An ordered list of values representing a path in an aggregation tree, from the root to a node.
 * Only the values are stored, not the nodes themselves.
 *
 * @author Yoav G.
 */
class AggregationPath {
  /**
   * The list of values forming the path. Each value, in turn, is used to
   * navigate down from the current node to one of its children.
   */
  val values: ju.List[Value] = Lists.newArrayList[Value]

  /**
   * Adds a value to this path.
   *
   * @param value The value to add.
   */
  def add(value: Value): Unit = values add value

  /**
   * Returns the list of values. This list is immutable.
   *
   * @return The list of values. This list is immutable.
   */
  def getValues: ju.List[Value] = ImmutableList copyOf (values: jl.Iterable[Value])

  /**
   * Reverses this path.
   */
  def reverse: Unit = ju.Collections reverse values

}

