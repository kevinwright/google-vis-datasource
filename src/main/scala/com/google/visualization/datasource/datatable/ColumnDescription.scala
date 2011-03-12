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
package com.google.visualization.datasource.datatable

import com.google.common.collect.Maps
import com.google.visualization.datasource.datatable.value.ValueType
import java.util.Collections
import java.util.Map

import collection.JavaConverters._

/**
 * Holds all the information we keep for a single column in a {@link DataTable}. 
 * This does not include all the values, which are kept in {@link TableCell}s. 
 * The information that is included is:
 * <ul>
 * <li>Data type (see {@link ValueType})</li>
 * <li>Id - used mainly for referencing columns in queries (see
 * {@link com.google.visualization.datasource.query.Query}))</li>
 * <li>Label - used mainly for display purposes</li>
 * <li>Custom properties - can be used for any purpose.</li>
 * </ul>
 *
 * @param id
 *   The column's identifier.
 *   This Id is used in query sort, filter and other query elements that
 *   need to refer to specific columns.
 *
 * @param pattern
 *   The column's formatting pattern. The pattern may be empty, otherwise indicates
 *   the formatting pattern in which the column is already, or should be, formatted.
 *
 * @author Yoah B.D.
 */
class ColumnDescription(id: String, valType: ValueType, var label: String, var pattern: String) extends CustomProperties {

  def this (id: String, valType: ValueType, label: String) =
    this(id, valType, label, "")

  def this() = this(null, null, null, null)

  def getId = id
  def getType = valType

  def getLabel = label
  def setLabel(label: String): Unit = {
    this.label = label
  }

  def getPattern = pattern
  def setPattern(pattern: String): Unit = {
    this.pattern = pattern
  }

  /**
   * Returns a clone of the ColumnDescription, meaning a column descriptor with the exact
   * same properties. This is a deep clone as it copies all custom properties.
   *
   * @return The cloned ColumnDescription.
   */
  override def clone: ColumnDescription = {
    val result = new ColumnDescription(id, valType, label)
    result setPattern pattern

    result.customProperties = customPropsClone
    result
  }

}

