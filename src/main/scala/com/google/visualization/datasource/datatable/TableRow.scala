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

import com.google.common.collect.ImmutableList
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.visualization.datasource.datatable.value.Value
import java.{util => ju, lang => jl}
import ju.{Collections, List, Map}

import collection.JavaConverters._

/**
 * A single row in a {@link DataTable}.
 *
 * The number of cells is expected to match the number of columns in the table, and the type
 * of the value held in each cell is expected to match the type defined for the corresponding
 * column.
 *
 * @author Yoah B.D.
 */
class TableRow extends CustomProperties {

  private var cells = Lists.newArrayList[TableCell]

  def addCell(cell: TableCell): Unit = cells.add(cell)
  def addCell(value: Value): Unit = addCell(new TableCell(value))
  def addCell(value: Double): Unit = addCell(new TableCell(value))
  def addCell(value: Boolean): Unit = addCell(new TableCell(value))
  def addCell(value: String): Unit = addCell(new TableCell(value))

  def getCells = ImmutableList.copyOf(cells: jl.Iterable[TableCell])
  def getCell(index: Int): TableCell = cells get index

  override def clone: TableRow = {
    val result = new TableRow
    for (cell <- cells.asScala) {
      result addCell cell.clone
    }
    result.customProperties = customPropsClone
    return result
  }

}

