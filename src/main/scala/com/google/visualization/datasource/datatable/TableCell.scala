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
package datatable

import com.google.common.collect.Maps
import value.BooleanValue
import value.NumberValue
import value.TextValue
import value.Value
import value.ValueType
import com.ibm.icu.util.ULocale
import java.util.Collections
import java.util.Comparator
import java.util.Map

import collection.JavaConverters._

/**
 * A single cell in a {@link DataTable}.
 *
 * Contains a value, a formatted value, and a dictionary of custom properties.
 * The value should match the type of the column in which it resides. See {@link ColumnDescription}.
 * The formatted value is used for display purposes. The custom properties can be used for any
 * purpose.
 *
 * @author Yoah B.D.
 */
object TableCell {
  /**
   * Returns a comparator that compares table cells according to their inner
   * values and, in the case of text values, according to a given locale.
   *
   * @param ulocale The ULocale defining the order relation of text values.
   *
   * @return A comparator that compares table cells according to their inner
   *     values and, in the case of text values, according to a given locale.
   */
  def getLocalizedComparator(ulocale: ULocale): Comparator[TableCell] = {
    new Comparator[TableCell] {
      def compare(cell1: TableCell, cell2: TableCell): Int = {
        if (cell1 == cell2)
          0
        else if (cell1.getType == ValueType.TEXT)
          textValueComparator.compare(cell1.value.asInstanceOf[TextValue], cell2.value.asInstanceOf[TextValue])
        else
          cell1.getValue.compareTo(cell2.getValue)
      }

      val textValueComparator: Comparator[TextValue] = TextValue.getTextLocalizedComparator(ulocale)
    }
  }
}

class TableCell(val value: Value, var formattedValue: String) extends CustomProperties {

  def this(value: Value) = this (value, null)
  def this(other: TableCell) = this(other.value, other.formattedValue)
  def this(value: String) = this(new TextValue(value))
  def this(value: Boolean) = this(BooleanValue.getInstance(value))
  def this(value: Double) = this(new NumberValue(value))

  def getValue = value
  def getType = value.getType
  def isNull = value.isNull

  def getFormattedValue = formattedValue
  def setFormattedValue(formattedValue: String): Unit = {
    this.formattedValue = formattedValue
  }

  override def toString = value.toString


  /**
   * Returns a copy of this TableCell.
   *
   * @return A copy of this TableCell.
   */
  override def clone: TableCell = {
    var result = new TableCell(value, formattedValue)
    result.customProperties = customPropsClone
    result
  }

}

