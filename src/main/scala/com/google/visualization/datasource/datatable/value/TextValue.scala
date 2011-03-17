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
package com.google.visualization.datasource.datatable.value

import com.ibm.icu.text.Collator
import com.ibm.icu.util.ULocale
import java.util.Comparator

/**
 * A value of type text (string).
 *
 * @author Yoah B.D.
 */
case class TextValue(value: String) extends Value {
  import TextValue._
  
  if (value == null) {
    throw new NullPointerException("Cannot create a text value from null.")
  }

  def getType = ValueType.TEXT

  override def toString = value

  def isNull = { this == NULL_VALUE }

  def compareTo(other: Value): Int = {
    if (this == other) 0
    else value.compareTo((other.asInstanceOf[TextValue]).value)
  }

  def getObjectToFormat = value

  def getValue = value

  protected def innerToQueryString: String = {
    if (value contains "\"") {
      if (value contains "'" ) {
        throw new RuntimeException("Cannot run toQueryString() on string" + " values that contain both \" and '.")
      } else "'" + value + "'"
    } else "\"" + value + "\""
  }

}

object TextValue {
  /**
   * A single static null value.
   */
  private val NULL_VALUE = TextValue("")

  /**
   * Static method to return the null value (same one for all calls).
   *
   * @return Null value.
   */
  def getNullValue = NULL_VALUE

  /**
   * Returns a comparator that compares text values according to a given locale.
   *
   * @param ulocale The ulocale defining the order relation for text values.
   *
   * @return A comparator that compares text values according to a given locale.
   */
  def getTextLocalizedComparator(ulocale: ULocale): Comparator[TextValue] = {
    return new Comparator[TextValue] {
      def compare(tv1: TextValue, tv2: TextValue): Int = {
        if (tv1 == tv2) 0
        else Collator.getInstance(ulocale).compare(tv1.value, tv2.value)
      }
    }
  }

}

