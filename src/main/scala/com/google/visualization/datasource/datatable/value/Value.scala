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
package value

import com.ibm.icu.util.ULocale
import java.util.Comparator

/**
 * An abstract value of a single cell in a table.
 * Acts as a base class for all the typed value classes.
 * A value should be an immutable class, because it is reference-copied when cloning complex
 * objects like rows or whole tables.
 *
 * @author Itai R.
 */

abstract class Value extends Comparable[Value] {
  /**
   * Returns the type of this cell.
   *
   * @return The type of this cell.
   */
  def getType: ValueType

  /**
   * Returns whether or not this cell's value is a logical null.
   *
   * @return true iff this cell's value is a logical null.
   */
  def isNull: Boolean

  /**
   * Checks if this object is equal to the given object.
   * Note that no instance can be equal to null and two
   * instances of different classes are not equal.
   *
   * @param o The object with which to compare.
   *
   * @return true if this equals o and false otherwise.
   */
  override def equals(o: Any): Boolean = {
    if ((null == o) || (this.getClass != o.asInstanceOf[AnyRef].getClass))
      false
    else
      (this compareTo o.asInstanceOf[Value]) == 0
  }

  /**
   * Returns the hashcode of this value. This is introduced here to force the
   * subclasses to define this function.
   *
   * @return The hashcode of this value.
   */
  override def hashCode: Int

  /**
   * Uses an ibm.icu.text.UFormat instance to format Values.
   * The following method returns an object representing this Value that a
   * UFormat can format.
   *
   * @return An object representing this value that can be formatted by a
   *     UFormat formatter, null for a NULL_VALUE.
   */
  def getObjectToFormat: AnyRef

  /**
   * Returns a string that, when parsed by the query parser, should return an
   * identical value. Throws an exception when called on a null value.
   *
   * @return A string form of this value.
   */
  final def toQueryString: String = {
    if (isNull) {
      throw new RuntimeException("Cannot run toQueryString() on a null value.")
    }
    return innerToQueryString
  }

  /**
   * Returns a string that, when parsed by the query parser, should return an
   * identical value. Assumes this value is not null.
   *
   * @return A string form of this value.
   */
  protected def innerToQueryString: String
}

object Value {
  /**
   * Returns the NullValue for the given type.
   *
   * @param type The value type.
   *
   * @return The NullValue for the given type.
   */
  def getNullValueFromValueType(valType: ValueType): Value = valType match {
    case ValueType.BOOLEAN => BooleanValue.getNullValue
    case ValueType.TEXT => TextValue.getNullValue
    case ValueType.NUMBER => NumberValue.getNullValue
    case ValueType.TIMEOFDAY => TimeOfDayValue.getNullValue
    case ValueType.DATE => DateValue.getNullValue
    case ValueType.DATETIME => DateTimeValue.getNullValue
  }

  /**
   * Returns a comparator that compares values according to a given locale
   * (in case of text values).
   *
   * @param ulocale The ULocale defining the order relation of text values.
   *
   * @return A comparator that compares values according to a given locale
   *     (in case of text values).
   */
  def getLocalizedComparator(ulocale: ULocale): Comparator[Value] = {
    new Comparator[Value] {
      def compare(value1: Value, value2: Value): Int = (value1,value2) match {
        case _ if (value1 == value2) =>
          0
        case (a:TextValue, b:TextValue) =>
          TextValue.getTextLocalizedComparator(ulocale).compare(a, b)
        case _ =>
          value1 compareTo value2
      }
    }
  }
}
