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

/**
 * A value of type number. Valid values are double-precision floating-point numbers
 * (including integers) or NULL_VALUE.
 *
 * @author Yoah B.D.
 */
case class NumberValue(val value: Option[Double]) extends Value {

  def this(value: Double) = this(Some(value))

  def getType = ValueType.NUMBER

  def throwNull = throw new NullValueException("This null number has no value")

  def getValue = value getOrElse throwNull

  override def toString = value map (_.toString) getOrElse "null"

  def isNull = value.isEmpty

  /**
   * Compares this value to another value of the same type.
   *
   * @param other Other value.
   *
   * @return 0 if equal, negative if this is smaller, positive if larger.
   */
  def compareTo(other: Value): Int = {
    if (this == other) {
      return 0
    }
    var otherNumber: NumberValue = other.asInstanceOf[NumberValue]
    if (isNull) {
      return -1
    }
    if (otherNumber.isNull) {
      return 1
    }
    return java.lang.Double.compare(value.get, otherNumber.value.get)
  }

  /**
   * Returns a hash code for this number value.
   *
   * @return A hash code for this number value.
   */
  override def hashCode: Int = value map (_.hashCode) getOrElse 0

  def getObjectToFormat: Number = value map (Double box _) orNull

  protected def innerToQueryString = value map (_.toString) getOrElse "null"

}

object NumberValue {
  private val NULL_VALUE = NumberValue(None)
  def getNullValue = NULL_VALUE
  def apply(value: Double): NumberValue = NumberValue(Some(value))

}
