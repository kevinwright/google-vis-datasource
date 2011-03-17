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

import java.{lang => jl}

/**
 * A value of type boolean. Valid values are TRUE, FALSE and NULL_VALUE.
 *
 * @author Yoah B.D.
 */

case class BooleanValue private (val value: Option[Boolean]) extends Value {
  import BooleanValue._

  def this(value: jl.Boolean) = this(Option(value) map {Boolean.unbox(_)})
  
  def getType = ValueType.BOOLEAN

  def getValue: Boolean =
    value getOrElse { throw new NullValueException("This null boolean has no value") }

  override def toString: String = if (this == NULL_VALUE) "null" else value.toString

  def isNull = value.isEmpty

  def compareTo(x: Value): Int = x match {
    case other if this eq other => 0
    case other: BooleanValue if this.isNull => -1
    case other: BooleanValue if other.isNull => 1
    case other: BooleanValue if this.value == other.value => 0
    case _ if this.value.get => 1
    case _ => -1
  }

  def getObjectToFormat = value map {_.asInstanceOf[AnyRef]} orNull

  /**
   * {@inheritDoc}
   */
  protected def innerToQueryString = value map {_.toString} getOrElse "false"
}

object BooleanValue {
  private val NULL_VALUE = BooleanValue(None)
  val TRUE = BooleanValue(Some(true))
  val FALSE = BooleanValue(Some(false))

  /**
   * Static method to return the null value (same one for all calls)
   *
   * @return Null value.
   */
  def getNullValue = NULL_VALUE

  /**
   * Static method to return a BooleanValue based on a given java boolean.
   * If the parameter is null, returns NULL_VALUE.
   *
   * @param value The java Boolean value to be represented.
   *
   * @return The static predefined instance of the given value.
   */
  def getInstance(value: jl.Boolean): BooleanValue = {
    if (value == null) NULL_VALUE
    else {
      if (Boolean.unbox(value)) TRUE else FALSE
    }
  }

}
