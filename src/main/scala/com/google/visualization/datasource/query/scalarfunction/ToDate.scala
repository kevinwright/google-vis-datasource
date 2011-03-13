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
package scalarfunction

import base.InvalidQueryException
import datatable.value.{DateTimeValue, DateValue, NumberValue, Value, ValueType}
import com.ibm.icu.util.{GregorianCalendar, TimeZone}
import java.util.{Date, List}

import collection.JavaConverters._

object ToDate extends ToDate

/**
 * The unary scalar function toDate().
 * Transforms the given value to date value.
 * If the given value is already a date, it's trivial.
 * If the given value is a date-time, it extracts its date part.
 * If the given value is number, it treats that number as the number of milliseconds since the
 * Epoch, and returns the corresponding date.
 *
 * @author Liron L.
 */
class ToDate extends ScalarFunction {
  val functionName = "toDate"
  def getFunctionName = functionName

  /**
   * Executes the scalar function toDate() on the given values. The list is expected to be
   * of length 1, and the value in it should be of an appropiate type.
   * The method does not validate the parameters, the user must check the
   * parameters before calling this method.
   *
   * @param values A list with the values that the scalar function is performed on them.
   *
   * @return A date value with the appropriate date.
   */
  override def evaluate(values: List[Value]): Value = {
    var value: Value = values.get(0)
    var date: Date = null
    var gc: GregorianCalendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"))
    if (value.isNull) {
      return DateValue.getNullValue
    }
    value.getType match {
      case ValueType.DATE =>
        value.asInstanceOf[DateValue]
      case ValueType.DATETIME =>
        new DateValue(((value.asInstanceOf[DateTimeValue]).getObjectToFormat).asInstanceOf[GregorianCalendar])
      case ValueType.NUMBER =>
        date = new Date((value.asInstanceOf[NumberValue]).getValue.asInstanceOf[Long])
        gc.setTime(date)
        new DateValue(gc)
      case _ =>
        throw new RuntimeException("Value type was not found: " + value.getType)
    }
  }

  /**
   * Returns the return type of the function. In this case, DATE. The method
   * does not validate the parameters, the user must check the parameters
   * before calling this method.
   *
   * @param types A list of the types of the scalar function parameters.
   *
   * @return The type of the returned value: DATE.
   */
  override def getReturnType(types: List[ValueType]): ValueType = {
    return ValueType.DATE
  }

  /**
   * Validates that there is only 1 parameter given for this function, and
   * that its type is either DATE, DATETIME or NUMBER. Throws a
   * ScalarFunctionException if the parameters are invalid.
   *
   * @param types A list with parameters types.
   *
   * @throws InvalidQueryException Thrown if the parameters are invalid.
   */
  override def validateParameters(types: List[ValueType]): Unit = {
    if (types.size != 1) {
      throw new InvalidQueryException("Number of parameters for the " + functionName + " function is wrong: " + types.size)
    }
    else if ((types.get(0) != ValueType.DATETIME) && (types.get(0) != ValueType.DATE) && (types.get(0) != ValueType.NUMBER)) {
      throw new InvalidQueryException("Can't perform the function '" + functionName + "' on values that are not date, dateTime or number values")
    }
  }

  /**
   * {@inheritDoc}
   */
  override def toQueryString(argumentsQueryStrings: List[String]): String = {
    return functionName + "(" + argumentsQueryStrings.get(0) + ")"
  }
}

