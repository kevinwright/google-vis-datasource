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
import com.ibm.icu.util.{Calendar, GregorianCalendar, TimeZone}
import java.util.{Date, List}

object DateDiff extends DateDiff

/**
 * The binary scalar function datediff().
 * Returns the difference in days between two dates or date time values.
 *
 * @author Liron L.
 */
class DateDiff private () extends ScalarFunction {
  val functionName = "dateDiff"
  def getFunctionName = functionName

  /**
   * Executes this scalar function on the given values. Returns values[0] - values[1] expressed
   * as a number value denoting the number of days from one date to the other. Both values can be
   * of type date or date-time. Only the date parts of date-time values are used in the calculation.
   * Thus the returned number is always an integer.
   * The method does not validate the parameters, the user must check the
   * parameters before calling this method.
   *
   * @param values A list of values on which the scalar function will be performed.
   *
   * @return Value holding the difference, in whole days, between the two given Date/DateTime
   *     values, or a null value (of type number) if one of the values is null.
   */
  override def evaluate(values: List[Value]): Value = {
    val firstValue = values.get(0)
    val secondValue = values.get(1)

    if (firstValue.isNull || secondValue.isNull) { NumberValue.getNullValue }
    else {
      val firstDate = getDateFromValue(firstValue)
      val secondDate = getDateFromValue(secondValue)
      val calendar = new GregorianCalendar(TimeZone getTimeZone "GMT")
      calendar setTime secondDate
      new NumberValue(calendar.fieldDifference(firstDate, Calendar.DATE))
    }
  }

  /**
   * Converts the given value to date. The value must be of type date or datetime.
   *
   * @param value The given value.
   *
   * @return Date object with the same value as the given value.
   */
  private def getDateFromValue(value: Value): Date = {
    val calendar =
      if (value.getType == ValueType.DATE) {
        (value.asInstanceOf[DateValue]).getObjectToFormat
      } else {
        (value.asInstanceOf[DateTimeValue]).getObjectToFormat
      }
    
    calendar.getTime
  }

  /**
   * Returns the return type of the function. In this case, NUMBER. The method
   * does not validate the parameters, the user must check the parameters
   * before calling this method.
   *
   * @param types A list of the types of the scalar function parameters.
   *
   * @return The type of the returned value: Number.
   */
  override def getReturnType(types: List[ValueType]): ValueType =
    ValueType.NUMBER

  /**
   * Validates that there are only 2 parameters given for this function, and that their types are
   * either DATE or DATETIME. Throws a ScalarFunctionException if the parameters are invalid.
   *
   * @param types A list with parameters types.
   *
   * @throws InvalidQueryException Thrown if the parameters are invalid.
   */
  override def validateParameters(types: List[ValueType]): Unit = {
    if (types.size != 2) {
      throw new InvalidQueryException("Number of parameters for the " + functionName + " function is wrong: " + types.size)
    }
    else if ((!isDateOrDateTimeValue(types.get(0))) || (!isDateOrDateTimeValue(types.get(1)))) {
      throw new InvalidQueryException("Can't perform the function '" + functionName + "' on values that are not a Date or a DateTime values")
    }
  }

  /**
   * Returns true if the given type is Date or DateTime.
   *
   * @param type The given type.
   *
   * @return True if the given type is Date or DateTime.
   */
  private def isDateOrDateTimeValue(valType : ValueType) =
    valType == ValueType.DATE || valType == ValueType.DATETIME

  override def toQueryString(argumentsQueryStrings: List[String]) =
    functionName + "(" + argumentsQueryStrings.get(0) + ", " + argumentsQueryStrings.get(1) + ")"
}

