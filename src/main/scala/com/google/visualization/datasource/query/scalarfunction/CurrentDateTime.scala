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
import datatable.value.{DateTimeValue, Value, ValueType}
import com.ibm.icu.util.{GregorianCalendar, TimeZone}
import java.util.List

object CurrentDateTime extends CurrentDateTime

/**
 * A 0-ary function that returns the current datetime. The return type is always DateTime.
 *
 * @author Liron L.
 */
class CurrentDateTime private () extends ScalarFunction {
  val functionName = "now"
  def getFunctionName = functionName

  def evaluate(values: List[Value]): Value =
    new DateTimeValue(new GregorianCalendar(TimeZone getTimeZone "GMT"))

  /**
   * Returns the return type of the function. In this case, DATETIME.
   */
  def getReturnType(types: List[ValueType]) = ValueType.DATETIME

  /**
   * Validates that there are no parameters given for the function. Throws a
   * ScalarFunctionException otherwise.
   *
   * @param types A list with parameters types. Should be empty for this type of function.
   *
   * @throws InvalidQueryException Thrown if the parameters are invalid.
   */
  def validateParameters(types: List[ValueType]): Unit = {
    if (!types.isEmpty) {
      throw new InvalidQueryException("The " + functionName + " function should not get any parameters")
    }
  }

  def toQueryString(argumentsQueryStrings: List[String]) = functionName + "()"
}

