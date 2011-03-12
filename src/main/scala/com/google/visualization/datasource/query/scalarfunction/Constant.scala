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
import datatable.value.{Value, ValueType}
import java.util.List

/**
 * A constant function, i.e., a 0-ary function that always returns the same value, given at
 * construction time. The value can be of any supported {@link ValueType}.
 *
 * @author Liron L.
 */
class Constant(value: Value) extends ScalarFunction {
  /**
   * {@inheritDoc}
   */
  def getFunctionName = value.toQueryString

  /**
   * Executes the scalar function constant(). The <code>values</code> parameter is ignored.
   * Returns the value supplied at construction time.
   *
   * @param values Ignored.
   *
   * @return The value supplied at construction time.
   */
  def evaluate(values: List[Value]) = value

  /**
   * Returns the return type of the function. This matches the type of the value
   * supplied at construction time. The <code>types</code> parameter is ignored.
   *
   * @param types Ignored.
   *
   * @return The return type of this function.
   */
  def getReturnType(types: List[ValueType]) = value.getType

  /**
   * Validates that there are no parameters given for the function. Throws a
   * ScalarFunctionException otherwise.
   *
   * @param types A list with parameters types. Should be empty for this type of function.
   *
   * @throws InvalidQueryException Thrown if the parameters are invalid.
   */
  def validateParameters(types: List[ValueType]): Unit = {
    if (types.size != 0) {
      throw new InvalidQueryException("The constant function should not get " + "any parameters")
    }
  }


  /**
   * {@inheritDoc}
   */
  def toQueryString(argumentsQueryStrings: List[String]): String = {
    return value.toQueryString
  }
}

