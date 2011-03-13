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
import java.{util=>ju}

/**
 * A scalar function.
 * A scalar function can have any arity, including 0, and can also have dynamic arity.
 * It must always produce a single value. The return type of such a function may depend on the
 * number and types of its arguments but never on their values. An instance of a class that
 * implements this interface can be used within a ScalarFunctionColumn.
 *
 * @author Liron L.
 */
abstract trait ScalarFunction {
  /**
   * Returns the function's name.
   *
   * @return The function's name.
   */
  def getFunctionName: String

  /**
   * Executes the scalar function on the given list of values, and returns the
   * Value which is the result of the execution the function.
   *
   * @param values The given list of parameter values for the scalar function.
   *
   * @return A Value which is the result of the executing the function.
   */
  def evaluate(values: ju.List[Value]): Value

  /**
   * Returns the return type of the function, given specific types for its parameters. Some
   * functions can be polymorphic, i.e., their return type changes according to the types of their
   * parameters.
   *
   * @param types A list of the types of the function's parameters.
   *
   * @return The return type of the function.
   */
  def getReturnType(types: ju.List[ValueType]): ValueType

  /**
   * Validates that the number and types of the function parameters are valid.
   * Throws an exception if they are not valid.
   *
   * @param types The given types of the function's parameters.
   *
   * @throws InvalidQueryException Thrown when the parameters are invalid.
   */
  @throws(classOf[InvalidQueryException])
  def validateParameters(types: ju.List[ValueType]): Unit

  /**
   * Returns a string that when given to the query parser will yield a similar scalar function.
   * Takes as arguments the query strings of the arguments.
   *
   * @param argumentQueryStrings The query strings of the actual arguments.
   *
   * @return The query string.
   */
  def toQueryString(argumentQueryStrings: ju.List[String]): String
}

