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
import datatable.value.{NumberValue, Value, ValueType}
import java.util.List

import collection.JavaConverters._

object Product extends Product

/**
 * The binary scalar function product().
 * Returns the product of two number values.
 *
 * @author Liron L.
 */
class Product extends ScalarFunction {

  val functionName = "product"
  def getFunctionName = functionName

  /**
   * Executes the scalar function product() on the given values. Returns the
   * product of the given values. All values are number values.
   * The method does not validate the parameters, the user must check the
   * parameters before calling this method.
   *
   * @param values A list of values on which the scalar function is performed.
   *
   * @return Value with the product of all given values, or number null value
   *     if one of the values is null.
   */
  override def evaluate(values: List[Value]): Value = {
    if (values.get(0).isNull || values.get(1).isNull) {
      return NumberValue.getNullValue
    }
    var product: Double = (values.get(0).asInstanceOf[NumberValue]).getValue * (values.get(1).asInstanceOf[NumberValue]).getValue
    return new NumberValue(product)
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
  def getReturnType(types: List[ValueType]) = ValueType.NUMBER

  /**
   * Validates that all function parameters are of type NUMBER, and that there
   * are exactly 2 parameters. Throws a ScalarFunctionException otherwise.
   *
   * @param types A list with parameters types.
   *
   * @throws InvalidQueryException Thrown if the parameters are invalid.
   */
  override def validateParameters(types: List[ValueType]): Unit = {
    if (types.size != 2) {
      throw new InvalidQueryException("The function " + functionName + " requires 2 parmaeters ")
    }
    if (types.asScala exists (_ != ValueType.NUMBER)) {
      throw new InvalidQueryException("Can't perform the function " + functionName + " on values that are not numbers")
    }
  }

  /**
   * {@inheritDoc}
   */
  def toQueryString(argumentsQueryStrings: List[String]) =
    "(" + argumentsQueryStrings.get(0) + " * " + argumentsQueryStrings.get(1) + ")"
}
