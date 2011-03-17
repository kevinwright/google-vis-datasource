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
import datatable.value.{TextValue, Value, ValueType}
import java.util.List

import collection.JavaConverters._

trait StringScalarFunction extends ScalarFunction {
  def functionName: String
  def invoke(a: String): String

  def getFunctionName = functionName

  def validateParameters(types: List[ValueType]): Unit = {
    if (types.size != 1) {
      throw new InvalidQueryException(functionName + " requires 1 parmaeter")
    }
    if (types.get(0) != ValueType.TEXT) {
      throw new InvalidQueryException(functionName + " takes a text parameter")
    }
  }

  /**
   * Returns the return type of the function. In this case, TEXT. The method
   * does not validate the parameters, the user must check the parameters
   * before calling this method.
   *
   * @param types A list of the types of the scalar function parameters.
   *
   * @return The type of the returned value: Number.
   */
  def getReturnType(types: List[ValueType]) = ValueType.TEXT

  /**
   * @param values A list that contains one text value.
   *
   * @return The transformed input text value.
   */
  def evaluate(values: List[Value]) =
    new TextValue( invoke(values.get(0).asInstanceOf[TextValue].getValue) )

  def toQueryString(argumentsQueryStrings: List[String]) =
    functionName + "(" + argumentsQueryStrings.get(0) + ")"
}

