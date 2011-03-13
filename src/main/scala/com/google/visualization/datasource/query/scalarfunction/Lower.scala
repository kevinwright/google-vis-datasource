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

object Lower extends Lower

/**
 * A unary scalar function that changes text to lower case.
 *
 * @author Yaniv S.
 */
class Lower extends ScalarFunction {
  val functionName = "lower"
  def getFunctionName = functionName

  /**
   * @param values A list that contains one text value.
   *
   * @return A lower-case version of the input text value.
   */
  override def evaluate(values: List[Value]) =
    new TextValue((values.get(0).asInstanceOf[TextValue]).getValue.toLowerCase)

  /**
   * @return The return type of this function - TEXT.
   */
  override def getReturnType(types: List[ValueType]) = ValueType.TEXT

  /**
   * {@inheritDoc}
   */
  override def validateParameters(types: List[ValueType]): Unit = {
    if (types.size != 1) {
      throw new InvalidQueryException(functionName + " requires 1 parmaeter")
    }
    if (types.get(0) != ValueType.TEXT) {
      throw new InvalidQueryException(functionName + " takes a text parameter")
    }
  }

  /**
   * {@inheritDoc}
   */
  override def toQueryString(argumentsQueryStrings: List[String]) =
    functionName + "(" + argumentsQueryStrings.get(0) + ")"
}


