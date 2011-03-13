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

import java.util.List

import collection.JavaConverters._

//object Difference extends Difference
//
///**
// * The binary scalar function difference().
// * Returns the difference between two number values.
// *
// * @author Liron L.
// */
//class Difference extends TwoArgNumericScalarFunction {
//  val functionName = "difference"
//  val operatorName = "-"
//
//  def invoke(a: Double, b: Double) = a - b
//
//}

object Difference extends TwoArgNumericScalarFunction {
  val functionName = "difference"
  val operatorName = "-"

  def invoke(a: Double, b: Double) = a - b

}

