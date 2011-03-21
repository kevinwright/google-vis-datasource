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
package engine

import datatable.value.Value

/**
 * A "title" (identification) of a row in a pivoted and/or grouped table.
 * What identifies a row is the list of values, one value from each of the group-by columns.
 *
 * @author Yonatan B.Y.
 */
case class RowTitle(values: Seq[Value]) {
  def this() = this(null)
}

