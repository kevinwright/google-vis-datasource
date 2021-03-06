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
package base

/**
 * An exception to be used by callers and callees of the library.
 * Each exception has a type taken from <code>ReasonType</code>, and a
 * message that can be used to output an appropriate message to the user.
 *
 * Example:
 * new DataSourceException(ReasonType.InvalidQuery, "The query cannot be empty")
 *
 * @author Hillel M.
 */
class DataSourceException(reasonType: ReasonType, messageToUser: String) extends Exception(messageToUser) {
  def getMessageToUser = messageToUser
  def getReasonType = reasonType

  @Deprecated override def getMessage = super.getMessage

}

