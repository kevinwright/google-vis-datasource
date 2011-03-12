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
package com.google.visualization.datasource.util

/**
 * This class contains all information required to connect to the sql database.
 *
 * @author Liron L.
 */
case class SqlDatabaseDescription(url: String, user: String, var password: String, tableName: String) {
  def getUrl = url
  def getUser = user
  def getPassword = password
  def setPassword(password: String): Unit = { this.password = password }
  def getTableName = tableName
}

