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
package parser

import base.{InvalidQueryException, MessagesEnum}
import com.ibm.icu.util.ULocale
import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.LogFactory

/**
 * A singleton class that can parse a user query string, i.e., accept a string such as
 * "SELECT dept, max(salary) GROUP BY dept" and return a Query object. This class basically
 * wraps the QueryParser class that is auto-generated from the QueryParser.jj specification.  
 *
 * @author Hillel M.
 */

class QueryBuilder private {
  /**
   * Log.
   */
  private val log = LogFactory.getLog(classOf[QueryBuilder])

  /**
   * Parses a user query into a Query object.
   *
   * @param tqValue The user query string.
   *
   * @return The parsed Query object.
   *
   * @throws InvalidQueryException Thrown if the query is invalid.
   */
  def parseQuery(tqValue: String): Query = parseQuery(tqValue, null)

  /**
   * Parses a user query into a Query object.
   *
   * @param tqValue The user query string.
   * @param ulocale The user locale.
   *
   * @return The parsed Query object.
   *
   * @throws InvalidQueryException Thrown if the query is invalid.
   */
  def parseQuery(tqValue: String, ulocale: ULocale): Query = {
    if (StringUtils isEmpty tqValue) new Query
    else {
      val query = try {
        QueryParser parseString tqValue
      } catch {
        case ex: ParseException =>
          val msg = ex.getMessage
          log error ("Parsing error: " + msg)
          throw new InvalidQueryException(MessagesEnum.PARSE_ERROR.getMessageWithArgs(ulocale, msg))
      }
      query setLocaleForUserMessages ulocale
      query.validate
      query
    }
  }
}

object QueryBuilder extends QueryBuilder {
  def getInstance: QueryBuilder = this
}
