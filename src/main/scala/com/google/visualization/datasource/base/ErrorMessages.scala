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
package com.google.visualization.datasource.base

import java.util.ListResourceBundle

/**
 * A resource bundle that contains all the error messages for a datasource in the en-US locale.
 * This is the default locale used in this library.
 *
 * @author Yaniv S.
 */
object ErrorMessages {
  /**
   * The contents of this bundle. A key to message map.
   */
  val CONTENTS: Array[Array[AnyRef]] = Array(
    Array("UNKNOWN_DATA_SOURCE_ID", "Unknown data source ID"),
    Array("ACCESS_DENIED", "Access denied"),
    Array("USER_NOT_AUTHENTICATED", "User not signed in"),
    Array("UNSUPPORTED_QUERY_OPERATION", "Unsupported query operation"),
    Array("INVALID_QUERY", "Invalid query"),
    Array("INVALID_REQUEST", "Invalid request"),
    Array("INTERNAL_ERROR", "Internal error"),
    Array("NOT_SUPPORTED", "Operation not supported"),
    Array("DATA_TRUNCATED", "Retrieved data was truncated"),
    Array("NOT_MODIFIED", "Data not modified"),
    Array("TIMEOUT", "Request timeout"),
    Array("ILLEGAL_FORMATTING_PATTERNS", "Illegal formatting patterns"),
    Array("OTHER", "Could not complete request"),
    Array("SIGN_IN", "Sign in"),
    Array("NO_COLUMN", "Column [{0}] does not exist in table."),
    Array("AVG_SUM_ONLY_NUMERIC", "'Average' and 'sum' aggreagation functions can be applied only on numeric values."),
    Array("INVALID_AGG_TYPE", "Invalid aggregation type: {0}"),
    Array("PARSE_ERROR", "Query parse error: {0}"),
    Array("CANNOT_BE_IN_GROUP_BY", "Column [{0}] cannot be in GROUP BY because it has an aggregation."),
    Array("CANNOT_BE_IN_PIVOT", "Column [{0}] cannot be in PIVOT because it has an aggregation."),
    Array("CANNOT_BE_IN_WHERE", "Column [{0}] cannot appear in WHERE because it has an aggregation."),
    Array("SELECT_WITH_AND_WITHOUT_AGG", "Column [{0}] cannot be selected both with and without aggregation in SELECT."),
    Array("COL_AGG_NOT_IN_SELECT", "Column [{0}] which is aggregated in SELECT, cannot appear in GROUP BY."),
    Array("CANNOT_GROUP_WITNOUT_AGG", "Cannot use GROUP BY when no aggregations are defined in SELECT."),
    Array("CANNOT_PIVOT_WITNOUT_AGG", "Cannot use PIVOT when no aggregations are defined in SELECT."),
    Array("AGG_IN_SELECT_NO_PIVOT", "Column [{0}] which is aggregated in SELECT, cannot appear in PIVOT."),
    Array("FORMAT_COL_NOT_IN_SELECT", "Column [{0}] which is referenced in FORMAT, is not part of SELECT clause."),
    Array("LABEL_COL_NOT_IN_SELECT", "Column [{0}] which is referenced in LABEL, is not part of SELECT clause."),
    Array("ADD_COL_TO_GROUP_BY_OR_AGG", "Column [{0}] should be added to GROUP BY, removed from SELECT, or aggregated in SELECT."),
    Array("AGG_IN_ORDER_NOT_IN_SELECT", "Aggregation [{0}] found in ORDER BY but was not found in SELECT"),
    Array("NO_AGG_IN_ORDER_WHEN_PIVOT", "Column [{0}] cannot be aggregated in ORDER BY when PIVOT is used."),
    Array("COL_IN_ORDER_MUST_BE_IN_SELECT", "Column [{0}] which appears in ORDER BY, must be in SELECT as well, " + "because SELECT contains aggregated columns."),
    Array("NO_COL_IN_GROUP_AND_PIVOT", "Column [{0}] cannot appear both in GROUP BY and in PIVOT."),
    Array("INVALID_OFFSET", "Invalid value for row offset: {0}"),
    Array("INVALID_SKIPPING", "Invalid value for row skipping: {0}"),
    Array("COLUMN_ONLY_ONCE", "Column [{0}] cannot appear more than once in {1}.")
  )
}

class ErrorMessages extends ListResourceBundle {
  /**
   * Returns the error messages.
   * Note that this method exposes the inner array. This means it can be changed by the outside
   * world. We are not cloning here to avoid the computation time hit. Please do not change the
   * inner values unless you know what you are doing. 
   */
  def getContents = ErrorMessages.CONTENTS
}

