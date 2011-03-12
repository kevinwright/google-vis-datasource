package com.google.visualization.datasource

import query.Query

/**
 * A product of splitQuery() method, composed of a data source query to be executed first by
 * the data source and a second completion query to be executed on the results of the
 * first one by the query engine.
 * The application of the first query and the second query is equivalent to the application of the
 * original query.
 *
 * @author Yonatan B.Y.
 */
case class QueryPair(dataSourceQuery: Query, completionQuery: Query) {
  def getDataSourceQuery = dataSourceQuery
  def getCompletionQuery = completionQuery
}
