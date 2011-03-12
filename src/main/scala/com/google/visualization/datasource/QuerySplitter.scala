package com.google.visualization.datasource

import com.google.common.collect.Lists
import com.google.visualization.datasource.base.{DataSourceException, InvalidQueryException, ReasonType}

import org.apache.commons.logging.{Log, LogFactory}

import collection.JavaConverters._
import query._

object QuerySplitter {
  val log = LogFactory getLog QuerySplitter.getClass.getName

  /**
   * Split the query into a data source query and completion query. The data source query runs
   * first directly on the underlying data. The completion query is run by
   * {@link com.google.visualization.datasource.query.engine.QueryEngine} engine on the result of
   * the data source query.
   *
   * @param query The <code>Query</code> to split.
   * @param capabilities The capabilities supported by the data source.
   *
   * @return A split query.
   *
   * @throws DataSourceException Thrown if the capabilities are not supported.
   */
  def splitQuery(query: Query, capabilities: Capabilities): QueryPair = capabilities match {
    case Capabilities.ALL => splitAll(query)
    case Capabilities.NONE => splitNone(query)
    case Capabilities.SQL => splitSQL(query)
    case Capabilities.SORT_AND_PAGINATION => splitSortAndPagination(query)
    case Capabilities.SELECT => return splitSelect(query)
    case _ =>
      log error "Capabilities not supported."
      throw new DataSourceException(ReasonType.NOT_SUPPORTED, "Capabilities not supported.")
  }

  /**
   * Splits the query for a data source with capabilities ALL. In this case, the original query is
   * copied to the data source query and the original query is empty.
   *
   * @param query The query to split.
   *
   * @return The split query.
   */
  def splitAll(query: Query) = {
    val dataSourceQuery = new Query()
    dataSourceQuery.copyFrom(query)
    val completionQuery = new Query()
    QueryPair(dataSourceQuery, completionQuery)
  }

  /**
   * Splits the query for a data source with capabilities NONE. In this case, the original query is
   * copied to the completionQuery and the data source query is empty. Furthermore, the data source
   * query is assigned null and shouldn't be used or referenced by the data source.
   *
   * @param query The query to split.
   *
   * @return The split query.
   */
  def splitNone(query: Query) = {
    val completionQuery = new Query()
    completionQuery.copyFrom(query)
    QueryPair(null, completionQuery)
  }

  /**
   * Splits the query for a data source with capabilities SQL.
   * If the query contains scalar functions, then the query is split as if data source capabilities
   * are NONE. If the query does not contain scalar functions, then the data source query contains
   * most of the operations.
   * Because SQL cannot handle pivoting, special care needs to be taken if the query includes a
   * pivot operation. The aggregation operation required for pivoting is passed to the data source
   * query. We make use of this, with some implementation tricks. See implementation comments.
   *
   * @param query The original query.
   *
   * @return The split query.
   */
  def splitSQL(query: Query): QueryPair = {
    // Situations we currently do not support good splitting of:
    // - Queries with scalar functions.
    // - Queries with pivot that also contain labels or formatting on aggregation columns.
    if (!query.getAllScalarFunctionsColumns.isEmpty
        || (query.hasPivot
            && ((query.hasUserFormatOptions &&
                !query.getUserFormatOptions.getAggregationColumns.isEmpty)
             || (query.hasLabels && !query.getLabels.getAggregationColumns.isEmpty)))) {
      val completionQuery = new Query()
      completionQuery.copyFrom(query)
      return QueryPair(new Query(), completionQuery)
    }

    val dataSourceQuery = new Query()
    val completionQuery = new Query()

    // sql supports select, where, sort, group, limit, offset.
    // The library further supports pivot.
    if (query.hasPivot) {
      // Make the pivot columns additional grouping columns, and handle the
      // transformation later.

      val pivotColumns = query.getPivot.getColumns

      dataSourceQuery copyFrom query
      dataSourceQuery setPivot null
      dataSourceQuery setSort null
      dataSourceQuery setOptions null
      dataSourceQuery setLabels null
      dataSourceQuery setUserFormatOptions null

      dataSourceQuery setRowSkipping 0
      dataSourceQuery setRowLimit -1
      dataSourceQuery setRowOffset 0

      // Let the data source group by all grouping/pivoting columns, and let it
      // select all selection/pivoting columns, e.g., SELECT A, max(B) GROUP BY A PIVOT C turns
      // into SELECT A, max(B), C GROUP BY A, C

      var newGroupColumns = collection.mutable.Seq.empty[AbstractColumn]
      var newSelectionColumns = collection.mutable.Seq.empty[AbstractColumn]
      if (dataSourceQuery.hasGroup) {
        // Same logic applies here, no calculated columns and no aggregations.
        newGroupColumns ++= dataSourceQuery.getGroup.getColumns.asScala
      }
      newGroupColumns ++= pivotColumns.asScala
      if (dataSourceQuery.hasSelection) {
        newSelectionColumns ++= dataSourceQuery.getSelection.getColumns.asScala
      }
      newSelectionColumns ++= pivotColumns.asScala
      val group = new QueryGroup()
      newGroupColumns foreach {group.addColumn}
      dataSourceQuery setGroup group
      val selection = new QuerySelection()
      newSelectionColumns foreach {selection.addColumn}
      dataSourceQuery setSelection selection

      // Build the completion query to group by the grouping columns. Because an aggregation is
      // required, make a dummy aggregation on the original column by which the aggregation is
      // required.
      // This original column must be unique for a given set of values for the grouping/pivoting
      // columns so any aggregation operation out of MIN, MAX, AVG will return the value
      // itself and will not aggregate anything. The example from before,
      // SELECT A, max(B) GROUP BY A PIVOT C turns into SELECT A, min(max-B) GROUP BY A PIVOT C

      completionQuery copyFrom query
      completionQuery setFilter null

      val completionSelection = new QuerySelection()
      val originalSelectedColumns = query.getSelection.getColumns
      for (i <- 0 to originalSelectedColumns.size) {
        val column = originalSelectedColumns get i
        if (query.getGroup.getColumns contains column) {
          completionSelection addColumn column
        } else { // Must be an aggregation column if doesn't appear in the grouping.
          // The id here is the id generated by the data source for the column containing
          // the aggregated data, e.g., max-B.
          val id = column.getId
          // MIN is chosen arbitrarily, because there will be exactly one.
          completionSelection.addColumn(
              new AggregationColumn(new SimpleColumn(id), AggregationType.MIN))
        }
      }

      completionQuery setSelection completionSelection
    } else {
      // When there is no pivoting, sql does everything (except skipping, options, labels, format).
      dataSourceQuery copyFrom query
      dataSourceQuery setOptions null
      completionQuery setOptions query.getOptions

      // If there is skipping pagination should be done in the completion query
      if (query.hasRowSkipping) {
        dataSourceQuery setRowSkipping 0
        dataSourceQuery setRowLimit -1
        dataSourceQuery setRowOffset 0

        completionQuery copyRowSkipping query
        completionQuery copyRowLimit query
        completionQuery copyRowOffset query
      }
      if (query.hasLabels) {
        dataSourceQuery setLabels null
        val labels = query.getLabels
        val newLabels = new QueryLabels()
        for (column <- labels.getColumns.asScala) {
          newLabels.addLabel(new SimpleColumn(column.getId), labels getLabel column)
        }
        completionQuery setLabels newLabels
      }
      if (query.hasUserFormatOptions) {
        dataSourceQuery setUserFormatOptions null
        val formats = query.getUserFormatOptions
        val newFormats = new QueryFormat()
        for (column <- formats.getColumns.asScala) {
          newFormats.addPattern(new SimpleColumn(column.getId), formats getPattern column)
        }
        completionQuery setUserFormatOptions newFormats
      }
    }
    QueryPair(dataSourceQuery, completionQuery)
  }

  /**
   * Splits the query for a data source with capabilities SORT_AND_PAGINATION.
   * Algorithm: if the query has filter, grouping, pivoting or skipping requirements the query is
   * split as in the NONE case.
   * If the query does not have filter, grouping, pivoting or skipping the data source query
   * receives any sorting or pagination requirements and the completion query receives
   * any selection requirements.
   *
   * @param query The query to split.
   *
   * @return The split query.
   */
  def splitSortAndPagination(query: Query): QueryPair = {
    if (!query.getAllScalarFunctionsColumns.isEmpty) {
      val completionQuery = new Query()
      completionQuery copyFrom query
      return QueryPair(new Query(), completionQuery)
    }

    val dataSourceQuery = new Query()
    val completionQuery = new Query()
    if (query.hasFilter || query.hasGroup || query.hasPivot) {
      // The query is copied to the completion query.
      completionQuery copyFrom query
    } else {
      // The execution order of the 3 relevant operators is:
      // sort -> skip -> paginate (limit and offset).
      // Skipping is not a possible data source capability, Therefore:
      // 1. Sorting can be performed in the data source query.
      // 2. Pagination should be performed in the data source query IFF skipping
      //    isn't stated in the original query.
      dataSourceQuery setSort query.getSort
      if (query.hasRowSkipping) {
        completionQuery copyRowSkipping query
        completionQuery copyRowLimit query
        completionQuery copyRowOffset query
      } else {
        dataSourceQuery copyRowLimit query
        dataSourceQuery copyRowOffset query
      }

      completionQuery setSelection query.getSelection
      completionQuery setOptions query.getOptions
      completionQuery setLabels query.getLabels
      completionQuery setUserFormatOptions query.getUserFormatOptions
    }
    QueryPair(dataSourceQuery, completionQuery)
  }

  /**
   * Splits the query for a data source with capabilities SELECT.
   * Algorithm: the data source query receives any select operation from the query, however the
   * completion query also receives selection to properly post-process the result.
   *
   * @param query The query to split.
   *
   * @return The split query.
   */
  def splitSelect(query: Query) = {
    val dataSourceQuery = new Query()
    val completionQuery = new Query()
    if (query.getSelection != null) {
      val selection = new QuerySelection()
      for (simpleColumnId <- query.getAllColumnIds.asScala) {
        selection.addColumn(new SimpleColumn(simpleColumnId))
      }
      // Column selection can be empty. For example, for query "SELECT 1".
      dataSourceQuery setSelection selection
    }

    completionQuery copyFrom query
    QueryPair(dataSourceQuery, completionQuery)
  }

}
