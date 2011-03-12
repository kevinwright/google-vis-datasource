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

import com.google.common.collect.{Lists, Sets}
import base.{InvalidQueryException, MessagesEnum}
import com.ibm.icu.util.ULocale
import org.apache.commons.lang.text.StrBuilder
import org.apache.commons.logging.{Log, LogFactory}
import java.{util => ju, lang => jl}
import ju.{List, Set}

import collection.JavaConverters._

/**
 * Holds the query data and clauses. This class is the result of parsing the query
 * string that comes from the user.
 * The clauses that can be included in a query are: select, filter, sort, group by, pivot, options,
 * labels, format, limit, offset and skipping.
 * For more details see the
 * <a href="http://code.google.com/apis/visualization/documentation/querylanguage.html">
 * query language reference
 * </a>.
 * A Query can be executed by the query engine
 * (see: {@link com.google.visualization.datasource.DataSourceHelper#applyQuery}).
 * It can be split in 2 Query objects based on the Capabilities value
 * (see: {@link com.google.visualization.datasource.DataSourceHelper#splitQuery}).
 *
 * Note on errors: Since this class handles a user generated query, all errors
 * (of type INVALID_QUERY) should provide a descriptive error message for both
 * the user.
 *
 * @author Itai R.
 * @author Yonatan B.Y.
 */
object Query {
  private val log = LogFactory getLog this.getClass
  /**
   * Checks the given list for duplicates. Throws an exception if a duplicate
   * is found, giving as a message a description containing information on which
   * item was found to be duplicate, and in which clause it occurred (the
   * clause name is given).
   *
   * @param selectionColumns The list to check for duplicates.
   * @param clauseName The clause name to report in the exception message.
   * @param userLocale The user locale.
   *
   * @throws InvalidQueryException Thrown if a duplicate was found.
   */
  private def checkForDuplicates[T](selectionColumns: List[T], clauseName: String, userLocale: ULocale): Unit = {
    for (colA <- selectionColumns.asScala; colB <- selectionColumns.asScala) {
      if (colA == colB) {
        val args = Array(colA.toString, clauseName)
        val message = MessagesEnum.COLUMN_ONLY_ONCE.getMessageWithArgs(userLocale, args: _*)
        log error message
        throw new InvalidQueryException(message)
      }
    }
  }

  /**
   * Creates a comma separated string of the query strings of the given columns.
   *
   * @param l The list of columns.
   *
   * @return A comma separated string of the query strings of the given columns.
   */
  private[query] def columnListToQueryString(l: List[AbstractColumn]): String = {
    val builder = new StrBuilder
    val stringList = Lists.newArrayList[String]
    for (col <- l.asScala) {
      stringList add col.toQueryString
    }
    builder.appendWithSeparators(stringList, ", ")
    builder.toString
  }

  /**
   * Creates a query-language representation of the given string, i.e., delimits it with either
   * double-quotes (") or single-quotes ('). Throws a runtime exception if the given string
   * contains both double-quotes and single-quotes and so cannot be expressed in the query
   * language. 
   *
   * @param s The string to create a query-language representation for.
   *
   * @return The query-language representation of the given string.
   */
  private[query] def stringToQueryStringLiteral(s: String): String = {
    if (s.contains("\"")) {
      if (s.contains("'")) {
        throw new RuntimeException("Cannot represent string that contains both double-quotes (\") " + " and single quotes (').")
      } else { "'" + s + "'" }
    } else { "\"" + s + "\"" }
  }

}

class Query {
  import Query._
  
  private val log = LogFactory getLog classOf[Query]

  /**
   * The required sort order.
   */
  var sort: Option[QuerySort] = None
  /**
   * The required selection.
   * If the selection is null, or is empty, the original
   * selection that was defined in the report is used.
   */
  var selection: Option[QuerySelection] = None
  /**
   * The required filter.
   * If the filter is null, then the results are not filtered at all.
   */
  var filter: Option[QueryFilter] = None
  /**
   * The required grouping.
   */
  var group: Option[QueryGroup] = None
  /**
   * The required pivoting.
   */
  var pivot: Option[QueryPivot] = None
  /**
   * The number of rows to skip when selecting only a subset of the rows using
   * skipping clause, e.g., skipping 4.
   * If a skipping clause, "skip k",  is added to the query, the resulting table
   * will be consisted of the first row of every k rows of the table returned by
   * the earlier part of the query, when k corresponds to rowSkipping value.
   * For example, if rowSkipping = 10 the returned table will consist of rows 0, 10...
   * The default value is 0, meaning no skipping should be performed.
   */
  var rowSkipping: Int = 0
  /**
   * Max number of rows to return to caller.
   * If the caller specified this parameter, and the data table returned from
   * the data source contains more than this number of rows, then the result is
   * truncated, and the query result contains a warning with this reason.
   * If this value is set to -1, which can only be done internally, this is ignored.
   * (0 is a legal value, denoting no rows should be retrieved).
   */
  var rowLimit: Int = -1
  /**
   * The number of rows that should be removed from the beginning of the
   * data table.
   * Together with the row limit parameter, this enables pagination.
   * The default value is 0, (skip 0 rows) - which means start from the
   * first row.
   */
  var rowOffset: Int = 0
  /**
   * Additional options for the query.
   */
  var options: QueryOptions = null
  /**
   * Column labels as specified in the query.
   */
  var labels: QueryLabels = null
  /**
   * Column formatting patterns as specified in the query.
   */
  var userFormatOptions: QueryFormat = null
  /**
   * The user locale, Used to create localized messages.
   */
  var localeForUserMessages: ULocale = null


  def setSort(x: QuerySort): Unit = { sort = Option(x).filter(!_.isEmpty) }
  def getSort: QuerySort = sort.orNull
  def hasSort: Boolean = sort.isDefined

  def setSelection(x: QuerySelection): Unit = { selection = Option(x).filter(!_.isEmpty) }
  def getSelection: QuerySelection = selection.orNull
  def hasSelection: Boolean = selection.isDefined

  def setFilter(x: QueryFilter): Unit = { filter = Option(x) }
  def getFilter: QueryFilter = filter.orNull
  def hasFilter: Boolean = filter.isDefined

  def setGroup(x: QueryGroup): Unit = { group = Option(x).filter(!_.getColumnIds.isEmpty) }
  def getGroup: QueryGroup = group.orNull
  def hasGroup: Boolean = group.isDefined

  def setPivot(x: QueryPivot): Unit = { pivot = Option(x).filter(!_.getColumnIds.isEmpty) }
  def getPivot: QueryPivot = pivot.orNull
  def hasPivot: Boolean = pivot.isDefined

  /**
   * Returns the number of rows to skip when selecting only a subset of
   * the rows using skipping clause, e.g., skipping 4.
   * The default value is 0, meaning no skipping should be performed.
   *
   * @return The number of rows to skip between each row selection.
   */
  def getRowSkipping: Int = rowSkipping

  /**
   * Sets the number of rows to skip when selecting only a subset of
   * the rows using skipping clause, e.g., skipping 4.
   * The default value is 0, meaning no skipping should be performed.
   *
   * If there is is an attempt to set the skipping value to non positive value,
   * then an InvalidQueryException is thrown.
   *
   * @param rowSkipping The number of rows to skip between each row selection.
   *        0 value means no skipping.
   *
   * @throws InvalidQueryException Thrown if an invalid value is specified.
   */
  def setRowSkipping(x: Int): Unit = {
    if (x < 0) {
      var message = MessagesEnum.INVALID_SKIPPING.getMessageWithArgs(localeForUserMessages, x.toString)
      log error message
      throw new InvalidQueryException(message)
    }
    rowSkipping = x
  }

  /**
   * Sets the number of rows to skip when selecting only a subset of
   * the row, based on another query.
   *
   * @param originalQuery The query from which the row skipping should be taken.
   */
  def copyRowSkipping(originalQuery: Query): Unit = {
    rowSkipping = originalQuery.getRowSkipping
  }

  /**
   * Returns true if this query has a row skipping set. A value of 0 means no
   * skipping.
   *
   * @return True if this query has a row skipping set.
   */
  def hasRowSkipping: Boolean = {
    return rowSkipping > 0
  }

  /**
   * Returns the maximum number of rows to return to the caller.
   * If the caller specified this parameter, and the data table returned from
   * the data source contains more than this number of rows, then the result is
   * truncated, and the query result contains a warning with this reason.
   * If this value is set to -1 it is just ignored, but this can only be done
   * internally.
   *
   * @return The maximum number of rows to return to the caller.
   */
  def getRowLimit: Int = {
    return rowLimit
  }

  /**
   * Sets the max number of rows to return to the caller.
   * If the caller specified this parameter, and the data table returned from
   * the data source contains more than this number of rows, then the result is
   * truncated, and the query result contains a warning with this reason.
   * By default, this is set to -1, which means that no limit is requested.
   *
   * If there is is an attempt to set the limit value to any negative number
   * smaller than -1 (which means no limit), then an InvalidQueryException is
   * thrown.
   *
   * @param rowLimit The max number of rows to return.
   *
   * @throws InvalidQueryException Thrown if an invalid value is specified.
   */
  def setRowLimit(rowLimit: Int): Unit = {
    if (rowLimit < -1) {
      var messageToLogAndUser: String = "Invalid value for row limit: " + rowLimit
      log.error(messageToLogAndUser)
      throw new InvalidQueryException(messageToLogAndUser)
    }
    this.rowLimit = rowLimit
  }

  /**
   * Sets the max number of rows to return to the caller, based on another
   * query.
   *
   * @param originalQuery The query from which the row limit should be taken.
   */
  def copyRowLimit(other: Query): Unit = { rowLimit = other.getRowLimit }

  /**
   * Returns true if this query has a row limit set. A value of -1 means no
   * limit so in case of -1 this function returns true.
   *
   * @return True if this query has a row limit set.
   */
  def hasRowLimit: Boolean = { rowLimit > -1 }

  /**
   * Returns the number of rows that should be removed from the beginning of the
   * data table.
   * Together with the row limit parameter, this enables pagination.
   * The default value is 0, (skip 0 rows) - which means to start from the
   * first row.
   *
   * @return The number of rows to skip from the beginning of the table.
   */
  def getRowOffset: Int = rowOffset

  /**
   * Sets the number of rows that should be removed from the beginning of the
   * data table.
   * Together with the row limit parameter, this enables pagination.
   * The default value is 0, (skip 0 rows) - which means to start from the
   * first row.
   *
   * If there is is an attempt to set the offset value to any negative number,
   * then an InvalidQueryException is thrown.
   *
   * @param rowOffset The number of rows to skip from the beginning of the
   *     table.
   *
   * @throws InvalidQueryException Thrown if an invalid value is specified.
   */
  def setRowOffset(x: Int): Unit = {
    if (x < 0) {
      var message = MessagesEnum.INVALID_OFFSET.getMessageWithArgs(localeForUserMessages, x.toString)
      log error message
      throw new InvalidQueryException(message)
    }
    rowOffset = x
  }
  def copyRowOffset(other: Query): Unit = { rowOffset = other.getRowOffset }
  def hasRowOffset: Boolean = { rowOffset > 0 }

  def getUserFormatOptions: QueryFormat = userFormatOptions
  def setUserFormatOptions(x: QueryFormat): Unit = { userFormatOptions = x }
  def hasUserFormatOptions: Boolean =
    (userFormatOptions != null) && (!userFormatOptions.getColumns.isEmpty)

  def getLabels: QueryLabels = labels
  def setLabels(x: QueryLabels): Unit = { labels = x }
  def hasLabels: Boolean =
    (labels != null) && (!labels.getColumns.isEmpty)

  def getOptions = options
  def setOptions(x: QueryOptions): Unit = { options = x }
  def hasOptions: Boolean = {
    return (options != null) && (!options.isDefault)
  }

  /**
   * Returns true if the query is empty, i.e. has no clauses or all the clauses are empty.
   *
   * @return true if the query is empty. 
   */
  def isEmpty: Boolean = {
    return (!hasSort && !hasSelection && !hasFilter && !hasGroup && !hasPivot && !hasRowSkipping && !hasRowLimit && !hasRowOffset && !hasUserFormatOptions && !hasLabels && !hasOptions)
  }

  def setLocaleForUserMessages(x: ULocale): Unit = { localeForUserMessages = x }

  /**
   * Copies all information from the given query to this query.
   *
   * @param query The query to copy from.
   */
  def copyFrom(query: Query): Unit = {
    setSort(query.getSort)
    setSelection(query.getSelection)
    setFilter(query.getFilter)
    setGroup(query.getGroup)
    setPivot(query.getPivot)
    copyRowSkipping(query)
    copyRowLimit(query)
    copyRowOffset(query)
    setUserFormatOptions(query.getUserFormatOptions)
    setLabels(query.getLabels)
    setOptions(query.getOptions)
  }

  /**
   * Validates the query. Runs a sanity check on the query, verifies that there are no
   * duplicates, and that the query follows a basic set of rules required for its execution.
   *
   * Specifically, verifies the following:
   * - There are no duplicate columns in the clauses.
   * - No column appears both as a selection and as an aggregation.
   * - When aggregation is used, checks that all selected columns are valid (a column is valid if
   *   it is either grouped-by, or is a scalar function column the arguments of which are all 
   *   valid columns).
   * - No column is both grouped-by, and aggregated in, the select clause.
   * - No column both appears as pivot, and aggregated in, the select clause.
   * - No grouping/pivoting is used when there is no aggregation in the select clause.
   * - If aggregation is used in the select clause, no column is ordered-by that is not in the
   *   select clause.
   * - No column appears both as a group-by and a pivot.
   * - If pivoting is used, the order by clause does not contain aggregation columns.
   * - The order-by clause does not contain aggregation columns that were not defined in the select
   *   clause.
   *
   * @throws InvalidQueryException
   */
  def validate: Unit = {
    var groupColumnIds: List[String] = group map (_.getColumnIds) getOrElse Lists.newArrayList[String]
    var groupColumns: List[AbstractColumn] = group map (_.getColumns) getOrElse Lists.newArrayList[AbstractColumn]
    var pivotColumnIds: List[String] = pivot map (_.getColumnIds) getOrElse Lists.newArrayList[String]
    var selectionColumns: List[AbstractColumn] = selection map (_.getColumns) getOrElse Lists.newArrayList[AbstractColumn]
    var selectionAggregated: List[AggregationColumn] = selection map (_.getAggregationColumns) getOrElse Lists.newArrayList[AggregationColumn]
    var selectionSimple: List[SimpleColumn] = selection map (_.getSimpleColumns) getOrElse Lists.newArrayList[SimpleColumn]
    var selectedScalarFunctionColumns: List[ScalarFunctionColumn] = selection map (_.getScalarFunctionColumns) getOrElse Lists.newArrayList[ScalarFunctionColumn]
    selectedScalarFunctionColumns.addAll(selectedScalarFunctionColumns)
    var sortColumns: List[AbstractColumn] = sort map (_.getColumns) getOrElse Lists.newArrayList[AbstractColumn]
    var sortAggregated: List[AggregationColumn] = sort map (_.getAggregationColumns) getOrElse Lists.newArrayList[AggregationColumn]
    checkForDuplicates(selectionColumns, "SELECT", localeForUserMessages)
    checkForDuplicates(sortColumns, "ORDER BY", localeForUserMessages)
    checkForDuplicates(groupColumnIds, "GROUP BY", localeForUserMessages)
    checkForDuplicates(pivotColumnIds, "PIVOT", localeForUserMessages)
    if (hasGroup) {
      for (g <- group; column <- g.getColumns.asScala) {
        if (!column.getAllAggregationColumns.isEmpty) {
          var message = MessagesEnum.CANNOT_BE_IN_GROUP_BY.getMessageWithArgs(localeForUserMessages, column.toQueryString)
          log error message
          throw new InvalidQueryException(message)
        }
      }
    }
    for (p <- pivot; column <- p.getColumns.asScala) {
      if (!column.getAllAggregationColumns.isEmpty) {
        var message = MessagesEnum.CANNOT_BE_IN_PIVOT.getMessageWithArgs(localeForUserMessages, column.toQueryString)
        log error message
        throw new InvalidQueryException(message)
      }
    }
    //will bail out on the first bad thing it finds...
    for(f <- filter; aggregation <- f.getAggregationColumns.asScala) {
      val message = MessagesEnum.CANNOT_BE_IN_WHERE.getMessageWithArgs(localeForUserMessages, aggregation.toQueryString)
      log error message
      throw new InvalidQueryException(message)
    }
    for (column1 <- selectionSimple.asScala) {
      var id: String = column1.getColumnId
      for (column2 <- selectionAggregated.asScala) {
        if (id == column2.getAggregatedColumn.getId) {
          var message = MessagesEnum.SELECT_WITH_AND_WITHOUT_AGG.getMessageWithArgs(localeForUserMessages, id)
          log error message
          throw new InvalidQueryException(message)
        }
      }
    }
    if (!selectionAggregated.isEmpty) {
      for (col <- selectionColumns.asScala) {
        checkSelectedColumnWithGrouping(groupColumns, col)
      }
    }
    if (hasSelection && hasGroup) {
      for (column <- selectionAggregated.asScala) {
        val id = column.getAggregatedColumn.getId
        if (groupColumnIds contains id) {
          var message = MessagesEnum.COL_AGG_NOT_IN_SELECT.getMessageWithArgs(localeForUserMessages, id)
          log error message
          throw new InvalidQueryException(message)
        }
      }
    }
    if (hasGroup && selectionAggregated.isEmpty) {
      var message = MessagesEnum.CANNOT_GROUP_WITNOUT_AGG.getMessage(localeForUserMessages)
      log error message
      throw new InvalidQueryException(message)
    }
    if (hasPivot && selectionAggregated.isEmpty) {
      var message = MessagesEnum.CANNOT_PIVOT_WITNOUT_AGG.getMessage(localeForUserMessages)
      log error message
      throw new InvalidQueryException(message)
    }
    if (hasSort && !selectionAggregated.isEmpty) {
      for (s <- sort; column <- s.getColumns.asScala) {
        var message = MessagesEnum.COL_IN_ORDER_MUST_BE_IN_SELECT.getMessageWithArgs(localeForUserMessages, column.toQueryString)
        checkColumnInList(selection.get.getColumns, column, message)
      }
    }
    if (hasPivot) {
      for (column <- selectionAggregated.asScala) {
        val id = column.getAggregatedColumn.getId
        if (pivotColumnIds.contains(id)) {
          var message = MessagesEnum.AGG_IN_SELECT_NO_PIVOT.getMessageWithArgs(localeForUserMessages, id)
          log error message
          throw new InvalidQueryException(message)
        }
      }
    }
    if (hasGroup && hasPivot) {
      for (id <- groupColumnIds.asScala) {
        if (pivotColumnIds contains id) {
          val message = MessagesEnum.NO_COL_IN_GROUP_AND_PIVOT.getMessageWithArgs(localeForUserMessages, id)
          log error message
          throw new InvalidQueryException(message)
        }
      }
    }
    if (hasPivot && !sortAggregated.isEmpty) {
      val column  = sortAggregated.get(0)
      val message = MessagesEnum.NO_AGG_IN_ORDER_WHEN_PIVOT.getMessageWithArgs(localeForUserMessages, column.getAggregatedColumn.getId)
      log error message
      throw new InvalidQueryException(message)
    }
    for (column <- sortAggregated.asScala) {
      var message = MessagesEnum.AGG_IN_ORDER_NOT_IN_SELECT.getMessageWithArgs(localeForUserMessages, column.toQueryString)
      checkColumnInList(selectionAggregated, column, message)
    }
    val labelColumns = (if (hasLabels) labels.getColumns else Sets.newHashSet[AbstractColumn])
    val formatColumns = (if (hasUserFormatOptions) userFormatOptions.getColumns else Sets.newHashSet[AbstractColumn])
    if (hasSelection) {
      for (col <- labelColumns.asScala) {
        if (!selectionColumns.contains(col)) {
          val message = MessagesEnum.LABEL_COL_NOT_IN_SELECT.getMessageWithArgs(localeForUserMessages, col.toQueryString)
          log error message
          throw new InvalidQueryException(message)
        }
      }
      for (col <- formatColumns.asScala) {
        if (!selectionColumns.contains(col)) {
          var message = MessagesEnum.FORMAT_COL_NOT_IN_SELECT.getMessageWithArgs(localeForUserMessages, col.toQueryString)
          log error message
          throw new InvalidQueryException(message)
        }
      }
    }
  }

  /**
   * Returns all simple column IDs in use in all parts of the query.
   *
   * @return All simple column IDs in use in all parts of the query.
   */
  def getAllColumnIds: Set[String] = {
    val result = Sets.newHashSet[String]
    for (s <- selection; col <- s.getColumns.asScala) {
      result addAll col.getAllSimpleColumnIds
    }
    for (s <- sort; col <- s.getColumns.asScala) {
      result addAll col.getAllSimpleColumnIds
    }
    group forall { g => result addAll g.getSimpleColumnIds }
    pivot forall { p => result addAll p.getSimpleColumnIds }
    filter forall { f => result addAll f.getAllColumnIds }
    if (hasLabels) {
      for (col <- labels.getColumns.asScala) {
        result addAll col.getAllSimpleColumnIds
      }
    }
    if (hasUserFormatOptions) {
      for (col <- userFormatOptions.getColumns.asScala) {
        result addAll col.getAllSimpleColumnIds
      }
    }
    result
  }

  /**
   * Returns all aggregation columns used in all parts of this query.
   *
   * @return All aggregation columns used in all parts of this query.
   */
  def getAllAggregations: Set[AggregationColumn] = {
    var result = Sets.newHashSet[AggregationColumn]
    selection forall { s => result addAll s.getAggregationColumns }
    for (s <- sort; col <- s.getColumns.asScala) {
      if (col.isInstanceOf[AggregationColumn]) {
        result add col.asInstanceOf[AggregationColumn]
      }
    }
    if (hasLabels) {
      for (col <- labels.getColumns.asScala) {
        if (col.isInstanceOf[AggregationColumn]) {
          result add col.asInstanceOf[AggregationColumn]
        }
      }
    }
    if (hasUserFormatOptions) {
      for (col <- userFormatOptions.getColumns.asScala) {
        if (col.isInstanceOf[AggregationColumn]) {
          result add col.asInstanceOf[AggregationColumn]
        }
      }
    }
    result
  }

  /**
   * Returns all scalar function columns in this query.
   *
   * @return All scalar function columns in this query.
   */
  def getAllScalarFunctionsColumns: Set[ScalarFunctionColumn] = {
    val mentionedScalarFunctionColumns = Sets.newHashSet[ScalarFunctionColumn]
    selection foreach { s => mentionedScalarFunctionColumns addAll s.getScalarFunctionColumns }
    filter foreach { f => mentionedScalarFunctionColumns addAll f.getScalarFunctionColumns }
    group foreach { g => mentionedScalarFunctionColumns addAll g.getScalarFunctionColumns }
    pivot foreach { p => mentionedScalarFunctionColumns addAll p.getScalarFunctionColumns }
    sort foreach { s => mentionedScalarFunctionColumns addAll s.getScalarFunctionColumns }
    if (hasLabels) {
      mentionedScalarFunctionColumns addAll labels.getScalarFunctionColumns
    }
    if (hasUserFormatOptions) {
      mentionedScalarFunctionColumns addAll userFormatOptions.getScalarFunctionColumns
    }
    mentionedScalarFunctionColumns
  }

  /**
   * Checks that the given column is valid, i.e., is in the given list of
   * columns, or is a scalar function column and all its inner columns are
   * valid (i.e., in the list).
   *
   * @param columns The given list of columns.
   * @param column  The given column.
   * @param messageToLogAndUser The error message for the exception that is
   *     thrown when the column is not in the list.
   *
   * @throws InvalidQueryException Thrown when the column is invalid.
   */
  private def checkColumnInList(columns: List[_ <: AbstractColumn], column: AbstractColumn, message: String): Unit = {
    if (columns.contains(column)) { return }
    else if (column.isInstanceOf[ScalarFunctionColumn]) {
      val innerColumns = (column.asInstanceOf[ScalarFunctionColumn]).getColumns
      for (innerColumn <- innerColumns.asScala) {
        checkColumnInList(columns, innerColumn, message)
      }
    } else {
      log error message
      throw new InvalidQueryException(message)
    }
  }

  /**
   * Checks the selection of the given column is valid, given the group
   * columns. A column is valid in this sense if it is either grouped-by, or is
   * a scalar function column whose arguments are all valid columns.
   *
   * @param groupColumns The group columns.
   * @param col The selected column.
   *
   * @throws InvalidQueryException Thrown when the given column is a simple
   *     column that is not grouped by.
   */
  private def checkSelectedColumnWithGrouping(groupColumns: List[AbstractColumn], col: AbstractColumn): Unit = {
    if (col.isInstanceOf[SimpleColumn]) {
      if (!groupColumns.contains(col)) {
        val message = MessagesEnum.ADD_COL_TO_GROUP_BY_OR_AGG.getMessageWithArgs(localeForUserMessages, col.getId)
        log error message
        throw new InvalidQueryException(message)
      }
    } else if (col.isInstanceOf[ScalarFunctionColumn]) {
      if (!groupColumns.contains(col)) {
        val innerColumns = (col.asInstanceOf[ScalarFunctionColumn]).getColumns
        for (innerColumn <- innerColumns.asScala) {
          checkSelectedColumnWithGrouping(groupColumns, innerColumn)
        }
      }
    }
  }

  override def hashCode: Int = {
    val prime: Int = 37
    var result: Int = 1
    result = prime * result + (if ((filter == null)) 0 else filter.hashCode)
    result = prime * result + (if ((group == null)) 0 else group.hashCode)
    result = prime * result + (if ((labels == null)) 0 else labels.hashCode)
    result = prime * result + (if ((options == null)) 0 else options.hashCode)
    result = prime * result + (if ((pivot == null)) 0 else pivot.hashCode)
    result = prime * result + rowSkipping
    result = prime * result + rowLimit
    result = prime * result + rowOffset
    result = prime * result + (if ((selection == null)) 0 else selection.hashCode)
    result = prime * result + (if ((sort == null)) 0 else sort.hashCode)
    result = prime * result + (if ((userFormatOptions == null)) 0 else userFormatOptions.hashCode)
    return result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) {
      return true
    }
    if (obj == null) {
      return false
    }
    if (getClass != obj.asInstanceOf[AnyRef].getClass) {
      return false
    }
    var other: Query = obj.asInstanceOf[Query]
    if (filter == null) {
      if (other.filter != null) {
        return false
      }
    }
    else if (!filter.equals(other.filter)) {
      return false
    }
    if (group == null) {
      if (other.group != null) {
        return false
      }
    }
    else if (!group.equals(other.group)) {
      return false
    }
    if (labels == null) {
      if (other.labels != null) {
        return false
      }
    }
    else if (!labels.equals(other.labels)) {
      return false
    }
    if (options == null) {
      if (other.options != null) {
        return false
      }
    }
    else if (!options.equals(other.options)) {
      return false
    }
    if (pivot == null) {
      if (other.pivot != null) {
        return false
      }
    }
    else if (!pivot.equals(other.pivot)) {
      return false
    }
    if (rowSkipping != other.rowSkipping) {
      return false
    }
    if (rowLimit != other.rowLimit) {
      return false
    }
    if (rowOffset != other.rowOffset) {
      return false
    }
    if (selection == null) {
      if (other.selection != null) {
        return false
      }
    }
    else if (!selection.equals(other.selection)) {
      return false
    }
    if (sort == null) {
      if (other.sort != null) {
        return false
      }
    }
    else if (!sort.equals(other.sort)) {
      return false
    }
    if (userFormatOptions == null) {
      if (other.userFormatOptions != null) {
        return false
      }
    }
    else if (!userFormatOptions.equals(other.userFormatOptions)) {
      return false
    }
    return true
  }

  /**
   * Returns a string that when fed to the query parser will yield an identical Query.
   * Used mainly for debugging purposes.
   *
   * @return The query string.
   */
  def toQueryString: String = {
    var clauses: List[String] = Lists.newArrayList[String]
    selection foreach { s => clauses.add("SELECT " + s.toQueryString) }
    filter foreach { f => clauses.add("WHERE " + f.toQueryString) }
    group foreach { g => clauses.add("GROUP BY " + g.toQueryString) }
    pivot foreach { p => clauses.add("PIVOT " + p.toQueryString) }
    sort foreach { s => clauses.add("ORDER BY " + s.toQueryString) }

    if (hasRowSkipping) {
      clauses.add("SKIPPING " + rowSkipping)
    }
    if (hasRowLimit) {
      clauses.add("LIMIT " + rowLimit)
    }
    if (hasRowOffset) {
      clauses.add("OFFSET " + rowOffset)
    }
    if (hasLabels) {
      clauses.add("LABEL " + labels.toQueryString)
    }
    if (hasUserFormatOptions) {
      clauses.add("FORMAT " + userFormatOptions.toQueryString)
    }
    if (hasOptions) {
      clauses.add("OPTIONS " + options.toQueryString)
    }
    val result = new StrBuilder
    result.appendWithSeparators(clauses, " ")
    result.toString
  }

}

