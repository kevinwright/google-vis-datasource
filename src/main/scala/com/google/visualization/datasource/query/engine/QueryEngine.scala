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

import com.google.common.collect.{Lists, Maps, Sets}
import base.{ReasonType, TypeMismatchException, Warning}
import datatable.{ColumnDescription, DataTable, TableCell, TableRow, ValueFormatter}
import datatable.value.Value
import query._
import com.ibm.icu.util.ULocale
import java.{util => ju}

//import java.util.ArrayList
//import java.util.Collections
//import java.util.List
//import java.util.Map
//import java.util.Set
//import java.util.SortedSet
//import java.util.TreeMap
//import java.util.TreeSet
import ju.concurrent.atomic.AtomicReference

import collection.{SortedSet, SortedMap}
import collection.JavaConverters._

/**
 * A collection of static methods that perform the operations involved in executing a query,
 * i.e., selection, sorting, paging (limit and offset), grouping and pivoting, filtering, skipping,
 * applying labels, and custom formatting. This also takes care of calculated columns.
 * This also takes care of scalar function columns.
 *
 * @author Yoah B.D.
 * @author Yonatan B.Y.
 * @author Liron L.
 */
object QueryEngine {
  /**
   * Returns the data that is the result of executing the query. The query is validated against the
   * data table before execution and an InvalidQueryException is thrown if it is invalid.
   * This function may change the given DataTable.
   *
   * @param query The query.
   * @param table The table to execute the query on.
   *
   * @return The data that is the result of executing the query.
   */
  def executeQuery(query: Query, table: DataTable, locale: ULocale): DataTable = {
    val columnIndices = new ColumnIndices
    val columnsDescription = table.getColumnDescriptions
    for((col,idx) <- columnsDescription.asScala.zipWithIndex) {
      columnIndices.put(new SimpleColumn(col.getId), idx)
    }

    try {
      val filtered = performFilter(table, query)
      val (grouped,columnLookups) = performGroupingAndPivoting(filtered, query, columnIndices)
      val sorted = performSort(grouped, query, locale)
      val skipped = performSkipping(sorted, query)
      val paginated = performPagination(skipped, query)
      
      val columnIndicesReference = new AtomicReference[ColumnIndices](columnIndices)
      val selected = performSelection(paginated, query, columnIndicesReference, columnLookups)
      val newColumnIndices = columnIndicesReference.get
      
      val labelled = performLabels(selected, query, newColumnIndices)
      performFormatting(labelled, query, newColumnIndices, locale)
    } catch { case e: TypeMismatchException => table}
  }


  /**
   * Creates the columns structure for a new table after pivoting and grouping.
   *
   * @param groupByColumnIds The column ids to group by. This is used for the
   *     ColumnDescriptions of the first columns in the table.
   * @param columnTitles The ColumnTitles of the aggregation columns in the table, i.e., columns
   *     that are composed of pivot values and aggregations.
   * @param original The original table, from which to get the original ColumnDescriptions.
   * @param scalarFunctionColumnTitles The scalar function column titles. i.e.,
   *     columns that are composed of pivot values and scalar function column.
   *
   * @return The new TableDescription.
   */
  private def createDataTable(
    groupByColumnIds: Seq[String],
    columnTitles: SortedSet[ColumnTitle],
    original: DataTable,
    scalarFunctionColumnTitles: Seq[ScalarFunctionColumnTitle]
  ): DataTable = {
    val result = new DataTable
    for (groupById <- groupByColumnIds) {
      result addColumn (original getColumnDescription groupById)
    }
    for (colTitle <- columnTitles) {
      result addColumn (colTitle createColumnDescription original)
    }
    for (scalarFunctionColumnTitle <- scalarFunctionColumnTitles) {
      result addColumn (scalarFunctionColumnTitle createColumnDescription original)
    }
    result
  }

  /**
   * Returns a table consisted of a subset of rows of the input table. 
   * We select the first out of every k rows in the table according to the 
   * skipping value in the query.
   * If there is no need to do anything, returns the original table.
   *
   * @param table The original table.
   * @param query The query.
   *
   * @return The skipped table, or the original if no skipping is needed. 
   */
  private def performSkipping(table: DataTable, query: Query): DataTable = {
	  
    val skip = query.getRowSkipping
    if (skip <= 1) {
      return table
    }
    val relevantRows = (0 to table.getNumberOfRows by skip) map { table.getRows get _ }

    val newTable = new DataTable
    newTable addColumns table.getColumnDescriptions
    newTable addRows relevantRows.asJavaCollection
    newTable
  }

  /**
   * Returns a paginated table, based on the row limit and offset parameters.
   * If there is no need to do anything, returns the original table.
   *
   * @param table The original table.
   * @param query The query.
   *
   * @return The paginated table, or the original if no pagination is needed.
   */
  private def performPagination(table: DataTable, query: Query): DataTable = {
    val rowOffset = query.getRowOffset
    val rowLimit = query.getRowLimit
    if ((rowLimit == -1 || table.getRows.size <= rowLimit) && rowOffset == 0) {
      return table
    }
    val numRows = table.getNumberOfRows
    val fromIndex = 0 max rowOffset
    val toIndex = if (rowLimit == -1) numRows else (numRows min (rowOffset + rowLimit))
    val relevantRows = table.getRows.subList(fromIndex, toIndex)

    val newTable = new DataTable
    newTable addColumns table.getColumnDescriptions
    newTable addRows relevantRows
    if (toIndex < numRows) {
      val warning = new Warning(ReasonType.DATA_TRUNCATED, "Data has been truncated due to user" + "request (LIMIT in query)")
      newTable addWarning warning
    }
    newTable
  }

  /**
   * Returns a table sorted according to the query's sort.
   * The returned table has the same rows as the original table.
   *
   * @param table The table to sort.
   * @param query The query.
   *
   * @return The sorted table.
   */
  private def performSort(table: DataTable, query: Query, locale: ULocale): DataTable = {
    if (!query.hasSort) table
    else {
      val sortBy = query.getSort
      val columnLookup = new DataTableColumnLookup(table)
      val comparator = new TableRowComparator(sortBy, locale, columnLookup)
      val newRows = table.getRows.asScala.toSeq.sorted(comparator)
      table setRows newRows.asJavaCollection
      table
    }
  }

  /**
   * Returns a table that has only the rows from the given table that match the filter
   * provided by a query.
   *
   * @param table The table to filter.
   * @param query The query.
   *
   * @return The filtered table.
   */
  private def performFilter(table: DataTable, query: Query): DataTable = {
    if (!query.hasFilter) table
    else {
      val queryFilter = query.getFilter
      val newRows = table.getRows.asScala filter {queryFilter.isMatch(table, _)}
      table setRows newRows.asJavaCollection
      table
    }
  }

  /**
   * Returns a table that has only the columns from the given table that are specified by
   * the query.
   *
   * @param table The table from which to select.
   * @param query The query.
   * @param columnIndicesReference A reference to a ColumnIndices instance, so that
   *     this function can change the internal ColumnIndices.
   * @param columnLookups A map of column lookups by their list of pivot values.
   *
   * @return The table with selected columns only.
   */
  private def performSelection(
    table: DataTable,
    query: Query,
    columnIndicesReference: AtomicReference[ColumnIndices],
    columnLookups: Map[Seq[Value], ColumnLookup]
  ): DataTable = {
    if (!query.hasSelection) {
      return table
    }
    var columnIndices = columnIndicesReference.get
    val selectedColumns = query.getSelection.getColumns.asScala
    val oldColumnDescriptions = table.getColumnDescriptions
    var newColumnDescriptions = Seq.empty[ColumnDescription]
    val newColumnIndices = new ColumnIndices

    var currIndex = Iterator from 0
    for (col <- selectedColumns) {
      val colIndices = columnIndices.getColumnIndices(col).asScala
      if (colIndices.size == 0) {
        newColumnDescriptions :+=
          new ColumnDescription(
            col.getId,
            col.getValueType(table),
            ScalarFunctionColumnTitle.getColumnDescriptionLabel(table, col))
        newColumnIndices.put(col, currIndex.next)
      } else {
        for (colIndex <- colIndices) {
          newColumnDescriptions :+= (oldColumnDescriptions get colIndex)
          newColumnIndices.put(col, currIndex.next)
        }
      }
    }
    columnIndices = newColumnIndices
    columnIndicesReference set columnIndices
    
    val result = new DataTable
    result addColumns newColumnDescriptions.asJavaCollection
    for (sourceRow <- table.getRows.asScala) {
      val newRow = new TableRow
      for (col <- selectedColumns) {
        var wasFound: Boolean = false
        val pivotValuesSet = columnLookups.keySet
        for (values <- pivotValuesSet) {
          if (columnLookups(values).containsColumn(col) && ((col.getAllAggregationColumns.size != 0) || !wasFound)) {
            wasFound = true
            newRow.addCell(sourceRow.getCell(columnLookups(values).getColumnIndex(col)))
          }
        }
        if (!wasFound) {
          val lookup = new DataTableColumnLookup(table)
          newRow addCell col.getCell(lookup, sourceRow)
        }
      }
      result addRow newRow
    }
    result
  }

  /**
   * Returns true if the query has aggregation columns and the table is not
   * empty.
   *
   * @param query The given query.
   *
   * @return true if the query has aggregation columns and the table is not
   *     empty.
   */
  private def queryHasAggregation(query: Query): Boolean =
    query.hasSelection && !query.getSelection.getAggregationColumns.isEmpty

  /**
   * Returns the result of performing the grouping (and pivoting) operations
   * on the given table, using the information provided in the query's group
   * and pivot.
   *
   * The new table generated has columns as follows where A is the number of group-by columns,
   * B is the number of combinations of values of pivot-by columns, and X is the number of 
   * aggregations requested:
   * - Columns 1..A are the original group-by columns, in the order they are
   * given in the group-by list.
   * - Columns (A+1)..B are pivot and aggregation columns, where each
   * column's id is composed of values of the pivot-by columns in the
   * original table, and an aggregation column, with separators between them.
   *
   * Note that the aggregations requested can be all on the same
   * aggregation column or on different aggregation columns. To this
   * mechanism, it doesn't matter.
   *
   * There is a row for each combination of the values of the group-by columns.
   *
   * The value in the cell at row X and column Y is the result of the requested
   * aggregation type described in Y on the set of values in the aggregation
   * column (also described in Y) for which the values of the group-by columns
   * are as determined by X and the values of the pivot-by columns are as
   * determined by the column.
   *
   * @param table The original table.
   * @param query The query.
   * @param columnIndices A map, in which this method sets the indices
   *     of the new columns, if grouping is performed, and then any
   *     previous values in it are cleared. If grouping is not performed, it is
   *     left as is.
   *
   * @return The new table, after grouping and pivoting was performed.
   */
  private def performGroupingAndPivoting(
    table: DataTable,
    query: Query,
    columnIndices: ColumnIndices
  ): (DataTable, Map[Seq[Value], ColumnLookup]) = {
    if (!queryHasAggregation(query) || (table.getNumberOfRows == 0)) {
      return (table, Map.empty)
    }
    val group = query.getGroup
    val pivot = query.getPivot
    val selection = query.getSelection
    
    val groupByIds =
      if (group != null) group.getColumnIds.asScala.toSeq
      else Nil

    val pivotByIds =
      if (pivot != null) pivot.getColumnIds.asScala.toSeq
      else Nil

    val groupAndPivotIds = groupByIds ++: pivotByIds

    val selectedScalarFunctionColumns = selection.getScalarFunctionColumns

    val columnAggregations = selection.getAggregationColumns.asScala.distinct

    val aggregationIds =
      columnAggregations map {_.getAggregatedColumn.getId}

    val groupAndPivotScalarFunctionColumns =
      (Option(group) map (_.scalarFunctionColumns) getOrElse Nil) ++:
      (Option(pivot) map (_.scalarFunctionColumns) getOrElse Nil)

    val newColumnDescriptions = table.getColumnDescriptions.asScala ++
      (groupAndPivotScalarFunctionColumns map {col =>
        new ColumnDescription(
          col.getId,
          col.getValueType(table),
          ScalarFunctionColumnTitle.getColumnDescriptionLabel(table, col)
        )
      })

    val newTable = new DataTable
    newTable addColumns newColumnDescriptions.asJavaCollection

    val lookup = new DataTableColumnLookup(table)
    for (sourceRow <- table.getRows.asScala) {
      val newRow = new TableRow

      sourceRow.getCells.asScala foreach { newRow addCell _ }

      groupAndPivotScalarFunctionColumns map { col =>
        new TableCell(col.getValue(lookup, sourceRow))
      } foreach { newRow addCell _ }

      try { newTable addRow newRow } catch { case e: TypeMismatchException => }
    }

    val aggregator = new TableAggregator(groupAndPivotIds.asJava, aggregationIds.toSet.asJava, newTable)
    var paths = aggregator.getPathsToLeaves
    
    var rowTitles: SortedSet[RowTitle] = SortedSet.empty(GroupingComparators.ROW_TITLE_COMPARATOR)
    var columnTitles: SortedSet[ColumnTitle] = SortedSet.empty(GroupingComparators getColumnTitleDynamicComparator columnAggregations.toBuffer.asJava)
    var pivotValuesSet: SortedSet[Seq[Value]] = SortedSet.empty(GroupingComparators.ValueSeqOrdering)
    
    var metaTable = new MetaTable
    for (columnAggregation <- columnAggregations; path <- paths.asScala) {
      val originalValues = path.getValues.asScala.toSeq
      val (rowValues, columnValues) = originalValues splitAt groupByIds.size
      
      val rowTitle = new RowTitle(rowValues)
      rowTitles += rowTitle
      
      pivotValuesSet += columnValues
      
      var columnTitle: ColumnTitle = new ColumnTitle(columnValues.toBuffer.asJava, columnAggregation, (columnAggregations.size > 1))
      columnTitles += columnTitle
      
      metaTable.put(rowTitle, columnTitle, new TableCell(aggregator.getAggregationValue(path, columnAggregation.getAggregatedColumn.getId, columnAggregation.getAggregationType)))
    }
    
    val scalarFunctionColumnTitles = for {
    	col <- selectedScalarFunctionColumns.asScala if col.getAllAggregationColumns.size != 0
        columnValues <- pivotValuesSet
    } yield { new ScalarFunctionColumnTitle(columnValues.toBuffer.asJava, col) }

    val result = createDataTable(groupByIds, columnTitles, newTable, scalarFunctionColumnTitles)
    val colDescs = result.getColumnDescriptions
    columnIndices.clear
    
    val columnIndex = Iterator from 0
    var columnLookups = SortedMap.empty[Seq[Value], ColumnLookup](GroupingComparators.ValueSeqOrdering)

    if (group != null) {
      columnLookups += (Nil -> new GenericColumnLookup)
      for (column <- group.getColumns.asScala) {
        columnIndices.put(column, columnIndex.next)
        if (!(column.isInstanceOf[ScalarFunctionColumn])) {
          (columnLookups.get(Nil).asInstanceOf[GenericColumnLookup]).put(column, columnIndex.next)
          for (vals <- pivotValuesSet) {
            if (!columnLookups.contains(vals)) {
              columnLookups += (vals -> new GenericColumnLookup)
            }
            (columnLookups(vals).asInstanceOf[GenericColumnLookup]).put(column, columnIndex.next)
          }
        }
      }
    }
    
    for (title <- columnTitles) {
      columnIndices.put(title.getAggregation, columnIndex.next)
      var values = title.values
      if (!columnLookups.contains(values)) {
        columnLookups += (values -> new GenericColumnLookup)
      }
      (columnLookups.get(values).asInstanceOf[GenericColumnLookup]).put(title.getAggregation, columnIndex.next)
    }
    
    for (rowTitle <- rowTitles) {
      var curRow: TableRow = new TableRow
      for (v <- rowTitle.values) {
        curRow.addCell(new TableCell(v))
      }
      var rowData: Map[ColumnTitle, TableCell] = metaTable.getRow(rowTitle).asScala.toMap
      for ((colTitle, i) <- columnTitles.zipWithIndex) {
        var cell: TableCell = rowData(colTitle)
        curRow.addCell(if ((cell != null)) cell else new TableCell(Value.getNullValueFromValueType(colDescs.get(i + rowTitle.values.size).getType)))
      }
      for (columnTitle <- scalarFunctionColumnTitles) {
        curRow.addCell(new TableCell(columnTitle.column.getValue(columnLookups(columnTitle.getValues.asScala), curRow)))
      }
      result.addRow(curRow)
    }
    
    for (title <- scalarFunctionColumnTitles) {
      val idx = columnIndex.next
      columnIndices.put(title.column, idx)
      val values = title.getValues.asScala
      if (!columnLookups.contains(values)) {
        columnLookups += (values -> new GenericColumnLookup)
      }
      (columnLookups.get(values).asInstanceOf[GenericColumnLookup]).put(title.column, idx)
    }
    (result, columnLookups)
  }

  /**
   * Apply labels to columns as specified in the user query.
   * If a column is specified in the query, but is not part of the data table,
   * this is still a valid situation, and the "invalid" column id is ignored.
   *
   * @param table The original table.
   * @param query The query.
   * @param columnIndices The map of columns to indices in the table.
   *
   * @return The table with labels applied.
   */
  private def performLabels(table: DataTable, query: Query, columnIndices: ColumnIndices): DataTable = {
    if (!query.hasLabels) {
      return table
    }
    var labels: QueryLabels = query.getLabels
    var columnDescriptions = table.getColumnDescriptions
    for (column <- labels.getColumns.asScala) {
      var label: String = labels.getLabel(column)
      var indices = columnIndices.getColumnIndices(column).asScala
      if (indices.size == 1) {
        columnDescriptions.get(indices(0)).setLabel(label)
      } else {
        var columnId: String = column.getId
        for (i <- indices) {
          var colDesc: ColumnDescription = columnDescriptions.get(i)
          var colDescId: String = colDesc.getId
          var specificLabel: String = colDescId.substring(0, colDescId.length - columnId.length) + label
          columnDescriptions.get(i).setLabel(specificLabel)
        }
      }
    }
    return table
  }

  /**
   * Add column formatters according to a given patterns list. Namely,
   * a visualization gadget can send a map of patterns by column ids. The following
   * method builds the appropriate formatters for these patterns.
   * An illegal pattern is recorded for later sending of a warning.
   *
   * @param table The original table.
   * @param query The query.
   * @param columnIndices The map of columns to indices in the table.
   * @param locale The locale by which to format.
   *
   * @return The table with formatting applied.
   */
  private def performFormatting(table: DataTable, query: Query, columnIndices: ColumnIndices, locale: ULocale): DataTable = {
    if (!query.hasUserFormatOptions) {
      return table
    }
    val queryFormat = query.getUserFormatOptions
    val columnDescriptions = table.getColumnDescriptions.asScala
    var indexToFormatter = Map.empty[Int, ValueFormatter]
    for (col <- queryFormat.getColumns.asScala) {
      val pattern = queryFormat.getPattern(col)
      val indices = columnIndices.getColumnIndices(col).asScala
      var allSucceeded: Boolean = true
      for ((colDesc, i) <- columnDescriptions.zipWithIndex) {
        val f = ValueFormatter.createFromPattern(colDesc.getType, pattern, locale)
        if (f == null) {
          allSucceeded = false
        } else {
          indexToFormatter += (i -> f)
          table.getColumnDescription(i).setPattern(pattern)
        }
      }
      if (!allSucceeded) {
        var warning: Warning = new Warning(ReasonType.ILLEGAL_FORMATTING_PATTERNS, "Illegal formatting pattern: " + pattern + " requested on column: " + col.getId)
        table.addWarning(warning)
      }
    }
    for (row <- table.getRows.asScala) {
      for (col <- indexToFormatter.keys) {
        var cell: TableCell = row.getCell(col)
        var value: Value = cell.getValue
        var formatter: ValueFormatter = indexToFormatter(col)
        var formattedValue: String = formatter.format(value)
        cell.setFormattedValue(formattedValue)
      }
    }
    table
  }
}

