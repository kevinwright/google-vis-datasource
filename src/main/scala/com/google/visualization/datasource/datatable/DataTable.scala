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
package datatable

import com.google.common.collect.{ImmutableList, Lists, Maps, Ordering, Sets}
import base.{TypeMismatchException, Warning}
import value.{Value, ValueType}
import com.ibm.icu.util.ULocale

import java.{util => ju, lang => jl}
import ju.Collection
import ju.Collections
import ju.Comparator
import ju.Iterator
import ju.List
import ju.Map
import ju.Set

import collection.JavaConverters._

/**
 * A table of data, arranged in typed columns.
 *
 * An instance of this class is the result of a request to a data source. A <code>DataTable</code>
 * can be rendered in many ways: JSON, HTML, CSV (see
 * {@link com.google.visualization.datasource.render}), and can be manipulated using queries (see
 * {@link com.google.visualization.datasource.query.Query} and
 * {@link com.google.visualization.datasource.query.engine.QueryEngine}).
 *
 * A table contains any number of typed columns (see (@link ColumnDescription}) each with an id and
 * a label, and any number of rows. Each row must have as many cells as there are columns
 * in the table, and the types of values in the cells must match the types of the columns.
 * Each cell contains, as well as the typed value, a formatted value of type string, used for
 * display purposes.
 * Also, you can use the custom properties mechanism to hold any other data you require. There are
 * custom properties on every cell, row, column, and on the entire table.
 *
 * @author Yoah B.D.
 */
object DataTable {

  /**
   * Returns a data table with str as the content of its single cell.
   *
   * @param str The cell's content
   * @return A data table with str as the content of its single cell.
   */
  def createSingleCellTable(str: String): DataTable = {
    var dataTable: DataTable = new DataTable
    var colDesc: ColumnDescription = new ColumnDescription("SingleCellTable", ValueType.TEXT, "")
    dataTable.addColumn(colDesc)
    var row: TableRow = new TableRow
    row.addCell(new TableCell(str))
    try {
      dataTable.addRow(row)
    }
    catch {
      case e: TypeMismatchException => {
      }
    }
    return dataTable
  }
}

class DataTable extends CustomProperties {
  /**
   * Column descriptions.
   */
  private var columns: ju.List[ColumnDescription] = Lists.newArrayList[ColumnDescription]
  /**
   * Map from a column to its index in the columns list.
   */
  private var columnIndexById: ju.Map[String, Int] = Maps.newHashMap[String, Int]
  /**
   * The list of returned rows.
   */
  private var rows: ju.List[TableRow] = Lists.newArrayList[TableRow]
  /**
   * A list of warnings.
   */
  private var warnings: ju.List[Warning] = Lists.newArrayList[Warning]
  /**
   * The user locale, used to create localized messages.
   */
  private var localeForUserMessages = null: ULocale


  /**
   * Adds a single row to the end of the result. Throws a TypeMismatchException if the row's cells
   * do not match the current columns. If the row is too short, i.e., has too few cells, then the
   * remaining columns are filled with null values (the given row is changed).
   *
   * @param row The row of values.
   *
   * @throws TypeMismatchException Thrown if the values in the cells do not match the columns.
   */
  @throws(classOf[TypeMismatchException])
  def addRow(row: TableRow): Unit = {
    val cells = row.getCells
    if (cells.size > columns.size) {
      throw new TypeMismatchException("Row has too many cells. Should be at most of size: " + columns.size)
    }
    for (i <- 0 until cells.size) {
      if (cells.get(i).getType != columns.get(i).getType) {
        throw new TypeMismatchException("Cell type does not match column type, at index: " + i + ". Should be of type: " + columns.get(i).getType.toString)
      }
    }
    for (i <- cells.size until columns.size) {
      row.addCell(new TableCell(Value.getNullValueFromValueType(columns.get(i).getType)))
    }
    rows.add(row)
  }

  /**
   * A convenience method for creating a row directly from its cell values and
   * adding it to the data table.
   *
   * @param values The row values.
   * @throws TypeMismatchException Thrown if a value does not match its
   * corresponding column.
   */
  def addRowFromValues(values: Array[AnyRef]): Unit = {
    val columnIt = columns.listIterator
    val row = new TableRow

    var i = 0
    while (i < values.length && columnIt.hasNext) {
      val colDesc = columnIt.next
      row.addCell(colDesc.getType createValue values(i))
      i += 1
    }
    addRow(row)
  }

  /**
   * Adds a collection of rows to the end of the result.
   *
   * @param rowsToAdd The row collection.
   */
  def addRows(rowsToAdd: Collection[TableRow]): Unit = {
    for (row <- rowsToAdd.asScala) {
      addRow(row)
    }
  }

  /**
   * Sets a collection of rows after clearing any current rows.
   *
   * @param rows The row collection.
   */
  def setRows(rows: Collection[TableRow]): Unit = {
    this.rows.clear
    addRows(rows)
  }

  def getRows: List[TableRow] = rows
  def getRow(rowIndex: Int): TableRow = rows.get(rowIndex)
  def getNumberOfRows: Int = rows.size
  def getNumberOfColumns: Int = columns.size

  def getColumnDescriptions: List[ColumnDescription] =
    ImmutableList.copyOf(columns: jl.Iterable[ColumnDescription])

  def getColumnDescription(colIndex: Int): ColumnDescription =
    columns.get(colIndex)

  def getColumnDescription(columnId: String): ColumnDescription =
    columns.get(getColumnIndex(columnId))

  /**
   * Returns the list of all cells of a certain column, by the column index.
   * Note: This is the most naive implementation, that for each request
   * to this method just creates a new List of the needed cells.
   *
   * @param columnIndex The index of the requested column.
   *
   * @return The list of all cells of the requested column.
   */
  def getColumnCells(columnIndex: Int): List[TableCell] = {
    val colCells = Lists.newArrayListWithCapacity[TableCell](getNumberOfRows)
    for (row <- getRows.asScala) {
      colCells add (row getCell columnIndex)
    }
    colCells
  }

  /**
   * Add a column to the table.
   *
   * @param columnDescription The column's description.
   */
  def addColumn(columnDescription: ColumnDescription): Unit = {
    val columnId = columnDescription.getId
    if (columnIndexById containsKey columnId) {
      throw new RuntimeException("Column Id [" + columnId + "] already in table description")
    }
    columnIndexById.put(columnId, columns.size)
    columns add columnDescription
    for (row <- rows.asScala) {
      row addCell (new TableCell(Value getNullValueFromValueType columnDescription.getType))
    }
  }

  /**
   * Adds columns to the table.
   *
   * @param columnsToAdd The columns to add.
   */
  def addColumns(columnsToAdd: Collection[ColumnDescription]): Unit = {
    for (column <- columnsToAdd.asScala) {
      addColumn(column)
    }
  }

  def getColumnIndex(columnId: String): Int =
    columnIndexById.get(columnId)

  def getColumnCells(columnId: String): List[TableCell] =
    getColumnCells(getColumnIndex(columnId))

  def getCell(rowIndex: Int, colIndex: Int): TableCell =
    getRow(rowIndex) getCell colIndex

  def getValue(rowIndex: Int, colIndex: Int): Value =
    getCell(rowIndex, colIndex).getValue

  def getWarnings: List[Warning] = {
    return ImmutableList.copyOf(warnings: jl.Iterable[Warning])
  }

  /**
   * Returns a sorted list of distinct table cells in the specified column.
   * The cells are sorted according to the given comparator in ascending order.
   *
   * @param columnIndex The index of the required column.
   * @param comparator A Comparator for TableCells.
   *
   * @return A sorted list of distinct table cells in the specified column.
   */
  def getColumnDistinctCellsSorted(columnIndex: Int, comparator: Comparator[TableCell]): List[TableCell] = {
    val colCells = Sets.newTreeSet(comparator)
    for (cell <- getColumnCells(columnIndex).asScala) {
      colCells add cell
    }
    Ordering from comparator sortedCopy colCells
  }

  /**
   * Returns an ordered list of all the distinct values of a single column.
   *
   * @param columnIndex The index of the requested column.
   *
   * @return An ordered list of all the distinct values of a single
   *     column.
   */
  private[datatable] def getColumnDistinctValues(columnIndex: Int): List[Value] = {
    val values = Sets.newTreeSet[Value]
    for (row <- getRows.asScala) {
      values add (row getCell columnIndex getValue)
    }
    Lists newArrayList values
  }

  def addWarning(warning: Warning): Unit = warnings.add(warning)

  /**
   * Returns an ordered list of all the distinct values of a single column.
   *
   * @param columnId The id of the requested column.
   *
   * @return An ordered list of all the distinct values of a single
   * column.
   */
  private[datatable] def getColumnDistinctValues(columnId: String): List[Value] =
    getColumnDistinctValues(getColumnIndex(columnId))

  def containsColumn(columnId: String): Boolean =
    columnIndexById containsKey columnId

  def containsAllColumnIds(colIds: Collection[String]): Boolean =
    !(colIds.asScala exists {!containsColumn(_)} )

  /**
   * Returns a new data table, with the same data and metadata as this one.
   * Any change to the returned table should not change this table and vice
   * versa. This is a deep clone.
   *
   * @return The cloned data table.
   */
  override def clone: DataTable = {
    var result = new DataTable
    for (column <- columns.asScala) { result addColumn column.clone }
    try {
      for (row <- rows.asScala) { result addRow row.clone }
    } catch { case e: TypeMismatchException => }

    result.customProperties = customPropsClone
    result.warnings = Lists.newArrayList[Warning]
    for (warning <- warnings.asScala) { result.warnings add warning }
    result setLocaleForUserMessages localeForUserMessages
    result
  }

  /**
   * Returns a string representation of the data table.
   * Useful mainly for debugging.
   *
   * @return A string representation of the data table.
   */
  override def toString =
    rows.asScala map {_.getCells.asScala mkString ","} mkString "\n"

  def getLocaleForUserMessages: ULocale = localeForUserMessages

  def setLocaleForUserMessages(localeForUserMessges: ULocale): Unit =
    this.localeForUserMessages = localeForUserMessges


}

