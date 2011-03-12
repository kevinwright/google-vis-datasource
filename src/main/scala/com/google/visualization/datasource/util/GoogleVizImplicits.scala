package com.google.visualization.datasource
package util


import query._

object GoogleVizImplicits {
  implicit def pimpQuery(q:Query) = new QueryOps(q)

  class QueryOps(q:Query) {
    import q._

    private def mkOpt[T](cond:Boolean, value: => T) = if(cond) Some(value) else None

    def sortOpt = mkOpt(hasSort, getSort)

    /**
     * The required selection.
     * If the selection is null, or is empty, the original
     * selection that was defined in the report is used.
     */
    def selectionOpt = mkOpt(hasSelection, getSelection)

    def filterOpt = mkOpt(hasFilter, getFilter)
    def groupOpt = mkOpt(hasGroup, getGroup)
    def pivotOpt = mkOpt(hasPivot, getPivot)
    def optionsOpt = mkOpt(hasOptions, getOptions)
    def labelsOpt = mkOpt(hasLabels, getLabels)
    def userFormatOptionsOpt = mkOpt(hasUserFormatOptions, getUserFormatOptions)
    def rowLimitOpt = mkOpt(hasRowLimit, getRowLimit)
    def rowOffsetOpt = mkOpt(hasRowOffset, getRowOffset)
  }

}
