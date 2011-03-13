package com.google.visualization.datasource.query.scalarfunction;

/**
 * Created by IntelliJ IDEA.
 * User: WrighKe
 * Date: 12/03/11
 * Time: 21:39
 * To change this template use File | Settings | File Templates.
 */
public enum TimeComponentExtractorComponent {
    YEAR("year"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour"),
    MINUTE("minute"),
    SECOND("second"),
    MILLISECOND("millisecond"),
    QUARTER("quarter"),
    // Returns 1 for Sunday, 2 for Monday, etc.
    DAY_OF_WEEK("dayofweek");

    private final String name;
    private TimeComponentExtractorComponent(String name) { this.name = name; }
    public String getName() { return name; }
}
