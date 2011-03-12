package com.google.visualization.datasource.query;

/**
 * Created by IntelliJ IDEA.
 * User: WrighKe
 * Date: 11/03/11
 * Time: 09:57
 * To change this template use File | Settings | File Templates.
 */
public enum ComparisonFilterOperator {
    EQ("=", true),
    NE("!=", true),
    LT("<", true),
    GT(">", true),
    LE("<=", true),
    GE(">=", true),
    CONTAINS("CONTAINS", false),
    STARTS_WITH("STARTS WITH", false),
    ENDS_WITH("ENDS WITH", false),
    // Note that the operator syntax is: val MATCHES regexp.
    MATCHES("MATCHES", false),
    LIKE("LIKE", false);

    /**
     * Initializes a new instance of this class, with the given boolean
     * saying whether or not this instance requires the types to be equal.
     *
     * @param requiresEqualTypes True if this instance requires the types
     *     to be equal, false otherwise.
     * @param queryStringForm The query string form of this operator.
     */
    ComparisonFilterOperator(String queryStringForm, boolean requiresEqualTypes) {
      this.queryStringForm = queryStringForm;
      this.requiresEqualTypes = requiresEqualTypes;
    }

    /**
     * Returns whether or not this instance requires the two types to be equal.
     *
     * @return Whether or not this instance requires the two types to be equal.
     */
    public boolean areEqualTypesRequired() {
      return requiresEqualTypes;
    }

    /**
     * Returns the query string form of this operator.
     *
     * @return The query string form of this operator.
     */
    public String toQueryString() {
      return queryStringForm;
    }

    /**
     * True if this instance requires the two types to be equal.
     */
    private boolean requiresEqualTypes;

    /**
     * The query string form of this operator, i.e., how this operator should
     * appear in a query string, to be parsed by the query parser.
     */
    private String queryStringForm;

}
