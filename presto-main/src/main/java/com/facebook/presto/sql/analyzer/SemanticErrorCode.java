/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.analyzer;

public enum SemanticErrorCode
{
    MUST_BE_AGGREGATE_OR_GROUP_BY,
    NESTED_AGGREGATION,
    NESTED_WINDOW,
    MUST_BE_WINDOW_FUNCTION,
    WINDOW_REQUIRES_OVER,
    INVALID_WINDOW_FRAME,

    MISSING_CATALOG,
    MISSING_SCHEMA,
    MISSING_TABLE,
    MISSING_COLUMN,
    MISMATCHED_COLUMN_ALIASES,
    NOT_SUPPORTED,

    INVALID_SCHEMA_NAME,

    TABLE_ALREADY_EXISTS,
    COLUMN_ALREADY_EXISTS,

    DUPLICATE_RELATION,

    TYPE_MISMATCH,
    AMBIGUOUS_ATTRIBUTE,
    MISSING_ATTRIBUTE,
    INVALID_ORDINAL,
    INVALID_LITERAL,

    ORDER_BY_MUST_BE_IN_SELECT,

    CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS,

    WILDCARD_WITHOUT_FROM,

    MISMATCHED_SET_COLUMN_TYPES,

    MULTIPLE_FIELDS_FROM_SCALAR_SUBQUERY,

    DUPLICATE_COLUMN_NAME,
    COLUMN_NAME_NOT_SPECIFIED,

    EXPRESSION_NOT_CONSTANT,

    VIEW_PARSE_ERROR,
    VIEW_ANALYSIS_ERROR,
    VIEW_IS_STALE,

    NON_NUMERIC_SAMPLE_PERCENTAGE,

    SAMPLE_PERCENTAGE_OUT_OF_RANGE,

    INVALID_SESSION_PROPERTY
}
