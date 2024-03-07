// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules;

import org.apache.doris.nereids.pattern.PatternMatcher;
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * Type of rules, each rule has its unique type.
 */
public enum RuleType {
    // just for UT
    TEST_REWRITE(RuleTypeClass.REWRITE),
    // binding rules

    // **** make sure BINDING_UNBOUND_LOGICAL_PLAN is the lowest priority in the rewrite rules. ****
    BINDING_RESULT_SINK(RuleTypeClass.REWRITE),
    BINDING_INSERT_TARGET_TABLE(RuleTypeClass.REWRITE),
    BINDING_INSERT_FILE(RuleTypeClass.REWRITE),
    BINDING_ONE_ROW_RELATION_SLOT(RuleTypeClass.REWRITE),
    BINDING_RELATION(RuleTypeClass.REWRITE),
    BINDING_PROJECT_SLOT(RuleTypeClass.REWRITE),
    BINDING_USING_JOIN_SLOT(RuleTypeClass.REWRITE),
    BINDING_JOIN_SLOT(RuleTypeClass.REWRITE),
    BINDING_FILTER_SLOT(RuleTypeClass.REWRITE),
    BINDING_AGGREGATE_SLOT(RuleTypeClass.REWRITE),
    BINDING_REPEAT_SLOT(RuleTypeClass.REWRITE),
    BINDING_HAVING_SLOT(RuleTypeClass.REWRITE),
    BINDING_SORT_SLOT(RuleTypeClass.REWRITE),
    BINDING_SORT_SET_OPERATION_SLOT(RuleTypeClass.REWRITE),
    BINDING_LIMIT_SLOT(RuleTypeClass.REWRITE),
    BINDING_GENERATE_SLOT(RuleTypeClass.REWRITE),
    BINDING_SUBQUERY_ALIAS_SLOT(RuleTypeClass.REWRITE),
    BINDING_UNBOUND_TVF_RELATION_FUNCTION(RuleTypeClass.REWRITE),
    BINDING_SET_OPERATION_SLOT(RuleTypeClass.REWRITE),
    BINDING_INLINE_TABLE_SLOT(RuleTypeClass.REWRITE),

    BINDING_SLOT_WITH_PATHS_SCAN(RuleTypeClass.REWRITE),
    COUNT_LITERAL_REWRITE(RuleTypeClass.REWRITE),

    REPLACE_SORT_EXPRESSION_BY_CHILD_OUTPUT(RuleTypeClass.REWRITE),

    FILL_UP_HAVING_AGGREGATE(RuleTypeClass.REWRITE),
    FILL_UP_HAVING_PROJECT(RuleTypeClass.REWRITE),
    FILL_UP_SORT_AGGREGATE(RuleTypeClass.REWRITE),
    FILL_UP_SORT_HAVING_PROJECT(RuleTypeClass.REWRITE),
    FILL_UP_SORT_HAVING_AGGREGATE(RuleTypeClass.REWRITE),
    FILL_UP_SORT_PROJECT(RuleTypeClass.REWRITE),

    RESOLVE_PROJECT_ALIAS(RuleTypeClass.REWRITE),
    RESOLVE_AGGREGATE_ALIAS(RuleTypeClass.REWRITE),
    PROJECT_TO_GLOBAL_AGGREGATE(RuleTypeClass.REWRITE),
    HAVING_TO_FILTER(RuleTypeClass.REWRITE),
    ONE_ROW_RELATION_EXTRACT_AGGREGATE(RuleTypeClass.REWRITE),
    PROJECT_WITH_DISTINCT_TO_AGGREGATE(RuleTypeClass.REWRITE),
    AVG_DISTINCT_TO_SUM_DIV_COUNT(RuleTypeClass.REWRITE),
    ANALYZE_CTE(RuleTypeClass.REWRITE),
    RELATION_AUTHENTICATION(RuleTypeClass.VALIDATION),

    ADJUST_NULLABLE_FOR_PROJECT_SLOT(RuleTypeClass.REWRITE),
    ADJUST_NULLABLE_FOR_AGGREGATE_SLOT(RuleTypeClass.REWRITE),
    ADJUST_NULLABLE_FOR_HAVING_SLOT(RuleTypeClass.REWRITE),
    ADJUST_NULLABLE_FOR_REPEAT_SLOT(RuleTypeClass.REWRITE),
    ADD_DEFAULT_LIMIT(RuleTypeClass.REWRITE),

    CHECK_ROW_POLICY(RuleTypeClass.REWRITE),
    CHECK_TYPE_TO_INSERT_TARGET_COLUMN(RuleTypeClass.REWRITE),

    RESOLVE_ORDINAL_IN_ORDER_BY(RuleTypeClass.REWRITE),
    RESOLVE_ORDINAL_IN_GROUP_BY(RuleTypeClass.REWRITE),

    // check analysis rule
    CHECK_AGGREGATE_ANALYSIS(RuleTypeClass.CHECK),
    CHECK_ANALYSIS(RuleTypeClass.CHECK),
    CHECK_OBJECT_TYPE_ANALYSIS(RuleTypeClass.CHECK),
    CHECK_DATA_TYPES(RuleTypeClass.CHECK),

    // rewrite rules
    NORMALIZE_AGGREGATE(RuleTypeClass.REWRITE),
    NORMALIZE_SORT(RuleTypeClass.REWRITE),
    NORMALIZE_REPEAT(RuleTypeClass.REWRITE),
    EXTRACT_AND_NORMALIZE_WINDOW_EXPRESSIONS(RuleTypeClass.REWRITE),
    CHECK_AND_STANDARDIZE_WINDOW_FUNCTION_AND_FRAME(RuleTypeClass.REWRITE),
    CHECK_MATCH_EXPRESSION(RuleTypeClass.REWRITE),
    CREATE_PARTITION_TOPN_FOR_WINDOW(RuleTypeClass.REWRITE),
    AGGREGATE_DISASSEMBLE(RuleTypeClass.REWRITE),
    SIMPLIFY_AGG_GROUP_BY(RuleTypeClass.REWRITE),
    DISTINCT_AGGREGATE_DISASSEMBLE(RuleTypeClass.REWRITE),
    LOGICAL_SUB_QUERY_ALIAS_TO_LOGICAL_PROJECT(RuleTypeClass.REWRITE),
    COLLECT_SUB_QUERY_ALIAS(RuleTypeClass.REWRITE),
    ELIMINATE_GROUP_BY_CONSTANT(RuleTypeClass.REWRITE),

    ELIMINATE_LOGICAL_SELECT_HINT(RuleTypeClass.REWRITE),
    ELIMINATE_ORDER_BY_CONSTANT(RuleTypeClass.REWRITE),
    ELIMINATE_ORDER_BY_UNDER_SUBQUERY(RuleTypeClass.REWRITE),
    ELIMINATE_ORDER_BY_UNDER_VIEW(RuleTypeClass.REWRITE),
    ELIMINATE_HINT(RuleTypeClass.REWRITE),
    ELIMINATE_JOIN_ON_EMPTYRELATION(RuleTypeClass.REWRITE),
    ELIMINATE_FILTER_ON_EMPTYRELATION(RuleTypeClass.REWRITE),
    ELIMINATE_AGG_ON_EMPTYRELATION(RuleTypeClass.REWRITE),
    ELIMINATE_UNION_ON_EMPTYRELATION(RuleTypeClass.REWRITE),
    ELIMINATE_INTERSECTION_ON_EMPTYRELATION(RuleTypeClass.REWRITE),
    ELIMINATE_EXCEPT_ON_EMPTYRELATION(RuleTypeClass.REWRITE),
    INFER_PREDICATES(RuleTypeClass.REWRITE),
    INFER_AGG_NOT_NULL(RuleTypeClass.REWRITE),
    INFER_SET_OPERATOR_DISTINCT(RuleTypeClass.REWRITE),
    INFER_FILTER_NOT_NULL(RuleTypeClass.REWRITE),
    INFER_JOIN_NOT_NULL(RuleTypeClass.REWRITE),
    // subquery analyze
    FILTER_SUBQUERY_TO_APPLY(RuleTypeClass.REWRITE),
    PROJECT_SUBQUERY_TO_APPLY(RuleTypeClass.REWRITE),
    JOIN_SUBQUERY_TO_APPLY(RuleTypeClass.REWRITE),
    ONE_ROW_RELATION_SUBQUERY_TO_APPLY(RuleTypeClass.REWRITE),
    // subquery rewrite rule
    ELIMINATE_LIMIT_UNDER_APPLY(RuleTypeClass.REWRITE),
    ELIMINATE_SORT_UNDER_APPLY(RuleTypeClass.REWRITE),
    ELIMINATE_SORT_UNDER_APPLY_PROJECT(RuleTypeClass.REWRITE),
    PULL_UP_PROJECT_UNDER_APPLY(RuleTypeClass.REWRITE),
    PULL_UP_PROJECT_UNDER_LIMIT(RuleTypeClass.REWRITE),
    PULL_UP_PROJECT_UNDER_TOPN(RuleTypeClass.REWRITE),
    AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION(RuleTypeClass.REWRITE),
    UN_CORRELATED_APPLY_FILTER(RuleTypeClass.REWRITE),
    UN_CORRELATED_APPLY_PROJECT_FILTER(RuleTypeClass.REWRITE),
    UN_CORRELATED_APPLY_AGGREGATE_FILTER(RuleTypeClass.REWRITE),
    PULL_UP_CORRELATED_FILTER_UNDER_APPLY_AGGREGATE_PROJECT(RuleTypeClass.REWRITE),
    SCALAR_APPLY_TO_JOIN(RuleTypeClass.REWRITE),
    IN_APPLY_TO_JOIN(RuleTypeClass.REWRITE),
    EXISTS_APPLY_TO_JOIN(RuleTypeClass.REWRITE),
    // predicate push down rules
    PUSH_DOWN_JOIN_OTHER_CONDITION(RuleTypeClass.REWRITE),
    PUSH_DOWN_PREDICATE_THROUGH_AGGREGATION(RuleTypeClass.REWRITE),
    PUSH_DOWN_PREDICATE_THROUGH_REPEAT(RuleTypeClass.REWRITE),
    PUSH_DOWN_EXPRESSIONS_IN_HASH_CONDITIONS(RuleTypeClass.REWRITE),
    // Pushdown filter
    PUSH_DOWN_FILTER_THROUGH_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_LEFT_SEMI_JOIN(RuleTypeClass.REWRITE),
    PUSH_FILTER_INSIDE_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_PROJECT(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_PROJECT_UNDER_LIMIT(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_WINDOW(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_PARTITION_TOPN(RuleTypeClass.REWRITE),
    PUSH_DOWN_PROJECT_THROUGH_LIMIT(RuleTypeClass.REWRITE),
    PUSH_DOWN_ALIAS_THROUGH_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_ALIAS_INTO_UNION_ALL(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_SET_OPERATION(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_SORT(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_GENERATE(RuleTypeClass.REWRITE),

    PUSH_DOWN_FILTER_THROUGH_CTE(RuleTypeClass.REWRITE),
    PUSH_DOWN_FILTER_THROUGH_CTE_ANCHOR(RuleTypeClass.REWRITE),

    PUSH_DOWN_DISTINCT_THROUGH_JOIN(RuleTypeClass.REWRITE),

    COLUMN_PRUNING(RuleTypeClass.REWRITE),
    ELIMINATE_SORT(RuleTypeClass.REWRITE),

    PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE(RuleTypeClass.REWRITE),
    PUSH_DOWN_AGG_THROUGH_JOIN(RuleTypeClass.REWRITE),

    TRANSPOSE_LOGICAL_SEMI_JOIN_LOGICAL_JOIN(RuleTypeClass.REWRITE),
    TRANSPOSE_LOGICAL_SEMI_JOIN_LOGICAL_JOIN_PROJECT(RuleTypeClass.REWRITE),
    LOGICAL_SEMI_JOIN_COMMUTE(RuleTypeClass.REWRITE),
    TRANSPOSE_LOGICAL_SEMI_JOIN_AGG(RuleTypeClass.REWRITE),
    TRANSPOSE_LOGICAL_SEMI_JOIN_AGG_PROJECT(RuleTypeClass.REWRITE),

    // expression of plan rewrite
    REWRITE_ONE_ROW_RELATION_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_PROJECT_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_AGG_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_FILTER_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_JOIN_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_GENERATE_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_SORT_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_HAVING_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_REPEAT_EXPRESSION(RuleTypeClass.REWRITE),
    EXTRACT_FILTER_FROM_JOIN(RuleTypeClass.REWRITE),
    REORDER_JOIN(RuleTypeClass.REWRITE),
    // Merge Consecutive plan
    MERGE_PROJECTS(RuleTypeClass.REWRITE),
    MERGE_FILTERS(RuleTypeClass.REWRITE),
    MERGE_LIMITS(RuleTypeClass.REWRITE),
    MERGE_GENERATES(RuleTypeClass.REWRITE),
    // Eliminate plan
    ELIMINATE_AGGREGATE(RuleTypeClass.REWRITE),
    ELIMINATE_LIMIT(RuleTypeClass.REWRITE),
    ELIMINATE_LIMIT_ON_ONE_ROW_RELATION(RuleTypeClass.REWRITE),
    ELIMINATE_LIMIT_ON_EMPTY_RELATION(RuleTypeClass.REWRITE),
    ELIMINATE_FILTER(RuleTypeClass.REWRITE),
    ELIMINATE_JOIN(RuleTypeClass.REWRITE),
    ELIMINATE_JOIN_BY_FOREIGN_KEY(RuleTypeClass.REWRITE),
    ELIMINATE_JOIN_CONDITION(RuleTypeClass.REWRITE),
    ELIMINATE_FILTER_ON_ONE_RELATION(RuleTypeClass.REWRITE),
    ELIMINATE_SEMI_JOIN(RuleTypeClass.REWRITE),
    ELIMINATE_NOT_NULL(RuleTypeClass.REWRITE),
    ELIMINATE_UNNECESSARY_PROJECT(RuleTypeClass.REWRITE),
    ELIMINATE_OUTER_JOIN(RuleTypeClass.REWRITE),
    ELIMINATE_MARK_JOIN(RuleTypeClass.REWRITE),
    ELIMINATE_GROUP_BY(RuleTypeClass.REWRITE),
    ELIMINATE_JOIN_BY_UK(RuleTypeClass.REWRITE),
    ELIMINATE_JOIN_BY_FK(RuleTypeClass.REWRITE),
    ELIMINATE_GROUP_BY_KEY(RuleTypeClass.REWRITE),
    ELIMINATE_DEDUP_JOIN_CONDITION(RuleTypeClass.REWRITE),
    ELIMINATE_NULL_AWARE_LEFT_ANTI_JOIN(RuleTypeClass.REWRITE),
    ELIMINATE_ASSERT_NUM_ROWS(RuleTypeClass.REWRITE),
    CONVERT_OUTER_JOIN_TO_ANTI(RuleTypeClass.REWRITE),
    FIND_HASH_CONDITION_FOR_JOIN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_FILTER_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_PROJECT_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_PROJECT_FILTER_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_FILTER_PROJECT_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_REPEAT_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_REPEAT_FILTER_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_REPEAT_PROJECT_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_REPEAT_PROJECT_FILTER_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_AGG_REPEAT_FILTER_PROJECT_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_FILTER_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_PROJECT_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_PROJECT_FILTER_SCAN(RuleTypeClass.REWRITE),
    MATERIALIZED_INDEX_FILTER_PROJECT_SCAN(RuleTypeClass.REWRITE),

    OLAP_SCAN_PARTITION_PRUNE(RuleTypeClass.REWRITE),
    FILE_SCAN_PARTITION_PRUNE(RuleTypeClass.REWRITE),
    PUSH_CONJUNCTS_INTO_JDBC_SCAN(RuleTypeClass.REWRITE),
    PUSH_CONJUNCTS_INTO_ODBC_SCAN(RuleTypeClass.REWRITE),
    PUSH_CONJUNCTS_INTO_ES_SCAN(RuleTypeClass.REWRITE),
    OLAP_SCAN_TABLET_PRUNE(RuleTypeClass.REWRITE),
    PUSH_AGGREGATE_TO_OLAP_SCAN(RuleTypeClass.REWRITE),
    EXTRACT_SINGLE_TABLE_EXPRESSION_FROM_DISJUNCTION(RuleTypeClass.REWRITE),
    HIDE_ONE_ROW_RELATION_UNDER_UNION(RuleTypeClass.REWRITE),
    PUSH_PROJECT_THROUGH_UNION(RuleTypeClass.REWRITE),
    MERGE_ONE_ROW_RELATION_INTO_UNION(RuleTypeClass.REWRITE),
    PUSH_PROJECT_INTO_ONE_ROW_RELATION(RuleTypeClass.REWRITE),
    PUSH_PROJECT_INTO_UNION(RuleTypeClass.REWRITE),
    MERGE_SET_OPERATION(RuleTypeClass.REWRITE),
    MERGE_SET_OPERATION_EXCEPT(RuleTypeClass.REWRITE),
    MERGE_TOP_N(RuleTypeClass.REWRITE),
    BUILD_AGG_FOR_UNION(RuleTypeClass.REWRITE),
    COUNT_DISTINCT_REWRITE(RuleTypeClass.REWRITE),
    INNER_TO_CROSS_JOIN(RuleTypeClass.REWRITE),
    CROSS_TO_INNER_JOIN(RuleTypeClass.REWRITE),
    PRUNE_EMPTY_PARTITION(RuleTypeClass.REWRITE),

    // split limit
    SPLIT_LIMIT(RuleTypeClass.REWRITE),
    PULL_UP_JOIN_FROM_UNIONALL(RuleTypeClass.REWRITE),
    // limit push down
    PUSH_LIMIT_THROUGH_JOIN(RuleTypeClass.REWRITE),
    PUSH_LIMIT_THROUGH_PROJECT_JOIN(RuleTypeClass.REWRITE),
    PUSH_LIMIT_THROUGH_PROJECT_WINDOW(RuleTypeClass.REWRITE),
    PUSH_LIMIT_THROUGH_UNION(RuleTypeClass.REWRITE),
    PUSH_LIMIT_THROUGH_WINDOW(RuleTypeClass.REWRITE),
    LIMIT_SORT_TO_TOP_N(RuleTypeClass.REWRITE),
    // topN push down
    PUSH_DOWN_TOP_N_THROUGH_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_TOP_N_THROUGH_PROJECT_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_TOP_N_DISTINCT_THROUGH_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_TOP_N_DISTINCT_THROUGH_PROJECT_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_TOP_N_THROUGH_PROJECT_WINDOW(RuleTypeClass.REWRITE),
    PUSH_DOWN_TOP_N_THROUGH_WINDOW(RuleTypeClass.REWRITE),
    PUSH_DOWN_TOP_N_THROUGH_UNION(RuleTypeClass.REWRITE),
    PUSH_DOWN_TOP_N_DISTINCT_THROUGH_UNION(RuleTypeClass.REWRITE),
    PUSH_DOWN_LIMIT_DISTINCT_THROUGH_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_LIMIT_DISTINCT_THROUGH_PROJECT_JOIN(RuleTypeClass.REWRITE),
    PUSH_DOWN_LIMIT_DISTINCT_THROUGH_UNION(RuleTypeClass.REWRITE),
    // adjust nullable
    ADJUST_NULLABLE(RuleTypeClass.REWRITE),
    ADJUST_CONJUNCTS_RETURN_TYPE(RuleTypeClass.REWRITE),
    // ensure having project on the top join
    ENSURE_PROJECT_ON_TOP_JOIN(RuleTypeClass.REWRITE),

    PULL_UP_CTE_ANCHOR(RuleTypeClass.REWRITE),
    CTE_INLINE(RuleTypeClass.REWRITE),
    REWRITE_CTE_CHILDREN(RuleTypeClass.REWRITE),
    COLLECT_FILTER_ABOVE_CTE_CONSUMER(RuleTypeClass.REWRITE),
    INLINE_VIEW(RuleTypeClass.REWRITE),
    CHECK_PRIVILEGES(RuleTypeClass.REWRITE),

    COLLECT_FILTER(RuleTypeClass.REWRITE),
    COLLECT_JOIN_CONSTRAINT(RuleTypeClass.REWRITE),
    COLLECT_PROJECT_ABOVE_CTE_CONSUMER(RuleTypeClass.REWRITE),
    COLLECT_PROJECT_ABOVE_FILTER_CTE_CONSUMER(RuleTypeClass.REWRITE),

    LEADING_JOIN(RuleTypeClass.REWRITE),
    REWRITE_SENTINEL(RuleTypeClass.REWRITE),

    // topn opts
    DEFER_MATERIALIZE_TOP_N_RESULT(RuleTypeClass.REWRITE),

    // exploration rules
    TEST_EXPLORATION(RuleTypeClass.EXPLORATION),
    OR_EXPANSION(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_COMMUTE(RuleTypeClass.EXPLORATION),
    LOGICAL_INNER_JOIN_LASSCOM(RuleTypeClass.EXPLORATION),
    LOGICAL_INNER_JOIN_LASSCOM_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_OUTER_JOIN_LASSCOM(RuleTypeClass.EXPLORATION),
    LOGICAL_OUTER_JOIN_LASSCOM_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_OUTER_JOIN_ASSOC(RuleTypeClass.EXPLORATION),
    LOGICAL_OUTER_JOIN_ASSOC_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_LEFT(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_RIGHT(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_LEFT_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_RIGHT_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_SEMI_JOIN_SEMI_JOIN_TRANSPOSE(RuleTypeClass.EXPLORATION),
    LOGICAL_SEMI_JOIN_SEMI_JOIN_TRANSPOSE_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_EXCHANGE(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_EXCHANGE_LEFT_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_EXCHANGE_RIGHT_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_EXCHANGE_BOTH_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_INNER_JOIN_LEFT_ASSOCIATIVE(RuleTypeClass.EXPLORATION),
    LOGICAL_INNER_JOIN_LEFT_ASSOCIATIVE_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_INNER_JOIN_RIGHT_ASSOCIATIVE(RuleTypeClass.EXPLORATION),
    LOGICAL_INNER_JOIN_RIGHT_ASSOCIATIVE_PROJECT(RuleTypeClass.EXPLORATION),
    TRANSPOSE_LOGICAL_AGG_SEMI_JOIN(RuleTypeClass.EXPLORATION),
    TRANSPOSE_LOGICAL_AGG_SEMI_JOIN_PROJECT(RuleTypeClass.EXPLORATION),
    TRANSPOSE_LOGICAL_JOIN_UNION(RuleTypeClass.EXPLORATION),
    PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN_LEFT(RuleTypeClass.EXPLORATION),
    PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN_RIGHT(RuleTypeClass.EXPLORATION),
    PUSH_DOWN_PROJECT_THROUGH_INNER_OUTER_JOIN_LEFT(RuleTypeClass.EXPLORATION),
    PUSH_DOWN_PROJECT_THROUGH_INNER_OUTER_JOIN_RIGHT(RuleTypeClass.EXPLORATION),
    EAGER_COUNT(RuleTypeClass.EXPLORATION),
    EAGER_GROUP_BY(RuleTypeClass.EXPLORATION),
    EAGER_GROUP_BY_COUNT(RuleTypeClass.EXPLORATION),
    EAGER_SPLIT(RuleTypeClass.EXPLORATION),

    EXPLORATION_SENTINEL(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_PROJECT_JOIN(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_FILTER_JOIN(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_PROJECT_FILTER_JOIN(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_FILTER_PROJECT_JOIN(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_ONLY_JOIN(RuleTypeClass.EXPLORATION),

    MATERIALIZED_VIEW_PROJECT_AGGREGATE(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_FILTER_AGGREGATE(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_PROJECT_FILTER_AGGREGATE(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_FILTER_PROJECT_AGGREGATE(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_ONLY_AGGREGATE(RuleTypeClass.EXPLORATION),

    MATERIALIZED_VIEW_FILTER_SCAN(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_PROJECT_SCAN(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_FILTER_PROJECT_SCAN(RuleTypeClass.EXPLORATION),
    MATERIALIZED_VIEW_PROJECT_FILTER_SCAN(RuleTypeClass.EXPLORATION),

    // implementation rules
    LOGICAL_ONE_ROW_RELATION_TO_PHYSICAL_ONE_ROW_RELATION(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_TVF_RELATION_TO_PHYSICAL_TVF_RELATION(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_AGG_TO_PHYSICAL_HASH_AGG_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_JOIN_TO_HASH_JOIN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_JOIN_TO_NESTED_LOOP_JOIN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_PROJECT_TO_PHYSICAL_PROJECT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_FILTER_TO_PHYSICAL_FILTER_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_CTE_PRODUCER_TO_PHYSICAL_CTE_PRODUCER_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_CTE_CONSUMER_TO_PHYSICAL_CTE_CONSUMER_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_CTE_ANCHOR_TO_PHYSICAL_CTE_ANCHOR_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_REPEAT_TO_PHYSICAL_REPEAT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_SORT_TO_PHYSICAL_QUICK_SORT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_TOP_N_TO_PHYSICAL_TOP_N_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_DEFER_MATERIALIZE_TOP_N_TO_PHYSICAL_DEFER_MATERIALIZE_TOP_N_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_PARTITION_TOP_N_TO_PHYSICAL_PARTITION_TOP_N_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_EMPTY_RELATION_TO_PHYSICAL_EMPTY_RELATION_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_LIMIT_TO_PHYSICAL_LIMIT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_OLAP_SCAN_TO_PHYSICAL_OLAP_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_DEFER_MATERIALIZE_OLAP_SCAN_TO_PHYSICAL_DEFER_MATERIALIZE_OLAP_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_SCHEMA_SCAN_TO_PHYSICAL_SCHEMA_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_FILE_SCAN_TO_PHYSICAL_FILE_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_JDBC_SCAN_TO_PHYSICAL_JDBC_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_ODBC_SCAN_TO_PHYSICAL_ODBC_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_ES_SCAN_TO_PHYSICAL_ES_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_OLAP_TABLE_SINK_TO_PHYSICAL_OLAP_TABLE_SINK_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_RESULT_SINK_TO_PHYSICAL_RESULT_SINK_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_DEFER_MATERIALIZE_RESULT_SINK_TO_PHYSICAL_DEFER_MATERIALIZE_RESULT_SINK_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_FILE_SINK_TO_PHYSICAL_FILE_SINK_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_ASSERT_NUM_ROWS_TO_PHYSICAL_ASSERT_NUM_ROWS(RuleTypeClass.IMPLEMENTATION),
    STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT(RuleTypeClass.IMPLEMENTATION),
    STORAGE_LAYER_AGGREGATE_WITH_PROJECT(RuleTypeClass.IMPLEMENTATION),
    STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT_FOR_FILE_SCAN(RuleTypeClass.IMPLEMENTATION),
    STORAGE_LAYER_AGGREGATE_WITH_PROJECT_FOR_FILE_SCAN(RuleTypeClass.IMPLEMENTATION),
    STORAGE_LAYER_AGGREGATE_MINMAX_ON_UNIQUE(RuleTypeClass.IMPLEMENTATION),
    STORAGE_LAYER_AGGREGATE_MINMAX_ON_UNIQUE_WITHOUT_PROJECT(RuleTypeClass.IMPLEMENTATION),
    COUNT_ON_INDEX(RuleTypeClass.IMPLEMENTATION),
    COUNT_ON_INDEX_WITHOUT_PROJECT(RuleTypeClass.IMPLEMENTATION),
    ONE_PHASE_AGGREGATE_WITHOUT_DISTINCT(RuleTypeClass.IMPLEMENTATION),
    TWO_PHASE_AGGREGATE_WITHOUT_DISTINCT(RuleTypeClass.IMPLEMENTATION),
    TWO_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI(RuleTypeClass.IMPLEMENTATION),
    THREE_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI(RuleTypeClass.IMPLEMENTATION),
    TWO_PHASE_AGGREGATE_WITH_DISTINCT(RuleTypeClass.IMPLEMENTATION),
    ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI(RuleTypeClass.IMPLEMENTATION),
    TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI(RuleTypeClass.IMPLEMENTATION),
    TWO_PHASE_AGGREGATE_WITH_MULTI_DISTINCT(RuleTypeClass.IMPLEMENTATION),
    THREE_PHASE_AGGREGATE_WITH_DISTINCT(RuleTypeClass.IMPLEMENTATION),
    FOUR_PHASE_AGGREGATE_WITH_DISTINCT(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_UNION_TO_PHYSICAL_UNION(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_EXCEPT_TO_PHYSICAL_EXCEPT(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_INTERSECT_TO_PHYSICAL_INTERSECT(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_GENERATE_TO_PHYSICAL_GENERATE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_WINDOW_TO_PHYSICAL_WINDOW_RULE(RuleTypeClass.IMPLEMENTATION),
    IMPLEMENTATION_SENTINEL(RuleTypeClass.IMPLEMENTATION),

    // sentinel, use to count rules
    SENTINEL(RuleTypeClass.SENTINEL),
    ;

    private final RuleTypeClass ruleTypeClass;

    RuleType(RuleTypeClass ruleTypeClass) {
        this.ruleTypeClass = ruleTypeClass;
    }

    public int type() {
        return ordinal();
    }

    public RuleTypeClass getRuleTypeClass() {
        return ruleTypeClass;
    }

    public <INPUT_TYPE extends Plan, OUTPUT_TYPE extends Plan> Rule build(
            PatternMatcher<INPUT_TYPE, OUTPUT_TYPE> patternMatcher) {
        return patternMatcher.toRule(this);
    }

    enum RuleTypeClass {
        REWRITE,
        EXPLORATION,
        // This type is used for unit test only.
        CHECK,
        IMPLEMENTATION,
        VALIDATION,
        SENTINEL,
    }
}
