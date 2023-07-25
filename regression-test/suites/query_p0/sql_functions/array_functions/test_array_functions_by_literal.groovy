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

suite("test_array_functions_by_literal") {
    // array_contains function
    qt_sql "select array_contains([1,2,3], 1)"
    qt_sql "select array_contains([1,2,3], 4)"
    qt_sql "select array_contains([1,2,3,NULL], 1)"
    qt_sql "select array_contains([1,2,3,NULL], NULL)"
    qt_sql "select array_contains([], 1)"
    qt_sql "select array_contains([], NULL)"
    qt_sql "select array_contains(NULL, 1)"
    qt_sql "select array_contains(NULL, NULL)"
    qt_sql "select array_contains([true], false)"
    qt_sql "select array_contains(array(cast ('2023-02-04 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))), cast ('2023-02-04 22:07:34.999' as datetimev2(3)))"
    qt_sql "select array_contains(array(cast ('2023-02-04' as datev2),cast ('2023-02-05' as datev2)), cast ('2023-02-05' as datev2))"
    qt_sql "select array_contains(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), cast (111.111 as decimalv3(6,3)))"

    // array_position function
    qt_sql "select array_position([1,2,3], 1)"
    qt_sql "select array_position([1,2,3], 3)"
    qt_sql "select array_position([1,2,3], 4)"
    qt_sql "select array_position([NULL,2,3], 2)"
    qt_sql "select array_position([NULL,2,3], NULL)"
    qt_sql "select array_position([], 1)"
    qt_sql "select array_position([], NULL)"
    qt_sql "select array_position(NULL, 1)"
    qt_sql "select array_position(NULL, NULL)"
    qt_sql "select array_position([null], 0)"
    qt_sql "select array_position([0], null)"
    qt_sql "select array_position([null, '1'], '')"
    qt_sql "select array_position([''], null)"
    qt_sql "select array_position([false, NULL, true], true)"
    qt_sql "select array_position(array(cast ('2023-02-04 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))), cast ('2023-02-04 22:07:34.999' as datetimev2(3)))"
    qt_sql "select array_position(array(cast ('2023-02-04' as datev2),cast ('2023-02-05' as datev2)), cast ('2023-02-05' as datev2))"
    qt_sql "select array_position(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), cast (111.111 as decimalv3(6,3)))"

    // element_at function
    qt_sql "select element_at([1,2,3], 1)"
    qt_sql "select element_at([1,2,3], 3)"
    qt_sql "select element_at([1,2,3], 4)"
    qt_sql "select element_at([1,2,3], -1)"
    qt_sql "select element_at([1,2,3], NULL)"
    qt_sql "select element_at([1,2,NULL], 3)"
    qt_sql "select element_at([1,2,NULL], 2)"
    qt_sql "select element_at([], -1)"
    qt_sql "select element_at([true, NULL, false], 2)"
    qt_sql "select element_at(array(cast ('2023-02-04 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))), 1)"
    qt_sql "select element_at(array(cast ('2023-02-04' as datev2),cast ('2023-02-05' as datev2)), 2)"
    qt_sql "select element_at(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), 1)"

    // array subscript function
    qt_sql "select [1,2,3][1]"
    qt_sql "select [1,2,3][3]"
    qt_sql "select [1,2,3][4]"
    qt_sql "select [1,2,3][-1]"
    qt_sql "select [1,2,3][NULL]"
    qt_sql "select [1,2,NULL][3]"
    qt_sql "select [1,2,NULL][2]"
    qt_sql "select [][-1]"
    qt_sql "select [true, false]"
    qt_sql "select (array(cast ('2023-02-04 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))[2]"
    qt_sql "select (array(cast ('2023-02-04' as datev2),cast ('2023-02-05' as datev2)))[2]"
    qt_sql "select (array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))[2]"

    // array_aggregation function
    qt_sql "select array_avg([1,2,3])"
    qt_sql "select array_sum([1,2,3])"
    qt_sql "select array_min([1,2,3])"
    qt_sql "select array_max([1,2,3])"
    qt_sql "select array_product([1,2,3])"
    qt_sql "select array_avg([1,2,3,null])"
    qt_sql "select array_sum([1,2,3,null])"
    qt_sql "select array_min([1,2,3,null])"
    qt_sql "select array_max([1,2,3,null])"
    qt_sql "select array_product([1,2,3,null])"
    qt_sql "select array_avg([])"
    qt_sql "select array_sum([])"
    qt_sql "select array_min([])"
    qt_sql "select array_max([])"
    qt_sql "select array_product([])"
    qt_sql "select array_avg([null])"
    qt_sql "select array_sum([null])"
    qt_sql "select array_min([null])"
    qt_sql "select array_max([null])"
    qt_sql "select array_product([null])"
    qt_sql "select array_product([1.12, 3.45, 4.23])"
    qt_sql "select array_product([1.12, 3.45, -4.23])"
    qt_sql "select array_min(array(cast ('2023-02-04 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select array_max(array(cast ('2023-02-04 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select array_min(array(cast ('2023-02-04' as datev2),cast ('2023-02-05' as datev2)))"
    qt_sql "select array_max(array(cast ('2023-02-04' as datev2),cast ('2023-02-05' as datev2)))"
    qt_sql "select array_avg(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"
    qt_sql "select array_sum(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"
    qt_sql "select array_min(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"
    qt_sql "select array_max(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"
    qt_sql "select array_product(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"

    // array_distinct function
    qt_sql "select array_distinct([1,1,2,2,3,3])"
    qt_sql "select array_distinct([1,1,2,2,3,3,null])"
    qt_sql "select array_distinct([1,1,3,3,null, null, null])"
    qt_sql "select array_distinct(['a','a','a'])"
    qt_sql "select array_distinct([null, 'a','a','a', null])"
    qt_sql "select array_distinct([true, false, false, null])"
    qt_sql "select array_distinct([])"
    qt_sql "select array_distinct([null,null])"
    qt_sql "select array_distinct([1, 0, 0, null])"
    qt_sql "select array_distinct(array(cast ('2023-02-04 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select array_distinct(array(cast ('2023-02-04' as datev2),cast ('2023-02-05' as datev2),cast ('2023-02-05' as datev2)))"
    qt_sql "select array_distinct(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"


    // array_remove function
    qt_sql "select array_remove([1,2,3], 1)"
    qt_sql "select array_remove([1,2,3,null], 1)"
    qt_sql "select array_remove(['a','b','c'], 'a')"
    qt_sql "select array_remove(['a','b','c',null], 'a')"
    qt_sql "select array_remove([true, false, false], false)"
    qt_sql "select array_remove(array(cast ('2023-02-04 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))), cast ('2023-02-04 22:07:34.999' as datetimev2(3)))"
    qt_sql "select array_remove(array(cast ('2023-02-04' as datev2),cast ('2023-02-05' as datev2)), cast ('2023-02-05' as datev2))"
    qt_sql "select array_remove(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), cast (111.111 as decimalv3(6,3)))"
 
    // array_sort function
    qt_sql_array_sort1 "select array_sort([1,2,3])"
    qt_sql_array_sort2 "select array_sort([3,2,1])"
    qt_sql_array_sort3 "select array_sort([1,2,3,null])"
    qt_sql_array_sort4 "select array_sort([null,1,2,3])"
    qt_sql_array_sort5 "select array_sort(['a','b','c'])"
    qt_sql_array_sort6 "select array_sort(['c','b','a'])"
    qt_sql_array_sort7 "select array_sort([true, false, true])"
    qt_sql_array_sort8 "select array_sort([])"
    qt_sql_array_sort9 "select array_sort(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql_array_sort10 "select array_sort(array(cast ('2023-02-06' as datev2),cast ('2023-02-05' as datev2)))"
    qt_sql_array_sort11 "select array_sort(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"

    // array_reverse_sort function
    qt_sql_array_reverse_sort1 "select array_reverse_sort([1,2,3])"
    qt_sql_array_reverse_sort2 "select array_reverse_sort([3,2,1])"
    qt_sql_array_reverse_sort3 "select array_reverse_sort([1,2,3,null])"
    qt_sql_array_reverse_sort4 "select array_reverse_sort([null,1,2,3])"
    qt_sql_array_reverse_sort5 "select array_reverse_sort(['a','b','c'])"
    qt_sql_array_reverse_sort6 "select array_reverse_sort(['c','b','a'])"
    qt_sql_array_reverse_sort7 "select array_reverse_sort([true, false, true])"
    qt_sql_array_reverse_sort8 "select array_reverse_sort([])"
    qt_sql_array_reverse_sort9 "select array_reverse_sort(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql_array_reverse_sort10 "select array_reverse_sort(array(cast ('2023-02-06' as datev2),cast ('2023-02-05' as datev2)))"
    qt_sql_array_reverse_sort11 "select array_reverse_sort(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"

    // array_overlap function
    qt_sql "select arrays_overlap([1,2,3], [4,5,6])"
    qt_sql "select arrays_overlap([1,2,3], [3,4,5])"
    qt_sql "select arrays_overlap([1,2,3,null], [3,4,5])"
    qt_sql "select arrays_overlap([true], [false])"
    qt_sql "select arrays_overlap([], [])"
    qt_sql "select arrays_overlap(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))),array(cast ('2023-02-07 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select arrays_overlap(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))),array(cast ('2023-02-07 22:07:34.999' as datetimev2(3)),cast ('2023-02-08 23:07:34.999' as datetimev2(3))))"
    qt_sql "select arrays_overlap(array(cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2)), array(cast ('2023-02-07' as datev2), cast ('2023-02-05' as datev2)))"
    qt_sql "select arrays_overlap(array(cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2)), array(cast ('2023-02-07' as datev2), cast ('2023-02-08' as datev2)))"
    qt_sql "select arrays_overlap(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), array(cast (222.222 as decimalv3(6,3)),cast (333.333 as decimalv3(6,3))))"

    // array_binary function
    qt_sql "select array_union([1,2,3], [2,3,4])"
    qt_sql "select array_except([1,2,3], [2,3,4])"
    qt_sql "select array_intersect([1,2,3], [2,3,4])"
    qt_sql "select array_union([1,2,3], [2,3,4,null])"
    qt_sql "select array_except([1,2,3], [2,3,4,null])"
    qt_sql "select array_intersect([1,2,3], [2,3,4,null])"
    qt_sql "select array_union([true], [false])"
    qt_sql "select array_except([true, false], [true])"
    qt_sql "select array_intersect([false, true], [false])"
    qt_sql "select array_union([], [])"
    qt_sql "select array_except([], [])"
    qt_sql "select array_intersect([], [])"
    qt_sql "select array_union([], [1,2,3])"
    qt_sql "select array_except([], [1,2,3])"
    qt_sql "select array_intersect([], [1,2,3])"
    qt_sql "select array_union([null], [1,2,3])"
    qt_sql "select array_except([null], [1,2,3])"
    qt_sql "select array_intersect([null], [1,2,3])"
    qt_sql "select array_union([1], [100000000])"
    qt_sql "select array_except([1], [100000000])"
    qt_sql "select array_intersect([1], [100000000])"
    qt_sql "select array_union(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))),array(cast ('2023-02-07 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select array_except(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))),array(cast ('2023-02-07 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select array_intersect(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))),array(cast ('2023-02-07 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select array_union(array(cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2)), array(cast ('2023-02-07' as datev2), cast ('2023-02-05' as datev2)))"
    qt_sql "select array_except(array(cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2)), array(cast ('2023-02-07' as datev2), cast ('2023-02-05' as datev2)))"
    qt_sql "select array_intersect(array(cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2)), array(cast ('2023-02-07' as datev2), cast ('2023-02-05' as datev2)))"
    qt_sql "select array_union(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), array(cast (222.222 as decimalv3(6,3)),cast (333.333 as decimalv3(6,3))))"
    qt_sql "select array_except(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), array(cast (222.222 as decimalv3(6,3)),cast (333.333 as decimalv3(6,3))))"
    qt_sql "select array_intersect(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), array(cast (222.222 as decimalv3(6,3)),cast (333.333 as decimalv3(6,3))))"

    // array_slice function
    qt_sql "select [1,2,3][1:1]"
    qt_sql "select [1,2,3][1:3]"
    qt_sql "select [1,2,3][1:5]"
    qt_sql "select [1,2,3][2:]"
    qt_sql "select [1,2,3][-2:]"
    qt_sql "select [1,2,3][2:-1]"
    qt_sql "select [1,2,3][0:]"
    qt_sql "select [1,2,3][-5:]"
    qt_sql "select [true, false, false][2:]"
    qt_sql "select (array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))[1:2]"
    qt_sql "select (array(cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2)))[1:2]"
    qt_sql "select (array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))[1:2]"

<<<<<<< HEAD
=======
    // array_intersect
    qt_sql_intersect_1 "select array_intersect([1,2,3], [1,2,3], [null])"
    qt_sql_intersect_2 "select array_intersect([1, 2, null], [1, 3, null], [1,2,3,null])"
    qt_sql_intersect_3 "select array_intersect([1,2,3, null], [1,2,3,null], [1,2,null], [1, null])"
    qt_sql_intersect_4 "select array_intersect([1,2,3], [1,2,3], [null], [])"

>>>>>>> 2.0.0-rc01
    // array_popfront function
    qt_sql "select array_popfront([1,2,3,4,5,6])"
    qt_sql "select array_popfront([])"
    qt_sql "select array_popfront(null)"
    qt_sql "select array_popfront([null,2,3,4,5])"
    qt_sql "select array_popfront([1,2,3,4,null])"
    qt_sql "select array_popfront(['1','2','3','4','5','6'])"
    qt_sql "select array_popfront([null,'2','3','4','5','6'])"
    qt_sql "select array_popfront(['1','2','3','4','5',null])"
    qt_sql "select array_popfront(array(cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2), cast ('2023-02-07' as datev2), cast ('2023-02-05' as datev2)))"
    qt_sql "select array_popfront(array(null, cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2), cast ('2023-02-07' as datev2), cast ('2023-02-05' as datev2)))"
    qt_sql "select array_popfront(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3)), cast ('2023-02-07 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select array_popfront(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"

    // array_join function 
    qt_sql "select array_join([1, 2, 3], '_')"
    qt_sql "select array_join(['1', '2', '3', null], '_')"
    qt_sql "select array_join([null, '1', '2', '3', null], '_')"
    qt_sql "select array_join(['', '2', '3'], '_')"
    qt_sql "select array_join(['1', '2', ''], '_')"
    qt_sql "select array_join(['1', '2', '', null], '_')"
    qt_sql "select array_join(['', '', '3'], '_')"
    qt_sql "select array_join(['1', '2', '', ''], '_')"
    qt_sql "select array_join([null, null, '1', '2', '', '', null], '_')"
    qt_sql "select array_join([null, null, 1, 2, '', '', null], '_', 'any')"
    qt_sql "select array_join([''], '_')"
    qt_sql "select array_join(['', ''], '_')"
    qt_sql_array_with_constant1 "select array_with_constant(3, '_'), array_repeat('_', 3)"
    qt_sql_array_with_constant2 "select array_with_constant(2, '1'), array_repeat('1', 2)"
    qt_sql_array_with_constant3 "select array_with_constant(4, 1223), array_repeat(1223, 4)"
    qt_sql_array_with_constant4 "select array_with_constant(8, null), array_repeat(null, 8)"
    qt_sql_array_with_constant5 "select array_with_constant(null, 'abc'), array_repeat('abc', null)"
    qt_sql_array_with_constant6 "select array_with_constant(null, null), array_repeat(null, null)"
    // array_compact function
    qt_sql "select array_compact([1, 2, 3, 3, null, null, 4, 4])"
    qt_sql "select array_compact([null, null, null])"
    qt_sql "select array_compact([1.2, 1.2, 3.4, 3.3, 2.1])"
    qt_sql "select array_compact(['a','b','c','c','d'])"
    qt_sql "select array_compact(['aaa','aaa','bbb','ccc','ccccc',null, null,'dddd'])"
    qt_sql "select array_compact(['2015-03-13','2015-03-13'])"
    qt_sql "select array_compact(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3)), cast ('2023-02-07 22:07:34.999' as datetimev2(3)),cast ('2023-02-04 23:07:34.999' as datetimev2(3))))"
    qt_sql "select array_compact(array(cast ('2023-02-06' as datev2), cast ('2023-02-05' as datev2), cast ('2023-02-07' as datev2), cast ('2023-02-05' as datev2)))"
    qt_sql "select array_compact(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3)),cast (333.333 as decimalv3(6,3))))"
    qt_sql "select array_compact(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"

    // array_apply
    qt_sql """select array_apply([1000000, 1000001, 1000002], '=', 1000002)"""
    qt_sql """select array_apply([1.111, 2.222, 3.333], '>=', 2)"""
    qt_sql """select array_apply(cast(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17") as array<datetimev2>), ">", '2020-01-02')"""
    qt_sql """select array_apply(array(cast (24.99 as decimal(10,3)),cast (25.99 as decimal(10,3))), ">", '25')"""
    qt_sql """select array_apply(array(cast (24.99 as decimal(10,3)),cast (25.99 as decimal(10,3))), "!=", '25')"""
    // qt_sql """select array_apply(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), ">", '111.111')"""

    qt_sql "select array_concat([1, 2, 3], [2, 3, 4], [8, 1, 2], [9])"
    qt_sql "select array_concat([12, 23], [25, null], [null], [66])"
    qt_sql "select array_concat([1.2, 1.8], [9.0, 2.2], [2.8])"
    qt_sql "select array_concat(['aaa', null], ['bbb', 'fff'], [null, 'ccc'])"
    qt_sql "select array_concat(null, [1, 2, 3], null)"
    qt_sql "select array_concat(array(cast (12.99 as decimal(10,3)), cast (34.99 as decimal(10,3))), array(cast (999.28 as decimal(10,3)), cast (123.99 as decimal(10,3))))"
    qt_sql "select array_concat(array(cast ('2023-03-05' as datev2), cast ('2023-03-04' as datev2)), array(cast ('2023-02-01' as datev2), cast ('2023-02-05' as datev2)))"
    qt_sql "select array_concat(array(cast ('2023-03-05 12:23:24.999' as datetimev2(3)),cast ('2023-03-05 15:23:23.997' as datetimev2(3))))"

    // array_shuffle
    qt_select_array_shuffle1 "SELECT array_sum(array_shuffle([1, 2, 3, 3, null, null, 4, 4])), array_shuffle([1, 2, 3, 3, null, null, 4, 4], 0), shuffle([1, 2, 3, 3, null, null, 4, 4], 0)"
    qt_select_array_shuffle2 "SELECT array_sum(array_shuffle([1.111, 2.222, 3.333])), array_shuffle([1.111, 2.222, 3.333], 0), shuffle([1.111, 2.222, 3.333], 0)"
    qt_select_array_shuffle3 "SELECT array_size(array_shuffle(['aaa', null, 'bbb', 'fff'])), array_shuffle(['aaa', null, 'bbb', 'fff'], 0), shuffle(['aaa', null, 'bbb', 'fff'], 0)"
    qt_select_array_shuffle4 """select array_size(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17")), array_shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0), shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0)"""

    // array_zip
    qt_sql "select array_zip(['a', 'b', 'c'], ['d', 'e', 'f'])"
    qt_sql "select array_zip(['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i'])"
    qt_sql "select array_zip([1, 2, 3, 4, 5], ['d', 'o', 'r', 'i', 's'])"
    qt_sql "select array_zip([1.1, 2.2, 3.3], [1, 2, 3])"
    qt_sql "select array_zip([1, null, 3], [null, 'b', null])"
    qt_sql "select array_zip(array(cast (3.05 as decimal(10,3)), cast (2.22 as decimal(10,3))), array(cast (3.14 as decimal(10,3)), cast (6.66 as decimal(10,3))))" 
    qt_sql "select array_zip(array(cast ('2000-03-05' as datev2), cast ('2023-03-10' as datev2)), array(cast ('2000-02-02' as datev2), cast ('2023-03-10' as datev2)))"
    qt_sql "select array_zip(array(cast ('2023-03-05 12:23:24.999' as datetimev2(3)),cast ('2023-03-05 15:23:23.997' as datetimev2(3))))"
    qt_sql "select array_zip([1, 2, 3], null, ['foo', 'bar', 'test'])"

    qt_sql "select array(8, null)"
    qt_sql "select array('a', 1, 2)"
    qt_sql "select array(null, null, null)"

    // array_enumerate_uniq
    qt_sql "select array_enumerate_uniq([])"
    qt_sql "select array_enumerate_uniq([1, 2, 3, 4, 5])"
    qt_sql "select array_enumerate_uniq([1, 2, 3, 4, 5, 1, 2, 3, 4, 5])"
    qt_sql "select array_enumerate_uniq([1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5])"
    qt_sql "select array_enumerate_uniq([1, 1, 2, 2, 3, 3, 4, 4])"
    qt_sql "select array_enumerate_uniq([1, 2, 3, 1, 3, 4, 2, 5, 4, 5])"
    qt_sql "select array_enumerate_uniq([1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1])"
    qt_sql "select array_enumerate_uniq([null])"
    qt_sql "select array_enumerate_uniq([1, 2, 3, 4, 5, null, null])"
    qt_sql "select array_enumerate_uniq([1, null, 2, null, 3, null, 4, 1, null, 2, null, 3, null, 4])"
    qt_sql "select array_enumerate_uniq(['11', '22', '33', '11', '33', '22'])"
    qt_sql "select array_enumerate_uniq(array(cast (24.99 as decimal(10,3)), cast (25.99 as decimal(10,3)), cast (24.99 as decimal(10,3))))"
    qt_sql "select array_enumerate_uniq(array(cast ('2023-02-06 22:07:34.999' as datetimev2(3)), cast ('2023-02-04 23:07:34.999' as datetimev2(3)), cast ('2023-02-06 22:07:34.999' as datetimev2(3))))"
    qt_sql "select array_enumerate_uniq(array(cast (384.2933 as decimalv3(7, 4)), cast (984.1913 as decimalv3(7, 4)), cast (384.2933 as decimalv3(7, 4)), cast (722.9333 as decimalv3(7, 4)), cast (384.2933 as decimalv3(7, 4))))"
    qt_sql "select array_enumerate_uniq([1, 2, 3, 4, 5], [1, 2, 3, 4, 5])"
    qt_sql "select array_enumerate_uniq([1, 1, 1, 1, 1], [1, 1, 1, 1, 1])"
    qt_sql "select array_enumerate_uniq([1, 1, 1, 1, 1], [1, 1, 1, 1, 1])"
    qt_sql "select array_enumerate_uniq([1, 1, 2, 2, 1, 2], [1, 2, 1, 2, 2, 1])"
    qt_sql "select array_enumerate_uniq([1, null, 1, null], [null, 1, null, 1])"
    qt_sql "select array_enumerate_uniq([1, 1, 1, 1, 1, 1], [2, 1, 2, 1, 2, 1], [3, 1, 3, 1, 3, 1])"
    qt_sql "select array_enumerate_uniq([1, 3, 1], [2.0, 5.0, 2.0], ['3', '8', '3'], array(cast (34.9876 as decimalv3(6, 4)), cast (89.9865 as decimalv3(6, 4)), cast (34.9876 as decimalv3(6, 4))))"
    
    // array_pushfront
    qt_sql "select array_pushfront([1, 2, 3], 6)"
    qt_sql "select array_pushfront([1, 2, 3], null)"
    qt_sql "select array_pushfront(null, 6)"
    qt_sql "select array_pushfront([1.111, 2.222, 3.333], 9.999)"
    qt_sql "select array_pushfront(['aaa', 'bbb', 'ccc'], 'dddd')"
    qt_sql "select array_pushfront(array(cast (12.99 as decimal(10,3)), cast (34.99 as decimal(10,3))), cast (999.28 as decimal(10,3)))"
    qt_sql "select array_pushfront(array(cast ('2023-03-05' as datev2), cast ('2023-03-04' as datev2)), cast ('2023-02-05' as datev2))"
    qt_sql "select array_pushfront(array(cast ('2023-03-05 12:23:24.999' as datetimev2(3)),cast ('2023-03-05 15:23:23.997' as datetimev2(3))), cast ('2023-03-08 16:23:54.999' as datetimev2(3)))"
    qt_sql "select array_pushfront(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), cast (333.333 as decimalv3(6,3)))"

<<<<<<< HEAD
=======
    // array_pushback
    qt_sql "select array_pushback([1, 2, 3], 6)"
    qt_sql "select array_pushback([1, 2, 3], null)"
    qt_sql "select array_pushback(null, 6)"
    qt_sql "select array_pushback([1.111, 2.222, 3.333], 9.999)"
    qt_sql "select array_pushback(['aaa', 'bbb', 'ccc'], 'dddd')"
    qt_sql "select array_pushback(array(cast (12.99 as decimal(10,3)), cast (34.99 as decimal(10,3))), cast (999.28 as decimal(10,3)))"
    qt_sql "select array_pushback(array(cast ('2023-03-05' as datev2), cast ('2023-03-04' as datev2)), cast ('2023-02-05' as datev2))"
    qt_sql "select array_pushback(array(cast ('2023-03-05 12:23:24.999' as datetimev2(3)),cast ('2023-03-05 15:23:23.997' as datetimev2(3))), cast ('2023-03-08 16:23:54.999' as datetimev2(3)))"
    qt_sql "select array_pushback(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))), cast (333.333 as decimalv3(6,3)))"
    qt_sql "select array_pushback([null,null], null)"
    qt_sql "select array_pushback([null,null,null,null], 80)"

>>>>>>> 2.0.0-rc01
    // array_cum_sum
    qt_sql "select array_cum_sum([0, 2, 127])"
    qt_sql "select array_cum_sum([254, 4, 0])"
    qt_sql "select array_cum_sum([1.0, 2.1 ,3.2, 4.3, 5.4])"
    qt_sql "select array_cum_sum([-1, 2 ,-3, 4, -5])"
    qt_sql "select array_cum_sum([-5.23, 4.12, -3.02, 2.00 ,1.01])"
    qt_sql "select array_cum_sum([1, 2, 3, null])"
    qt_sql "select array_cum_sum([null, 1, null, 3, 8, null])"
    qt_sql "select array_cum_sum([null, null])"
    qt_sql "select array_cum_sum([8])"
    qt_sql "select array_cum_sum([1.1])"
    qt_sql "select array_cum_sum([null])"
    qt_sql "select array_cum_sum([])"
    qt_sql "select array_cum_sum(array(cast (12.99 as decimal(10,3)), cast (34.99 as decimal(10,3)), cast (999.28 as decimal(10,3))))"
    qt_sql "select array_cum_sum(array(cast (111.111 as decimalv3(6,3)),cast (222.222 as decimalv3(6,3))))"
    qt_sql "select array_cum_sum(array(cast (11.9999 as decimalv3(6,4)),cast (22.0001 as decimalv3(6,4))))"

    // abnormal test
    try {
        sql "select array_intersect([1, 2, 3, 1, 2, 3], '1[3, 2, 5]')"
    } catch (Exception ex) {
        assert("${ex}".contains("errCode = 2, detailMessage = No matching function with signature: array_intersect"))
    }
}
