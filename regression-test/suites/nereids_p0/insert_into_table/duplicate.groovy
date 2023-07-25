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

suite("nereids_insert_duplicate") {
    sql 'use nereids_insert_into_table_test'
    sql 'clean label from nereids_insert_into_table_test'

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set parallel_fragment_exec_instance_num=13'

    sql '''insert into dup_t
            select * except(kaint) from src'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_11 'select * from dup_t order by id, kint'

    sql '''insert into dup_t with label label_dup_cte
            with cte as (select * except(kaint) from src)
            select * from cte'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_12 'select * from dup_t order by id, kint'

    sql '''insert into dup_t partition (p1, p2) with label label_dup
            select * except(kaint) from src where id < 4'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_13 'select * from dup_t order by id, kint'

    sql '''insert into dup_light_sc_t
            select * except(kaint) from src'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_21 'select * from dup_light_sc_t order by id, kint'

    sql '''insert into dup_light_sc_t with label label_dup_light_sc_cte
            with cte as (select * except(kaint) from src)
            select * from cte'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_22 'select * from dup_light_sc_t order by id, kint'

    sql '''insert into dup_light_sc_t partition (p1, p2) with label label_dup_light_sc
            select * except(kaint) from src where id < 4'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_23 'select * from dup_light_sc_t order by id, kint'

    sql '''insert into dup_not_null_t
            select * except(kaint) from src where id is not null'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_31 'select * from dup_not_null_t order by id, kint'

    sql '''insert into dup_not_null_t with label label_dup_not_null_cte
            with cte as (select * except(kaint) from src)
            select * from cte where id is not null'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_32 'select * from dup_not_null_t order by id, kint'

    sql '''insert into dup_not_null_t partition (p1, p2) with label label_dup_not_null
            select * except(kaint) from src where id < 4 and id is not null'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_33 'select * from dup_not_null_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_t
            select * except(kaint) from src where id is not null'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_41 'select * from dup_light_sc_not_null_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_t with label label_dup_light_sc_not_null_cte
            with cte as (select * except(kaint) from src)
            select * from cte where id is not null'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_42 'select * from dup_light_sc_not_null_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_t partition (p1, p2) with label label_dup_light_sc_not_null
            select * except(kaint) from src where id < 4 and id is not null'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_43 'select * from dup_light_sc_not_null_t order by id, kint'

    // test light_schema_change
    sql 'alter table dup_light_sc_t rename column ktint ktinyint'
    sql 'alter table dup_light_sc_not_null_t rename column ktint ktinyint'

    sql '''insert into dup_light_sc_t
            select * except(kaint) from src'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_lsc1 'select * from dup_light_sc_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_t
            select * except(kaint) from src where id is not null'''
<<<<<<< HEAD
=======
    sql 'sync'
>>>>>>> 2.0.0-rc01
    qt_lsc2 'select * from dup_light_sc_not_null_t order by id, kint'
}