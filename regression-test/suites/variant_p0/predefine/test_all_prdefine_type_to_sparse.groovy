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
suite("test_all_prdefine_type_to_sparse", "p0"){ 

    sql """ set describe_extend_variant_column = true """

    def tableName = "test_all_prdefine_type_to_sparse"
    sql "set enable_decimal256 = true"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
        `id` bigint NOT NULL AUTO_INCREMENT,
        `var`  variant <
                'boolean_*':boolean,
                'tinyint_*':tinyint,
                'smallint_*':smallint,
                'int_*':int, 
                'bigint_*':bigint,
                'largeint_*':largeint,
                'char_*': text,
                'string_*':string, 
                'float_*':float,
                'double_*':double,
                'decimal32_*':decimalv3(8,2),
                'decimal64_*':decimalv3(16,9),
                'decimal128_*':decimalv3(36,9),
                'decimal256_*':decimalv3(70,60),
                'datetime_*':datetime,
                'date_*':date,
                'ipv4_*':ipv4,
                'ipv6_*':ipv6,
                'array_boolean_*':array<boolean>,
                'array_tinyint_*':array<tinyint>,
                'array_smallint_*':array<smallint>,
                'array_int_*':array<int>,
                'array_bigint_*':array<bigint>,
                'array_largeint_*':array<largeint>,
                'array_char_*':array<text>,
                'array_string_*':array<string>,
                'array_float_*':array<float>,
                'array_double_*':array<double>,
                'array_decimal32_*':array<decimalv3(8,2)>,
                'array_decimal64_*':array<decimalv3(16,9)>,
                'array_decimal128_*':array<decimalv3(36,9)>,
                'array_decimal256_*':array<decimalv3(70,60)>,
                'array_datetime_*':array<datetime>,
                'array_date_*':array<date>,
                'array_ipv4_*':array<ipv4>,
                'array_ipv6_*':array<ipv6>,
                properties (
                    "variant_enable_typed_paths_to_sparse" = "true",
                    "variant_max_subcolumns_count" = "1"
                )
            > NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")

    """

    sql """
         INSERT INTO ${tableName} (`var`) VALUES
        (
            '{
              "boolean_1": true,
              "tinyint_1": 1,
              "smallint_1": 1,
              "int_1": 1,
              "bigint_1": 1,
              "largeint_1": 1,
              "char_1": "1",
              "string_1": "1",
              "float_1": 1.0,
              "double_1": 1.0,
              "decimal32_1": 1.0,
              "decimal64_1": 1.0,
              "decimal128_1": 1.0,
              "decimal256_1": 1.0,
              "datetime_1": "2021-01-01 00:00:00",
              "date_1": "2021-01-01",
              "ipv4_1": "192.168.1.1",
              "ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
              "array_boolean_1": [true],
              "array_tinyint_1": [1],
              "array_smallint_1": [1],
              "array_int_1": [1],
              "array_bigint_1": [1],
              "array_largeint_1": [1],
              "array_char_1": ["1"],
              "array_string_1": ["1"],
              "array_float_1": [1.0],
              "array_double_1": [1.0],
              "array_decimal32_1": [1.0],
              "array_decimal64_1": [1.0],
              "array_decimal128_1": [1.0],
              "array_decimal256_1": [1.0],
              "array_datetime_1": ["2021-01-01 00:00:00"],
              "array_date_1": ["2021-01-01"],
              "array_ipv4_1": ["192.168.1.1"],
              "array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],
              "other_1": "1"
            }'
        ),
        (
            '{"other_1": "1"}'
        ); 
    """


    qt_sql """ desc ${tableName} """
    qt_sql """ select * from ${tableName} """
}