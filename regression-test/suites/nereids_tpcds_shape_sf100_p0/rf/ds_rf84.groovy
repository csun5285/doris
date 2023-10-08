/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("ds_rf84") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set broadcast_row_count_limit = 30000000'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_pipeline_engine=true'
    String stmt = '''
    explain physical plan
    select  c_customer_id as customer_id
       , concat(concat(coalesce(c_last_name,''), ','), coalesce(c_first_name,'')) as customername
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where ca_city	        =  'Oakwood'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  5806
   and ib_upper_bound   <=  5806 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 limit 100;

    '''
    String plan = sql "${stmt}"
    println plan
    def getRuntimeFilters = { plantree ->
        {
            def lst = []
            plantree.eachMatch("RF\\d+\\[[^#]+#\\d+->\\[[^\\]]+\\]") {
                ch ->
                    {
                        lst.add(ch.replaceAll("#\\d+", ''))
                    }
            }
            return lst.join(',')
        }
    }
    
    // def outFile = "regression-test/suites/nereids_tpcds_shape_sf100_p0/ddl/rf/rf.84"
    // File file = new File(outFile)
    // file.write(getRuntimeFilters(plan))
    
     assertEquals("RF4[cd_demo_sk->[sr_cdemo_sk],RF3[c_current_cdemo_sk->[cd_demo_sk],RF2[hd_demo_sk->[c_current_hdemo_sk],RF1[ca_address_sk->[c_current_addr_sk],RF0[ib_income_band_sk->[hd_income_band_sk]", getRuntimeFilters(plan))
}
