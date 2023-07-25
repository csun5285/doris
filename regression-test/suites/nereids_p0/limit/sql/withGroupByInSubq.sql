-- database: presto; groups: limit; tables: partsupp
<<<<<<< HEAD
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2) */ 
=======
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2, parallel_pipeline_task_num=2) */
>>>>>>> 2.0.0-rc01
COUNT(*) FROM (
    SELECT suppkey, COUNT(*) FROM tpch_tiny_partsupp
    GROUP BY suppkey LIMIT 20) t1
