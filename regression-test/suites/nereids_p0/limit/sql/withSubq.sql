-- database: presto; groups: limit; tables: nation
<<<<<<< HEAD
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2) */
=======
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2, parallel_pipeline_task_num=2) */
>>>>>>> 2.0.0-rc01
COUNT(*) FROM (SELECT * FROM tpch_tiny_nation LIMIT 10) t1
