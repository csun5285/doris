<<<<<<< HEAD
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2) */
=======
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2, parallel_pipeline_task_num=2) */
>>>>>>> 2.0.0-rc01
COUNT(*) FROM (SELECT n1.regionkey, n1.nationkey FROM tpch_tiny_nation n1 JOIN tpch_tiny_nation n2 ON n1.regionkey = n2.regionkey LIMIT 5) foo
