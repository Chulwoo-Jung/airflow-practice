WITH today_dag AS (
    SELECT 
         dag_id
        ,next_dagrun_data_interval_start
        ,next_dagrun_data_interval_end
    FROM dag 
    WHERE is_paused = false
      AND is_stale = false
      AND has_import_errors = false
      AND asset_expression IS NULL
      AND (
          date(next_dagrun_data_interval_start) BETWEEN current_date - 1 AND current_date	---- DAGs executed yesterday or today
          OR date(next_dagrun_data_interval_end) BETWEEN current_date - 1 AND current_date	---- DAGs scheduled for yesterday or today
      )
)
, today_dagrun AS (
    SELECT 
         dag_id
        ,COUNT(*) AS run_cnt
        ,COUNT(CASE WHEN state = 'success' THEN 1 END) AS success_cnt
        ,COUNT(CASE WHEN state = 'failed' THEN 1 END) AS failed_cnt
        ,COUNT(CASE WHEN state = 'running' THEN 1 END) AS running_cnt
        ,MAX(CASE WHEN state = 'failed' THEN data_interval_end END) AS last_failed_date
        ,MAX(CASE WHEN state = 'success' THEN data_interval_end END) AS last_success_date
    FROM dag_run 
    WHERE date(data_interval_end) BETWEEN current_date - 1 AND current_date
    GROUP BY dag_id
)
SELECT 
     d.dag_id
    ,COALESCE(r.run_cnt, 0) AS run_cnt
    ,COALESCE(r.success_cnt, 0) AS success_cnt
    ,COALESCE(r.failed_cnt, 0) AS failed_cnt
    ,COALESCE(r.running_cnt, 0) AS running_cnt
    ,r.last_failed_date
    ,r.last_success_date
    ,d.next_dagrun_data_interval_start
    ,d.next_dagrun_data_interval_end
FROM today_dag d
LEFT JOIN today_dagrun r ON d.dag_id = r.dag_id;