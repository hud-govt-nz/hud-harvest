DROP TABLE [linz_historic].[run_logs];
CREATE TABLE [linz_historic].[run_logs] (
    run_name VARCHAR(40)
  , run_args VARCHAR(max)
  , jobs_count SMALLINT
  , tasks_count SMALLINT
  , tasks_succeeded SMALLINT
  , tasks_failed SMALLINT
  , tasks_skipped SMALLINT
  , status VARCHAR(20)
  , started_at DATETIME
  , finished_at DATETIME
);
