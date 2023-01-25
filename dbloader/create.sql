DROP TABLE [source].[dbtask_logs];
CREATE TABLE [source].[dbtask_logs] (
    task_name VARCHAR(40) PRIMARY KEY, -- Unique name describing this task
    table_name VARCHAR(40), -- Where is the data being loaded to
    schema_name VARCHAR(40), -- Where is the data being loaded to
    database_name VARCHAR(40), -- Where is the data being loaded to
    source_url VARCHAR(max), -- URL to help identify where the data came from
    file_type VARCHAR(10),
    size BIGINT,
    hash VARBINARY(1024),
    row_count INT,
    data_start DATE, -- Optional, makes it easier to select a date range
    data_end DATE, -- Optional, makes it easier to select a date range
    store_status VARCHAR(20),
    load_status VARCHAR(20),
    stored_at DATETIME,
    loaded_at DATETIME
);
