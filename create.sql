-- Tracks all the grabs and loads
DROP TABLE [source].[botlogs];
CREATE TABLE [source].[botlogs] (
    task_name VARCHAR(40) PRIMARY KEY, -- Unique name describing this task
    table_name VARCHAR(40), -- Where is the data being loaded to
    source_url VARCHAR(100), -- URL to help identify where the data came from
    file_type VARCHAR(10),
    start_date DATE, -- Optional, makes it easier to select a date range
    end_date DATE, -- Optional, makes it easier to select a date range
    size BIGINT,
    hash VARBINARY(1024),
    row_count INT,
    stored_at DATETIME,
    loaded_at DATETIME
);
