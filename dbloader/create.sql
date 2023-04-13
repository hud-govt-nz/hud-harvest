DROP TABLE [Source].[DBTask_Logs];
CREATE TABLE [Source].[DBTask_Logs] (
    Task_Name VARCHAR(40) PRIMARY KEY, -- Unique name describing this task
    Table_Name VARCHAR(40), -- Where is the data being loaded to
    Schema_Name VARCHAR(40), -- Where is the data being loaded to
    Database_Name VARCHAR(40), -- Where is the data being loaded to
    Source_URL VARCHAR(max), -- URL to help identify where the data came from
    File_Type VARCHAR(10),
    Size BIGINT,
    Hash VARBINARY(1024),
    Row_Count INT,
    Data_Start DATE, -- Optional, makes it easier to select a date range
    Data_End DATE, -- Optional, makes it easier to select a date range
    Store_Status VARCHAR(20),
    Load_Status VARCHAR(20),
    Stored_At DATETIME,
    Loaded_At DATETIME
);
