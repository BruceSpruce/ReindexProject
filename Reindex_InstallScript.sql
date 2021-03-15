--------------------------------------------------
-- VARIABLES -------------------------------------
-- CHANGE BELLOW IN WHOLE SCRIPT BEFORE EXECUTE --
-- CTRL + H --------------------------------------
--------------------------------------------------
-- N'sa'                    ==> Job owner
-- N'MSSQL Admins'          ==> SQL Agent notification for failure
-- MSSQLAdmins@domain.com   ==> Who will receive the reports
--------------------------------------------------

USE [msdb]
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'__REINDEX__')
EXEC msdb.dbo.sp_delete_job @job_name=N'__REINDEX__', @delete_unused_schedule=1
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'__REINDEX_CHECK_FRAGM__')
EXEC msdb.dbo.sp_delete_job @job_name=N'__REINDEX_CHECK_FRAGM__', @delete_unused_schedule=1
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'__REINDEX_KILLER__')
EXEC msdb.dbo.sp_delete_job @job_name=N'__REINDEX_KILLER__', @delete_unused_schedule=1
GO

-------------------------------
-- PROCEDURES -----------------
-------------------------------
USE [_SQL_];
GO

IF NOT EXISTS
(
    SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE SPECIFIC_SCHEMA = N'idx'
)
    EXEC ('CREATE SCHEMA [idx];');
GO

IF NOT EXISTS
(
    SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE SPECIFIC_SCHEMA = N'idx'
          AND SPECIFIC_NAME = N'usp_CheckIndexFragmentation'
)
    EXEC ('CREATE PROCEDURE [idx].[usp_CheckIndexFragmentation] AS SELECT 1');
GO

ALTER PROCEDURE [idx].[usp_CheckIndexFragmentation] @IgnoreDatabases NVARCHAR(MAX) = NULL
AS
BEGIN
    EXEC [master].[dbo].[sp_BlitzIndex] @Mode = 2,
                                        @GetAllDatabases = 1,
                                        @OutputDatabaseName = '_SQL_',
                                        @OutputSchemaName = 'idx',
                                        @OutputTableName = 'IndexInventory',
                                        @IgnoreDatabases = @IgnoreDatabases;


    -- CREATE STRUCTURE ---
    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE name = 'IX_run_datetime'
              AND object_id = OBJECT_ID('idx.IndexInventory')
    )
        EXEC (' USE [_SQL_]
            
            CREATE NONCLUSTERED INDEX [IX_run_datetime] ON [idx].[IndexInventory]
            (
	            [run_datetime] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
            
            
            USE [_SQL_];
            
            CREATE TABLE [idx].[IndexFragmentation]
            (
                [id] [INT] IDENTITY(1, 1) NOT NULL,
                [id_inventory] INT NOT NULL,
                [index_type_desc] NVARCHAR(60) NOT NULL,
                [index_depth] TINYINT NOT NULL,
                [avg_fragmentation_in_percent] FLOAT NOT NULL,
                [fragment_count] BIGINT NOT NULL,
                [page_count] BIGINT NOT NULL,
                [time_to_check] INT NOT NULL,
                CONSTRAINT [PK_ID_IndexFragmentation]
                    PRIMARY KEY CLUSTERED ([id] ASC)
                    WITH(FILLFACTOR = 95) ON [PRIMARY]
            ) ON [PRIMARY];

            IF NOT EXISTS
            (
                SELECT 1
                FROM sys.indexes
                WHERE name = ''IX_id_inventory''
                      AND object_id = OBJECT_ID(''idx.IndexFragmentation'')
            )
            CREATE INDEX [IX_id_inventory] ON [_SQL_].[idx].[IndexFragmentation] ([id_inventory]);
                                   
            ');

    -- Get last check

    DECLARE @id_current UNIQUEIDENTIFIER;

    SELECT TOP (1)
           @id_current = run_id
    FROM [_SQL_].[idx].[IndexInventory]
    WHERE run_datetime IN
          (
              SELECT MAX(run_datetime) FROM [_SQL_].[idx].[IndexInventory]
          );

    DECLARE @id INT = 0;
    DECLARE @database_name NVARCHAR(128);
    DECLARE @object_name NVARCHAR(128);
    DECLARE @index_id INT;
    DECLARE @SQL NVARCHAR(4000);


    DECLARE id_cursor CURSOR FOR
    SELECT id
    FROM [_SQL_].[idx].[IndexInventory]
    WHERE run_id = @id_current;

    OPEN id_cursor;
    FETCH NEXT FROM id_cursor
    INTO @id;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- GET DATA
        SELECT @database_name = database_name,
               @object_name = N'[' +schema_name + N'].[' + table_name + N']',
               @index_id = index_id
        FROM [_SQL_].[idx].[IndexInventory]
        WHERE id = @id;

        -- INSERT RESULTS OF FRAGMENTATION
        SET @SQL
            = N'
        USE [' + @database_name
              + N'];

        DECLARE @time1 DATETIME = (SELECT GETDATE());
        DECLARE @Ident INT;
    
        INSERT INTO [_SQL_].[idx].[IndexFragmentation] (id_inventory, index_type_desc, index_depth, avg_fragmentation_in_percent, fragment_count, page_count, time_to_check)
        SELECT ' + CAST(@id AS NVARCHAR(10))
              + N' AS id_inventory, ips.index_type_desc, ips.index_depth, ips.avg_fragmentation_in_percent, ips.fragment_count, ips.page_count, 0
        FROM sys.dm_db_index_physical_stats(DB_ID(''' + @database_name + N'''), OBJECT_ID(''' + @object_name + N'''), '
              + CAST(@index_id AS NVARCHAR(10))
              + N', 0, ''LIMITED'') ips
            INNER JOIN sys.indexes i
                ON (ips.object_id = i.object_id)
                    AND (ips.index_id = i.index_id)
        WHERE alloc_unit_type_desc = ''IN_ROW_DATA'';
    
        SET @Ident = (SELECT SCOPE_IDENTITY());
        DECLARE @time2 DATETIME = (SELECT GETDATE());

        -- UPDATE how long did it take
    
        UPDATE [_SQL_].[idx].[IndexFragmentation]
        SET time_to_check = DATEDIFF(SECOND, @time1, @time2)
        WHERE id = @Ident;

        ';
        --PRINT @SQL;
        EXECUTE (@SQL);

        FETCH NEXT FROM id_cursor
        INTO @id;
    END;

    CLOSE id_cursor;
    DEALLOCATE id_cursor;

END;
GO

USE [_SQL_];
GO

IF NOT EXISTS
(
    SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE SPECIFIC_SCHEMA = N'idx'
)
    EXEC ('CREATE SCHEMA [idx];');
GO

IF NOT EXISTS
(
    SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE SPECIFIC_SCHEMA = N'idx'
          AND SPECIFIC_NAME = N'usp_CreateToDoIndexList'
)
    EXEC ('CREATE PROCEDURE [idx].[usp_CreateToDoIndexList] AS SELECT 1');
GO

-- EXEC [_SQL_].[idx].[usp_CreateToDoIndexList] @email_rec = 'mireks@atena.pl'
ALTER PROCEDURE [idx].[usp_CreateToDoIndexList] @profile_name NVARCHAR(128) = 'mail_profile', @email_rec NVARCHAR(MAX) = 'MSSQLAdmins@domain.com'
AS
BEGIN
    -- CREATE STRUCTURE
    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.tables
        WHERE name = 'IndexToDo'
    )
        EXEC (' USE [_SQL_]

                CREATE TABLE [idx].[IndexToDo](
	                [id] [INT] IDENTITY(1,1) NOT NULL,
                    [AddedDate] DATETIME2 NOT NULL,
                    [server_name] [nvarchar](128) NULL,
	                [database_name] [nvarchar](128) NULL,
	                [schema_name] [nvarchar](128) NULL,
	                [table_name] [nvarchar](128) NULL,
	                [index_name] [nvarchar](128) NULL,
	                [index_type_desc] [nvarchar](60) NULL,
	                [total_reserved_MB] [numeric](29, 2) NULL,
	                [avg_fragmentation_in_percent] [float] NULL,
	                [SQL] [nvarchar](max) NULL,
	                [ClassifiedBy] [varchar](200) NOT NULL,
                    [Done] BIT NOT NULL
                    CONSTRAINT [PK_ID_IndexToDo] PRIMARY KEY CLUSTERED 
                    (
	                    [id] ASC
                    )
                ) ON [PRIMARY]
                
                                   
            ');

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.tables
        WHERE name = 'IndexToDoExceptions'
    )
        EXEC (' USE [_SQL_]

                CREATE TABLE [idx].[IndexToDoExceptions](
	                [id] [INT] IDENTITY(1,1) NOT NULL,
                    [SQL] [nvarchar](max) NULL
	                CONSTRAINT [PK_ID_IndexToDoExceptions] PRIMARY KEY CLUSTERED 
                    (
	                    [id] ASC
                    )
                ) ON [PRIMARY]
                
                                   
            ');
    
    -- Check exists rows in [_SQL_].[idx].[IndexToDo]
    IF NOT EXISTS
    (
        SELECT 1 FROM [_SQL_].[idx].[IndexToDo] WHERE Done = 0
    )
    BEGIN
        -- CHECK EDITION
        DECLARE @RebuildMode NVARCHAR(15);
        SELECT @RebuildMode = CASE
                WHEN SERVERPROPERTY ('EditionID') IN (1804890536, 1872460670, 610778273, -2117995310, 1674378470) THEN 'ONLINE = ON(WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = SELF))'
                ELSE 'ONLINE = OFF(WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = SELF))'
               END;

        -- GET TOP 10 THE MOST FRAGMENTATION (LESS THEN 1 GB) --
        INSERT INTO [_SQL_].[idx].[IndexToDo]
        SELECT TOP(10)
               SYSDATETIME() AS AddedDate,
               indxi.server_name,
               indxi.database_name,
               indxi.schema_name,
               indxi.table_name,
               indxi.index_name,
               indxf.index_type_desc,
               indxi.total_reserved_MB,
               indxf.avg_fragmentation_in_percent,
               CASE 
                WHEN (indxf.index_type_desc = 'HEAP') THEN N'ALTER TABLE [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'NONCLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'CLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'PRIMARY XML INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                ELSE N'WARNING! STRANGE INDEX ==> [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] SIZE: ' + CAST(indxi.total_reserved_MB AS NVARCHAR(100)) + ' MB'
               END AS [SQL],
               'CL1 - MOST FRAMGMENTATION (LESS THEN 1 GB)' AS ClassifiedBy,
               0 AS Done
        FROM [_SQL_].[idx].[IndexInventory] AS indxi
            LEFT JOIN [_SQL_].[idx].[IndexFragmentation] AS indxf
                ON indxi.id = indxf.id_inventory
        WHERE indxi.run_datetime =
        (
            SELECT MAX(run_datetime) FROM [_SQL_].[idx].[IndexInventory]
        )
        AND indxi.total_reserved_MB <= 1000
        AND indxf.avg_fragmentation_in_percent > 80
        ORDER BY indxf.avg_fragmentation_in_percent DESC

        -- GET TOP 10 THE MOST FRAGMENTATION (BIGGER THEN 50 GB AND LESS THEN 120 GB) --
        INSERT INTO [_SQL_].[idx].[IndexToDo]
        SELECT TOP(10)
               SYSDATETIME() AS AddedDate,
               indxi.server_name,
               indxi.database_name,
               indxi.schema_name,
               indxi.table_name,
               indxi.index_name,
               indxf.index_type_desc,
               indxi.total_reserved_MB,
               indxf.avg_fragmentation_in_percent,
               CASE 
                WHEN (indxf.index_type_desc = 'HEAP') THEN N'ALTER TABLE [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'NONCLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'CLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'PRIMARY XML INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                ELSE N'WARNING! STRANGE INDEX ==> [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] SIZE: ' + CAST(indxi.total_reserved_MB AS NVARCHAR(100)) + ' MB'
               END AS [SQL],
               'CL2 - MOST FRAMGMENTATION (BIGGER THEN 1 GB AND LESS THEN 50 GB)' AS ClassifiedBy,
               0 AS Done
        FROM [_SQL_].[idx].[IndexInventory] AS indxi
            LEFT JOIN [_SQL_].[idx].[IndexFragmentation] AS indxf
                ON indxi.id = indxf.id_inventory
        WHERE indxi.run_datetime =
        (
            SELECT MAX(run_datetime) FROM [_SQL_].[idx].[IndexInventory]
        )
        AND indxi.total_reserved_MB > 1000
        AND indxi.total_reserved_MB <= 50000
        AND indxf.avg_fragmentation_in_percent > 70
        ORDER BY indxf.avg_fragmentation_in_percent DESC

        -- GET TOP 10 THE MOST FRAGMENTATION (BIGGER THEN 50 GB AND LESS THEN 120 GB) --
        INSERT INTO [_SQL_].[idx].[IndexToDo]
        SELECT TOP(10)
               SYSDATETIME() AS AddedDate,
               indxi.server_name,
               indxi.database_name,
               indxi.schema_name,
               indxi.table_name,
               indxi.index_name,
               indxf.index_type_desc,
               indxi.total_reserved_MB,
               indxf.avg_fragmentation_in_percent,
               CASE 
                WHEN (indxf.index_type_desc = 'HEAP') THEN N'WARNING! STRANGE HEAP: [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] Size: ' + CAST(indxi.total_reserved_MB AS NVARCHAR(100)) + ' MB'
                WHEN (indxf.index_type_desc = 'NONCLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'CLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'PRIMARY XML INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REORGANIZE'
                ELSE N'WARNING! STRANGE INDEX ==> [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] SIZE: ' + CAST(indxi.total_reserved_MB AS NVARCHAR(100)) + ' MB'
               END AS [SQL],
               'CL3 - MOST FRAMGMENTATION (BIGGER THEN 50 GB AND LESS THEN 120 GB)' AS ClassifiedBy,
               0 AS Done
        FROM [_SQL_].[idx].[IndexInventory] AS indxi
            LEFT JOIN [_SQL_].[idx].[IndexFragmentation] AS indxf
                ON indxi.id = indxf.id_inventory
        WHERE indxi.run_datetime =
        (
            SELECT MAX(run_datetime) FROM [_SQL_].[idx].[IndexInventory]
        )
        AND indxi.total_reserved_MB > 50000
        AND indxi.total_reserved_MB <= 120000
        AND indxf.avg_fragmentation_in_percent > 60
        ORDER BY indxf.avg_fragmentation_in_percent DESC

        -- GET TOP 10 THE MOST FRAGMENTATION (BIGGER THEN 120 GB) --
        INSERT INTO [_SQL_].[idx].[IndexToDo]
        SELECT TOP(10)
               SYSDATETIME() AS AddedDate,
               indxi.server_name,
               indxi.database_name,
               indxi.schema_name,
               indxi.table_name,
               indxi.index_name,
               indxf.index_type_desc,
               indxi.total_reserved_MB,
               indxf.avg_fragmentation_in_percent,
               CASE 
                WHEN (indxf.index_type_desc = 'HEAP') THEN N'WARNING! STRANGE HEAP: [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] Size: ' + CAST(indxi.total_reserved_MB AS NVARCHAR(100)) + ' MB'
                WHEN (indxf.index_type_desc = 'NONCLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REORGANIZE'
                WHEN (indxf.index_type_desc = 'CLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REORGANIZE'
                WHEN (indxf.index_type_desc = 'PRIMARY XML INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REORGANIZE'
                ELSE N'WARNING! STRANGE INDEX ==> [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] SIZE: ' + CAST(indxi.total_reserved_MB AS NVARCHAR(100)) + ' MB'
               END AS [SQL],
               'CL4 - MOST FRAMGMENTATION (BIGGER THEN 120 GB)' AS ClassifiedBy,
               0 AS Done
        FROM [_SQL_].[idx].[IndexInventory] AS indxi
            LEFT JOIN [_SQL_].[idx].[IndexFragmentation] AS indxf
                ON indxi.id = indxf.id_inventory
        WHERE indxi.run_datetime =
        (
            SELECT MAX(run_datetime) FROM [_SQL_].[idx].[IndexInventory]
        )
        AND indxi.total_reserved_MB > 120000
        AND indxf.avg_fragmentation_in_percent > 50
        ORDER BY indxf.avg_fragmentation_in_percent DESC

        -- GET TOP 10 THE MOST FRAGMENTATION (THE MOST READERS) --
        INSERT INTO [_SQL_].[idx].[IndexToDo]
        SELECT TOP(10)
               SYSDATETIME() AS AddedDate,
               indxi.server_name,
               indxi.database_name,
               indxi.schema_name,
               indxi.table_name,
               indxi.index_name,
               indxf.index_type_desc,
               indxi.total_reserved_MB,
               indxf.avg_fragmentation_in_percent,
               CASE 
                WHEN (indxf.index_type_desc = 'HEAP') AND indxi.total_reserved_MB < 50000 THEN N'ALTER TABLE [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REBUILD WITH (' + @RebuildMode + ')'
                WHEN (indxf.index_type_desc = 'HEAP') AND indxi.total_reserved_MB >= 50000 THEN N'WARNING! STRANGE HEAP: [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] Size: ' + CAST(indxi.total_reserved_MB AS NVARCHAR(100)) + ' MB'
                WHEN (indxf.index_type_desc = 'NONCLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REORGANIZE'
                WHEN (indxf.index_type_desc = 'CLUSTERED INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REORGANIZE'
                WHEN (indxf.index_type_desc = 'PRIMARY XML INDEX') THEN N'ALTER INDEX [' + indxi.index_name + '] ON [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] REORGANIZE'
                ELSE N'WARNING! STRANGE INDEX ==> [' + indxi.database_name + '].[' + indxi.schema_name + '].[' + indxi.table_name + '] SIZE: ' + CAST(indxi.total_reserved_MB AS NVARCHAR(100)) + ' MB'
               END AS [SQL],
               'CL5 - MOST FRAMGMENTATION (THE MOST READERS)' AS ClassifiedBy,
               0 AS Done
        FROM [_SQL_].[idx].[IndexInventory] AS indxi
            LEFT JOIN [_SQL_].[idx].[IndexFragmentation] AS indxf
                ON indxi.id = indxf.id_inventory
        WHERE indxi.run_datetime =
        (
            SELECT MAX(run_datetime) FROM [_SQL_].[idx].[IndexInventory]
        )
        AND indxi.reads_per_write > 0.5
        AND indxf.avg_fragmentation_in_percent > 50
        ORDER BY indxf.avg_fragmentation_in_percent DESC
   
        --- ENABLE JOB FOR REINDEX ---
        EXEC msdb.dbo.sp_update_job @job_name=N'__REINDEX__', 
		        @enabled=1;

        --- SEND EMAILS FOR STRANGE INDEXES
        DECLARE @sql NVARCHAR(MAX);
        DECLARE @send_email BIT = 0;
        DECLARE @subject NVARCHAR(256);
        DECLARE @body NVARCHAR(MAX) = N'';

        DECLARE sql_cursor CURSOR FOR
        SELECT [SQL]
        FROM [_SQL_].[idx].[IndexToDo]
        WHERE SQL LIKE 'WARNING!%' AND Done = 0;

        OPEN sql_cursor;
        FETCH NEXT FROM sql_cursor
        INTO @sql;

        WHILE @@FETCH_STATUS = 0
        BEGIN
        
            SET @send_email = 1;
            SET @body += @sql + '</br>'; 

            FETCH NEXT FROM sql_cursor
            INTO @sql;
        END;

        CLOSE sql_cursor;
        DEALLOCATE sql_cursor;

        IF (@send_email = 1)
        BEGIN
            SET @subject = '[' + @@SERVERNAME + '] I found big, on the exceptions list or strange index!'
            EXEC msdb.dbo.sp_send_dbmail
				    @profile_name = @profile_name,
				    @recipients = @email_rec,
				    @body =  @body,
				    @subject = @subject,
				    @body_format = 'HTML';
        
            UPDATE [_SQL_].[idx].[IndexToDo]
            SET Done = 1 
            WHERE SQL LIKE 'WARNING!%' AND Done = 0;
        END -- send email
        
        -- Update Exceptions
        UPDATE [_SQL_].[idx].[IndexToDo]
            SET Done = 1 
        WHERE [SQL] IN (SELECT [SQL] FROM [_SQL_].[idx].[IndexToDoExceptions]);

    END -- check if not exists rows in [_SQL_].[idx].[IndexToDo]
END -- OF PROCEDURE
GO

USE [_SQL_];
GO

IF NOT EXISTS
(
    SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE SPECIFIC_SCHEMA = N'idx'
)
    EXEC ('CREATE SCHEMA [idx];');
GO

IF NOT EXISTS
(
    SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE SPECIFIC_SCHEMA = N'idx'
          AND SPECIFIC_NAME = N'usp_Reindex'
)
    EXEC ('CREATE PROCEDURE [idx].[usp_Reindex] AS SELECT 1');
GO

-- EXAMPLE: EXEC [_SQL_].[idx].[usp_Reindex] @profile_name = 'mail_profile', @email_rec = 'MSSQLAdmins@domain.com'
ALTER PROCEDURE [idx].[usp_Reindex] @profile_name NVARCHAR(128) = 'mail_profile', @email_rec NVARCHAR(MAX) = 'MSSQLAdmins@domain.com'
AS
BEGIN
    DECLARE @Subject NVARCHAR(255)
    SET @Subject = '[' + @@SERVERNAME + '] - ERROR DURING REBUILD/REORGANIZATION OF THE INDEX';

    DECLARE @ID INT
    DECLARE @command NVARCHAR(MAX)
    DECLARE @errortext NVARCHAR(MAX)
    DECLARE @Count INT

    SELECT @Count = COUNT(*) FROM [_SQL_].[idx].[IndexToDo] WHERE Done = 0
    IF (@Count > 0)
    BEGIN
	    SELECT TOP(1) @ID = [id]
		      ,@command = [SQL]
	      FROM [_SQL_].[idx].[IndexToDo] 
          WHERE Done = 0
	      ORDER BY [id];	    
	      BEGIN TRY
		        EXEC (@command)
	      END TRY
	      BEGIN CATCH
		        SELECT @errortext = ERROR_MESSAGE();
		        -- check text = Online index operation cannot be performed%
		        IF (@errortext like '%online index operation cannot be performed%' or @errortext like '%online operation cannot be performed for index%')
		        BEGIN
			        BEGIN TRY
				        SET @command = REPLACE(@command, 'REBUILD WITH (ONLINE)', 'REORGANIZE')
				        SET @command = REPLACE(@command, 'REBUILD WITH (OFFLINE)', 'REORGANIZE')
				        EXEC (@command)
			        END TRY
			        BEGIN CATCH
				        SELECT @errortext = ERROR_MESSAGE();
				        SET @errortext = @command + CHAR(10) + CHAR(13) + @errortext;
				        --send email error message
				        EXEC msdb.dbo.sp_send_dbmail
					        @profile_name = @profile_name,
					        @recipients = @email_rec,
					        @body = @errortext,
					        @subject = @Subject;
			        END CATCH
		        END
		        ELSE
		        BEGIN
			        SELECT @errortext = ERROR_MESSAGE();
			        SET @errortext = @command + CHAR(10) + CHAR(13) + @errortext;
			        --send email error message
			        EXEC msdb.dbo.sp_send_dbmail
				        @profile_name = @profile_name,
				        @recipients = @email_rec,
				        @body = @errortext,
				        @subject = @Subject;
		        END
	    END CATCH

	    UPDATE [_SQL_].[idx].[IndexToDo] SET Done = 1 WHERE [Done] = 0 AND id = @ID
    END
    ELSE
    BEGIN
	    EXEC msdb.dbo.sp_update_job @job_name=N'__REINDEX__', 
		    @enabled=0
    END
END
GO

USE [_SQL_];
GO

IF NOT EXISTS
(
    SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE SPECIFIC_SCHEMA = N'idx'
)
    EXEC ('CREATE SCHEMA [idx];');
GO

IF NOT EXISTS
(
    SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE SPECIFIC_SCHEMA = N'idx'
          AND SPECIFIC_NAME = N'usp_KillLongReindexSession'
)
    EXEC ('CREATE PROCEDURE [idx].[usp_KillLongReindexSession] AS SELECT 1');
GO

-- EXAMPLE: EXEC [_SQL_].[idx].[usp_KillLongReindexSession]
ALTER PROCEDURE [idx].[usp_KillLongReindexSession]
AS
BEGIN
    DECLARE @SPID INT;
    
    SELECT TOP(1) @SPID = session_id
    FROM sys.dm_exec_requests r
        OUTER APPLY sys.dm_exec_sql_text(sql_handle) t
    JOIN sys.sysprocesses sp ON r.session_id = sp.spid
    WHERE session_id != @@SPID
          AND session_id > 50
          AND SUBSTRING(   t.text,
                           (r.statement_start_offset / 2) + 1,
                           CASE
                               WHEN statement_end_offset = -1
                                    OR statement_end_offset = 0 THEN
                           (DATALENGTH(t.text) - r.statement_start_offset / 2) + 1
                               ELSE
                           (r.statement_end_offset - r.statement_start_offset) / 2 + 1
                           END
                       ) LIKE 'ALTER%REORGANIZE%'
          OR SUBSTRING(   t.text,
                          (r.statement_start_offset / 2) + 1,
                          CASE
                              WHEN statement_end_offset = -1
                                   OR statement_end_offset = 0 THEN
                          (DATALENGTH(t.text) - r.statement_start_offset / 2) + 1
                              ELSE
                          (r.statement_end_offset - r.statement_start_offset) / 2 + 1
                          END
                      ) LIKE 'ALTER%REBUILD%';
    IF (@SPID IS NOT NULL)
        EXEC ('KILL ' + @SPID);
END;
GO
-------------------------------
-- JOBS -----------------------
-------------------------------

USE [msdb]
GO
DECLARE @jobId BINARY(16)
DECLARE @desc_1 NVARCHAR(MAX) = 'Index inventory and fragmentation check - ' + SUSER_NAME() + ' - ' + CONVERT(CHAR(10), GETDATE(), 121)

EXEC  msdb.dbo.sp_add_job @job_name=N'__REINDEX_CHECK_FRAGM__', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description= @desc_1, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'MSSQL Admins', @job_id = @jobId OUTPUT
select @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'__REINDEX_CHECK_FRAGM__'
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'__REINDEX_CHECK_FRAGM__', @step_name=N'_fragm_check_', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [idx].[usp_CheckIndexFragmentation]
                   EXEC [idx].[usp_CreateToDoIndexList] @email_rec = ''MSSQLAdmins@domain.com''', 
		@database_name=N'_SQL_', 
		@flags=0
GO
USE [msdb]
GO
DECLARE @desc_1 NVARCHAR(MAX) = 'Index inventory and fragmentation check - ' + SUSER_NAME() + ' - ' + CONVERT(CHAR(10), GETDATE(), 121)
EXEC msdb.dbo.sp_update_job @job_name=N'__REINDEX_CHECK_FRAGM__', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=@desc_1, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'MSSQL Admins', 
		@notify_page_operator_name=N''
GO
USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'__REINDEX_CHECK_FRAGM__', @name=N'third Sat of every Month', 
		@enabled=1, 
		@freq_type=32, 
		@freq_interval=7, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=4, 
		@freq_recurrence_factor=1, 
		@active_start_date=20191020, 
		@active_end_date=99991231, 
		@active_start_time=230000, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO

USE [msdb]
GO

/****** Object:  Job [__REINDEX__]    Script Date: 20.11.2019 15:09:06 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 20.11.2019 15:09:06 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
DECLARE @desc_1 NVARCHAR(MAX) = 'Rebuild indexes from [_SQL_].[idx].[IndexToDo] list - ' + SUSER_NAME() + ' - ' + CONVERT(CHAR(10), GETDATE(), 121)

EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'__REINDEX__', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=@desc_1, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'MSSQL Admins', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [_RX_]    Script Date: 20.11.2019 15:09:06 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'_RX_', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [_SQL_].[idx].[usp_Reindex] @profile_name = ''mail_profile'', @email_rec = ''MSSQLAdmins@domain.com''', 
		@database_name=N'_SQL_', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Sunday At Midday During 1 Hour', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=2, 
		@freq_subday_interval=10, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20191120, 
		@active_end_date=99991231, 
		@active_start_time=120000, 
		@active_end_time=125959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'__REINDEX__', 
		@enabled=0
GO



USE [msdb]
GO

/****** Object:  Job [__REINDEX_KILLER__]    Script Date: 25.11.2019 11:41:17 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 25.11.2019 11:41:17 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
DECLARE @desc_1 NVARCHAR(MAX) = 'Killing long sessions with rebuilt indexes - ' + SUSER_NAME() + ' - ' + CONVERT(CHAR(10), GETDATE(), 121)

EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'__REINDEX_KILLER__', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=@desc_1, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'MSSQL Admins', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [_RX_]    Script Date: 25.11.2019 11:41:17 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'_RX_', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [_SQL_].[idx].[usp_KillLongReindexSession]', 
		@database_name=N'_SQL_', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Sunday At 4 PM', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=10, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20191120, 
		@active_end_date=99991231, 
		@active_start_time=160000, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


