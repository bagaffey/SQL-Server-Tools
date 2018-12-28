SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[CreateAuditTrail]
	@TableName varchar(128),
	@Owner varchar(128) = 'dbo',
	@AuditNameExtension varchar(128) = '_Log',
	@DropAuditTable bit = 0,
	@InsertOldLogData bit = 0
AS
BEGIN
	-- Check if table exists
	IF NOT EXISTS (SELECT * FROM sys.objects AS o WHERE o.object_id = OBJECT_ID(N'[' + @Owner + '].[' + @TableName + ']') AND OBJECTPROPERTY(o.object_id, N'IsUserTable') = 1)
	BEGIN
		PRINT 'ERROR: Table does not exist'
		RETURN
	END

	-- Check @AuditNameExtension
	IF @AuditNameExtension IS NULL
	BEGIN
		PRINT 'ERROR: @AuditNameExtension cannot be null'
		RETURN
	END
	
	-- Drop audit table if it exists and drop should be forced
	IF (EXISTS (SELECT * FROM sys.objects AS o WHERE o.object_id = OBJECT_ID(N'[' + @Owner + '].[' + @TableName + @AuditNameExtension + ']') AND OBJECTPROPERTY(o.object_id, N'IsUserTable') = 1 ) AND @DropAuditTable = 1)
	BEGIN
		PRINT 'Dropping audit table [' + @Owner + '].[' + @TableName + @AuditNameExtension + ']';
		IF (@InsertOldLogData = 1)
		BEGIN
			DECLARE @OldLogTableName VARCHAR(256) = @TableName + @AuditNameExtension;
			SET NOCOUNT ON;
			-- Get all the columns names that we might be putting into the new version of the table.
			SELECT c.name INTO #OldLogColumnNames
			FROM sys.columns as c
			INNER JOIN sys.tables as t ON t.object_id = c.object_id
			WHERE t.name = @OldLogTableName AND c.name <> 'AuditId'
			
			-- Save the data
			DECLARE @GETOLDDATAQRY VARCHAR(512) = 'SELECT * INTO ##OldLogData FROM [' + @OldLogTableName + ']';
			EXEC (@GETOLDDATAQRY);
		END;
		EXEC ('DROP TABLE [' + @TableName + @AuditNameExtension + ']')
	END

	-- Declare cursor to loop over columns
	DECLARE TableColumns CURSOR Read_Only
	FOR SELECT c.name, t.name AS TypeName, c.max_length, c.is_nullable, c.collation_name, c.precision, c.scale
		FROM sys.objects as o
		INNER JOIN sys.columns as c ON o.object_id = c.object_id
		INNER JOIN sys.types as t ON c.system_type_id = t.system_type_id AND c.user_type_id = t.user_type_id AND t.name <> 'sysname'
		WHERE o.object_id = OBJECT_ID(N'[' + @Owner + '].[' + @TableName + ']')
		AND OBJECTPROPERTY(o.object_id, N'IsUserTable') = 1
		ORDER BY c.column_id
	-- Old query for SQL Server 2000 compatibility
	--SELECT b.name, c.name as TypeName, b.length, b.isnullable, b.collation, b.xprec, b.xscale
	--	FROM sysobjects a 
	--	inner join syscolumns b on a.id = b.id 
	--	inner join systypes c on b.xtype = c.xtype and c.name <> 'sysname' 
	--	WHERE a.id = object_id(N'[' + @Owner + '].[' + @TableName + ']') 
	--	and OBJECTPROPERTY(a.id, N'IsUserTable') = 1 
	--	ORDER BY b.colId

	OPEN TableColumns

	-- Declare temp variable to fetch records into
	DECLARE @ColumnName varchar(128)
	DECLARE @ColumnType varchar(128)
	DECLARE @ColumnLength smallint
	DECLARE @ColumnNullable int
	DECLARE @ColumnCollation sysname
	DECLARE @ColumnPrecision tinyint
	DECLARE @ColumnScale tinyint

	-- Declare variable to build statements
	DECLARE @CreateStatement varchar(MAX)
	DECLARE @ListOfFields varchar(4000) = ''

	-- Check if audit log table exists
	IF EXISTS (SELECT * FROM sys.objects AS o WHERE o.object_id = object_id(N'[' + @Owner + '].[' + @TableName + @AuditNameExtension + ']') AND OBJECTPROPERTY(o.object_id, N'IsUserTable') = 1)
	BEGIN
		PRINT 'Audit Table already exists. Only triggers will be updated.'

		FETCH Next FROM TableColumns
		INTO @ColumnName, @ColumnType, @ColumnLength, @ColumnNullable, @ColumnCollation, @ColumnPrecision, @ColumnScale
		
		WHILE @@FETCH_STATUS = 0
		BEGIN
			IF (@ColumnType <> 'text' and @ColumnType <> 'ntext' and @ColumnType <> 'image' and @ColumnType <> 'timestamp')
			BEGIN
				SET @ListOfFields += '[' + @ColumnName + '],'
			END

			FETCH Next FROM TableColumns
			INTO @ColumnName, @ColumnType, @ColumnLength, @ColumnNullable, @ColumnCollation, @ColumnPrecision, @ColumnScale
		END
	END
	ELSE
	BEGIN
		-- AuditTable does not exist, create new
		-- Start of create table
		SET @CreateStatement = 'CREATE TABLE [' + @Owner + '].[' + @TableName + @AuditNameExtension + '] ('
		SET @CreateStatement += '[AuditId] [int] IDENTITY (1, 1) NOT NULL,'

		FETCH Next FROM TableColumns
		INTO @ColumnName, @ColumnType, @ColumnLength, @ColumnNullable, @ColumnCollation, @ColumnPrecision, @ColumnScale
		
		WHILE @@FETCH_STATUS = 0
		BEGIN
			IF (@ColumnType <> 'text' and @ColumnType <> 'ntext' and @ColumnType <> 'image' and @ColumnType <> 'timestamp')
			BEGIN
				SET @ListOfFields += '[' + @ColumnName + '],'
		
				SET @CreateStatement += '[' + @ColumnName + '] [' + @ColumnType + '] '
				
				IF @ColumnType in ('binary', 'char', 'nchar', 'nvarchar', 'varbinary', 'varchar')
				BEGIN
					IF (@ColumnLength = -1)
						SET @CreateStatement += '(max) '	 	
					ELSE
						SET @CreateStatement += '(' + CAST(@ColumnLength as varchar(10)) + ') '	 	
				END
		
				IF @ColumnType in ('decimal', 'numeric')
					SET @CreateStatement += '(' + CAST(@ColumnPrecision as varchar(10)) + ',' + CAST(@ColumnScale as varchar(10)) + ') '	 	
		
				IF @ColumnType in ('char', 'nchar', 'nvarchar', 'varchar', 'text', 'ntext')
					SET @CreateStatement += 'COLLATE ' + @ColumnCollation + ' '
				-- NULLs should be allowed in the log table to prevent errors when adding new non-nullable columns
				-- IF @ColumnNullable = 0
				-- SET @CreateStatement += 'NOT '
		
				SET @CreateStatement += 'NULL, '	 	
			END

			FETCH Next FROM TableColumns
			INTO @ColumnName, @ColumnType, @ColumnLength, @ColumnNullable, @ColumnCollation, @ColumnPrecision, @ColumnScale
		END
		
		-- Audit Trail columns
		-- Removed NOT NULL property because this information may not be known during old log data import.
		-- SET @CreateStatement += '[AuditAction] [char] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL ,'
		SET @CreateStatement += '[AuditAction] [char] (1) COLLATE SQL_Latin1_General_CP1_CI_AS ,'
		SET @CreateStatement += '[AuditDate] [datetime] NOT NULL ,'
		SET @CreateStatement += '[AuditUser] [varchar] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,'
		SET @CreateStatement += '[AuditApp] [varchar](128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL)' 

		-- Create audit table
		PRINT 'Creating audit table [' + @Owner + '].[' + @TableName + @AuditNameExtension + ']'
		EXEC (@CreateStatement)

		-- Set primary key and default values
		SET @CreateStatement = 'ALTER TABLE [' + @Owner + '].[' + @TableName + @AuditNameExtension + '] ADD '
		SET @CreateStatement += 'CONSTRAINT [DF_' + @TableName + @AuditNameExtension + '_AuditDate] DEFAULT (GETDATE()) FOR [AuditDate],'
		SET @CreateStatement += 'CONSTRAINT [DF_' + @TableName + @AuditNameExtension + '_AuditUser] DEFAULT (SUSER_SNAME()) FOR [AuditUser],CONSTRAINT [PK_' + @TableName + @AuditNameExtension + '] PRIMARY KEY  CLUSTERED '
		SET @CreateStatement += '([AuditId])  ON [PRIMARY], '
		SET @CreateStatement += 'CONSTRAINT [DF_' + @TableName + @AuditNameExtension + '_AuditApp]  DEFAULT (RTRIM(ISNULL(APP_NAME(),''UNNAMED APP''))) FOR [AuditApp]'

		EXEC (@CreateStatement)
		
		IF (@InsertOldLogData = 1)
		BEGIN
			DECLARE @T VARCHAR(256) = @TableName + @AuditNameExtension
			DECLARE @ListOfOldLogFields VARCHAR(4000)
			SELECT @ListOfOldLogFields = ISNULL(@ListOfOldLogFields + ',' + c.name, c.name) 
			FROM sys.columns as c 
			WHERE c.object_id = OBJECT_ID(@T) AND c.name IN (select name from #OldLogColumnNames)
			-- PRINT @ListOfOldLogFields -- for debugging
			-- Import the data now.
			DECLARE @ImportOldDataStatement VARCHAR(8000) = 'INSERT INTO [' + @T + '] (' + @ListOfOldLogFields + ') SELECT ' + @ListOfOldLogFields + ' FROM ##OldLogData'
			-- PRINT @ImportOldDataStatement; -- for debugging
			BEGIN TRY
				EXEC (@ImportOldDataStatement);
				DROP TABLE ##OldLogData;
			END TRY
			BEGIN CATCH
				PRINT 'An error occurred while trying to import old log data.'
				PRINT 'Check ##OldLogData for making another attempt to import the data.'
				PRINT 'The INSERT statement attempted was as follows:'
				PRINT @ImportOldDataStatement
			END CATCH;
		END;
	END

	CLOSE TableColumns
	DEALLOCATE TableColumns

	/* Drop Triggers, if they exist */
	PRINT 'Dropping triggers'
	IF EXISTS (SELECT * FROM sys.objects AS o WHERE o.object_id = OBJECT_ID(N'[' + @Owner + '].[tr_' + @TableName + '_Insert]') AND OBJECTPROPERTY(o.object_id, N'IsTrigger') = 1) 
		EXEC ('DROP TRIGGER [' + @Owner + '].[tr_' + @TableName + '_Insert]');

	IF EXISTS (SELECT * FROM sys.objects AS o WHERE o.object_id = OBJECT_ID(N'[' + @Owner + '].[tr_' + @TableName + '_Update]') AND OBJECTPROPERTY(o.object_id, N'IsTrigger') = 1) 
		EXEC ('DROP TRIGGER [' + @Owner + '].[tr_' + @TableName + '_Update]');

	IF EXISTS (SELECT * FROM sys.objects AS o WHERE o.object_id = OBJECT_ID(N'[' + @Owner + '].[tr_' + @TableName + '_Delete]') AND OBJECTPROPERTY(o.object_id, N'IsTrigger') = 1) 
		EXEC ('DROP TRIGGER [' + @Owner + '].[tr_' + @TableName + '_Delete]');

	PRINT 'Creating triggers' 
	/* BGaffey 
	 * Modified the CREATE TRIGGER statements below to include a block
	 * that sets NOCOUNT to ON */
	EXEC ('CREATE TRIGGER [tr_' + @TableName + '_Insert] ON [' + @Owner + '].[' + @TableName + '] FOR INSERT AS BEGIN SET NOCOUNT ON; INSERT INTO [' + @TableName + @AuditNameExtension + '](' +  @ListOfFields + 'AuditAction) SELECT ' + @ListOfFields + '''I'' FROM Inserted END')
	EXEC ('CREATE TRIGGER [tr_' + @TableName + '_Update] ON [' + @Owner + '].[' + @TableName + '] FOR UPDATE AS BEGIN SET NOCOUNT ON; INSERT INTO [' + @TableName + @AuditNameExtension + '](' +  @ListOfFields + 'AuditAction) SELECT ' + @ListOfFields + '''U'' FROM Inserted END')
	EXEC ('CREATE TRIGGER [tr_' + @TableName + '_Delete] ON [' + @Owner + '].[' + @TableName + '] FOR DELETE AS BEGIN SET NOCOUNT ON; INSERT INTO [' + @TableName + @AuditNameExtension + '](' +  @ListOfFields + 'AuditAction) SELECT ' + @ListOfFields + '''D'' FROM Deleted END')
END