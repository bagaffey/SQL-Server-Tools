USE [master]
GO

/****** Object:  DdlTrigger [connection_trigger]    Script Date: 12/31/2018 10:44:16 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/*
 * <<EXCLUDED_LOGINNAMES_APPNAMES_TABLE>> Replace this with a table that contains login names and application names that
 * should be excluded from being logged.
 *      Assuming these are columns in a table that contains a login name that should be excluded from loggin and 
 *      application names that the login should be excluded from logging when they're using that app or any app.
 *		<<LOGIN_NAME>>
 *		<<APPLICATION_NAME>>
 *
 * <<LOGON_CONNECTION_LOG>> Replace this with the name of the table that will actually contain the log entries.
 *
 */
CREATE TRIGGER [logon_connection_trigger]
ON ALL SERVER
FOR LOGON
AS 
BEGIN
	SET NOCOUNT ON;

	DECLARE @DATA XML = EVENTDATA()
	DECLARE @IP VARCHAR(512) = @DATA.value('(/EVENT_INSTANCE/ClientHost)[1]', 'sysname');
	DECLARE @LOGONTYPE VARCHAR(256) = @DATA.value('(/EVENT_INSTANCE/LoginType)[1]', 'sysname');

	DECLARE @LOGGED BIT = 0;

	IF EXISTS(SELECT 1 FROM sys.dm_exec_sessions AS A
			  WHERE A.session_id = @@SPID AND 
			  ORIGINAL_LOGIN() NOT LIKE '%$' AND
			  A.is_user_process = 1 AND
			  (
				A.nt_domain = 'MAY' OR
				A.nt_domain IS NULL
			  )
			  /* Use this query to exclude certain users & apps
			  AND NOT EXISTS(SELECT 1
							 FROM <<EXCLUDED_LOGINNAMES_APPNAMES_TABLE>> AS E
							 WHERE UPPER(<<LOGIN_NAME>>) = UPPER(ORIGINAL_LOGIN()) AND 
							 (<<APPLICATION_NAME>> IS NULL OR <<APPLICATION_NAME>> = APP_NAME()))
			  */
			 ) 
	BEGIN
		SET @LOGGED = 1;
	END;

	IF @LOGGED = 1
	BEGIN
	LOG_IT_HERE:
	/* Logs the entry in a table
		INSERT INTO <<LOGON_CONNECTION_LOG>>
		([LogDateTime], [LogonType], [AppName], [ComputerName], [Login], [IPAddress], [DataDump], [SPID])
		VALUES
		(GETDATE(), @LOGONTYPE, APP_NAME(), HOST_NAME(), ORIGINAL_LOGIN(), @IP, @DATA, @@SPID);
	*/
	END;
END;
GO


