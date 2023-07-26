USE [msdb]
GO

/****** Object:  Job [PartitionJob]    Script Date: 27/02/2021 11:27:38 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 27/02/2021 11:27:38 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'PartitionJob', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'ESMAEILI\katy', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [PartitionStep]    Script Date: 27/02/2021 11:27:38 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'PartitionStep', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=2, 
		@retry_interval=5, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'declare @currentDate datetime 
set @currentDate=GETDATE()+7
declare @m int,@month char(2),@year char(4),@FGName CHAR(9),@DFName char(9),@path varchar(60)
set @m= month(@currentDate)
set @year=year(@currentDate)
select @month=
case 
when @m>0 and @m<=3 then ''1''
when @m>3 and @m<=6 then ''2''
when @m>6 and @m<=9 then ''3''
else ''4''
end

print @month
print @year
set @FGName=''FGP''+@year+''_''+@month
set @DFName=''DFP''+@year+''_''+@month
set @path=''G:\DataBase\GPS\''

select * from sys.filegroups where name=@FGName
--SP_HELPFileGroup PLACES
if not exists
(
select * from sys.filegroups where name=@FGName
)
begin
	exec(''ALTER DATABASE gpsdb ADD FILEGROUP ''+@FGName)
	
	
end
 SELECT 
	DB_NAME() AS [DatabaseName],
	 Name, file_id, 
	 physical_name,
	(size * 8.0/1024) as Size,
	((size * 8.0/1024) - (FILEPROPERTY(name, ''SpaceUsed'') * 8.0/1024)) As FreeSpace
From sys.database_files where name =@DFName

 if not exists
 (
 SELECT 
	DB_NAME() AS [DatabaseName],
	 Name, file_id, 
	 physical_name,
	(size * 8.0/1024) as Size,
	((size * 8.0/1024) - (FILEPROPERTY(name, ''SpaceUsed'') * 8.0/1024)) As FreeSpace
From sys.database_files where name =@DFName
)
begin 
exec(''
 ALTER DATABASE gpsdb ADD FILE
(
 NAME=''+@DFName+'',FILENAME=''''''+@path+@DFName+''.ndf '''', FILEGROWTH = 1048576KB 
 ) TO FILEGROUP  ''+ @FGName
 )
 use gpsdb
 exec(''
 ALTER PARTITION SCHEME PS_PLACES   
NEXT USED''+@FGName 
)
end

--if not exists @FGName

', 
		@database_name=N'gpsdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'PartitionSchedule', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=32, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210227, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'd94db6d2-094a-45bd-9424-9b26527dc070'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


