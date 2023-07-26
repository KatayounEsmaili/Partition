USE [msdb]
GO


declare @LoginName nvarchar(50),@DBName nvarchar(30)
set @LoginName=N'ESMAEILI\katy'
set @DBName='gpsdb'


/****** Object:  Job [PartitionJob]    Script Date: 01/03/2021 14:06:38 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 01/03/2021 14:06:38 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'PartitionJob', 
		@enabled=1, 
		@notify_level_eventlog=3, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=@LoginName, @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [PartitionStep]    Script Date: 01/03/2021 14:06:38 ******/
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
set @currentDate=GETDATE()+3

print @currentDate
declare @m int,@month char(2),@year char(4),@FGName CHAR(9),@DFName char(9),@path varchar(60),@partitionrange varchar(30),@nextyear char(4)
set @m= month(@currentDate)+1
set @year=year(@currentDate)




select @month=
case 
when @m>0 and @m<=3 then ''1''
when @m>3 and @m<=6 then ''2''
when @m>6 and @m<=9 then ''3''
else ''4''
end

set @nextyear=cast ((cast(@year as int) +1) as char)

select @partitionrange=
case 
when @m>0 and @m<=3 then @year+''0401 00:00:00:000''
when @m>3 and @m<=6 then @year+''0701 00:00:00:000''
when @m>6 and @m<=9 then @year+''1001 00:00:00:000''
else  @nextyear+''0101 00:00:00:000''
end
print @partitionrange
print @month
print @year
set @FGName=''FGP''+@year+''_''+@month
set @DFName=''DFP''+@year+''_''+@month
set @path=''G:\DataBase\GPS\''


if not exists
(
select * from sys.filegroups where name=@FGName

)
begin
	exec(''ALTER DATABASE gpsdb ADD FILEGROUP ''+@FGName)	
	
end
 

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
 ALTER PARTITION SCHEME PS_PLACES NEXT USED ''+@FGName 
)
if exists
(
SELECT DISTINCT o.name as table_name, ps.name as PScheme, f.name as PFunction, rv.value as partition_range, fg.name as file_groupName, p.partition_number, p.rows as number_of_rows
FROM sys.partitions p
INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
INNER JOIN sys.objects o ON p.object_id = o.object_id
INNER JOIN sys.system_internals_allocation_units au ON p.partition_id = au.container_id
INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
INNER JOIN sys.partition_functions f ON f.function_id = ps.function_id
INNER JOIN sys.destination_data_spaces dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
LEFT OUTER JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
WHERE o.object_id = OBJECT_ID(''Places'') and rv.value is null
)
	ALTER PARTITION FUNCTION PF_PLACES() SPLIT RANGE (@partitionrange);

end



', 
		@database_name=@DBName, 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'PartitionSchedule', 
		@enabled=1, 
		@freq_type=16, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=4, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210228, 
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


