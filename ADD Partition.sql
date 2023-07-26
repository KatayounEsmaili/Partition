declare @DBName nvarchar(30)
set @DBName='gpsdb'

exec('use '+@DBName)
declare @currentDate datetime 
set @currentDate=GETDATE()+3

print @currentDate
declare @m int,@month char(2),@year char(4),@FGName CHAR(9),@DFName char(9),@path varchar(60),@partitionrange varchar(30),@nextyear char(4)
set @m= month(@currentDate)+1
set @year=year(@currentDate)

select @month=
case 
when @m>0 and @m<=3 then '1'
when @m>3 and @m<=6 then '2'
when @m>6 and @m<=9 then '3'
else '4'
end

set @nextyear=cast ((cast(@year as int) +1) as char)

select @partitionrange=
case 
when @m>0 and @m<=3 then @year+'0401 00:00:00:000'
when @m>3 and @m<=6 then @year+'0701 00:00:00:000'
when @m>6 and @m<=9 then @year+'1001 00:00:00:000'
else  @nextyear+'0101 00:00:00:000'
end
print @partitionrange
print @month
print @year
set @FGName='FGP'+@year+'_'+@month
set @DFName='DFP'+@year+'_'+@month
set @path='G:\DataBase\GPS\'

--select * from sys.filegroups where name=@FGName
--SP_HELPFileGroup PLACES
if not exists
(
select * from sys.filegroups where name=@FGName
)
begin
	exec('ALTER DATABASE gpsdb ADD FILEGROUP '+@FGName)	
	
end
 

 if not exists
 (
 SELECT 
	DB_NAME() AS [DatabaseName],
	 Name, file_id, 
	 physical_name,
	(size * 8.0/1024) as Size,
	((size * 8.0/1024) - (FILEPROPERTY(name, 'SpaceUsed') * 8.0/1024)) As FreeSpace
From sys.database_files where name =@DFName
)
begin 
	exec('
	 ALTER DATABASE gpsdb ADD FILE
	(
	 NAME='+@DFName+',FILENAME='''+@path+@DFName+'.ndf '', FILEGROWTH = 1048576KB 
	 ) TO FILEGROUP  '+ @FGName+'
 

	 use '+@DBName+'

 
	 ALTER PARTITION SCHEME PS_PLACES NEXT USED '+@FGName +'


 

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
		ALTER PARTITION FUNCTION PF_PLACES() SPLIT RANGE ('+@partitionrange+');
')
end



