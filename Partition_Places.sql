use gpsdb
GO
 --ADD FILE GROUP FOR PARTIONINIG
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2016
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2017
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2018_1
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2018_2
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2019_1
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2019_2
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2020_1
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2020_2
ALTER DATABASE gpsdb ADD FILEGROUP FGP2020_3
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2020_4
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2021_1
  ALTER DATABASE gpsdb ADD FILEGROUP FGP2021_2
 ALTER DATABASE gpsdb ADD FILEGROUP FGP2021_3

 --ADD DATA FILE FOR PARTIONINIG

 ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2016,FILENAME='G:\DataBase\GPS\DFP2016.ndf', FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2016

 ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2017,FILENAME='G:\DataBase\GPS\DFP2017.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2017

  ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2018_1,FILENAME='G:\DataBase\GPS\DFP2018_1.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2018_1

  ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2018_2,FILENAME='G:\DataBase\GPS\DFP2018_2.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2018_2

  ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2019_1,FILENAME='G:\DataBase\GPS\DFP2019_1.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2019_1


 ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2019_2,FILENAME='G:\DataBase\GPS\DFP2019_2.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2019_2


  ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2020_1,FILENAME='G:\DataBase\GPS\DFP2020_1.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2020_1

  ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2020_2,FILENAME='G:\DataBase\GPS\DFP2020_2.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2020_2

  ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2020_3,FILENAME='G:\DataBase\GPS\DFP2020_3.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2020_3


 ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2020_4,FILENAME='G:\DataBase\GPS\DFP2020_4.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2020_4


 ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2021_1,FILENAME='G:\DataBase\GPS\DFP2021_1.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2021_1
 GO

ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2021_2,FILENAME='G:\DataBase\GPS\DFP2021_2.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2021_2
 GO

 ALTER DATABASE gpsdb ADD FILE
(
 NAME=DFP2021_3,FILENAME='G:\DataBase\GPS\DFP2021_3.ndf' , FILEGROWTH = 1048576KB 
 ) TO FILEGROUP FGP2021_3
 GO


 --10

 CREATE PARTITION FUNCTION PF_PLACES (DATETIME)
AS RANGE RIGHT
FOR VALUES('20170101 00:00:00:000',
			'20180101 00:00:00:000',
			'20180701 00:00:00:000',
			'20190101 00:00:00:000',
			'20190701 00:00:00:000',
			'20200101 00:00:00:000',
			'20200401 00:00:00:000',
			'20200701 00:00:00:000',
			'20201001 00:00:00:000',
			'20210101 00:00:00:000',
			'20210401 00:00:00:000',
			'20210701 00:00:00:000'
		  )
		  go
--drop PARTITION if exists PF_PLACES
go

create PARTITION SCHEME PS_PLACES AS PARTITION PF_PLACES
	TO (FGP2016,FGP2017,FGP2018_1,FGP2018_2,FGP2019_1,FGP2019_2,FGP2020_1,FGP2020_2,FGP2020_3,FGP2020_4,FGP2021_1,FGP2021_2,FGP2021_3)
GO	
--ALTER PARTITION SCHEME PS_PLACES   
--NEXT USED FGP2021_3



ALTER TABLE GeoFence_Places NOCHECK CONSTRAINT FK_GeoFence_Places_Places
 GO


 exec sp_spaceused GeoFence_Places


 ALTER TABLE ODBHistory NOCHECK CONSTRAINT FK_ODBHistory_Places
 GO
 
  exec sp_spaceused ODBHistory


 --ADD FILEGROUP FOR TEMP TABLE
 ALTER DATABASE gpsdb ADD FILEGROUP FGTest
GO
--ALTER DATABASE gpsdb SET MULTI_USER
ALTER DATABASE gpsdb ADD FILE
(
 NAME=DATAFileTest_import,FILENAME='G:\DataBase\GPS\DATAFileTest_import.ndf',size=25600MB--,FILEGROWTH=unlimited
 ) TO FILEGROUP FGTest

 ALTER DATABASE gpsdb ADD FILE
(
 NAME=DATAFileTest_import2,FILENAME='G:\DataBase\GPS\DATAFileTest_import2.ndf',size=25600MB--,FILEGROWTH=unlimited
 ) TO FILEGROUP FGTest
 GO


 ALTER DATABASE gpsdb SET SINGLE_USER WITH ROLLBACK IMMEDIATE
 go

 ALTER DATABASE gpsdb MODIFY FILEGROUP FGTest AUTOGROW_ALL_FILES
 GO

 ALTER DATABASE gpsdb SET MULTI_USER WITH ROLLBACK IMMEDIATE
 GO

 --CREATE TEMP TABLE AND BULK INSERT

drop table if exists  temp_places 

CREATE TABLE [dbo].[temp_places](
	[gpsID] [int] NOT NULL,
	[_date] [datetime] NOT NULL,
	[speed] [float] NULL,
	[height] [float] NULL,
	[lat] [float] NULL,
	[lng] [float] NULL,
	[alarm] [char](2) NULL,
	[temperature] [float] NULL,
	[rfid] [nvarchar](50) NULL,
	[sos] [bit] NULL,
	[input1] [bit] NULL,
	[input2] [bit] NULL,
	[input3] [bit] NULL,
	[input4] [bit] NULL,
	[output1] [bit] NULL,
	[output2] [bit] NULL,
	[output3] [bit] NULL,
	[output4] [bit] NULL,
	[batvoltage] [float] NULL,
	[involtage] [float] NULL,
	[ADC0] [float] NULL,
	[ADC1] [float] NULL,
	[eventHndID] [int] NULL,
	[fuel] [float] NULL
) ON FGTest

ALTER TABLE [temp_places] ADD CONSTRAINT PK_temp_places PRIMARY KEY CLUSTERED 
(	
	
	gpsID,
	_date
) 
go
ALTER TABLE [temp_places] REBUILD WITH (IGNORE_DUP_KEY = ON)
go 

exec SP_SPACEUSED temp_places

---Return Number of Rows in Places

DECLARE @spaceUsed TABLE (
    name varchar(255), 
    rows int, 
    reserved varchar(50), 
    data varchar(50), 
    index_size varchar(50), 
    unused varchar(50))
INSERT INTO @spaceUsed
exec SP_SPACEUSED Places 

declare @rows int
select top 1 @rows=rows from @spaceUsed
select @rows

declare @x int, @x2 int,@y int
set @y=@rows /2000000
select @y
set @x=1
set @x2=0

while(@x2<=@y)
begin
set @x=1
	while(@x<=200)
	begin
	insert into temp_places (gpsID,_date,speed,height,lat,lng,alarm,temperature,rfid,sos,input1,input2,input3,input4,output1,output2,output3,output4,batvoltage,involtage,ADC0,ADC1,eventHndID,fuel)  
	(
	select  top 10000        gpsID,_date,speed,height,lat,lng,alarm,temperature,rfid,sos,input1,input2,input3,input4,output1,output2,output3,output4,batvoltage,involtage,ADC0,ADC1,eventHndID,fuel  
	from Places 
	)
	delete top(10000) from Places 
	set @x+=1
	end

	
	set @x2+=1
	select @x2
end



--insert into  #temp_ODBHistory(pid,gpsid,Date,Value)

--(select  pid,gpsid,ODBHistory.Date,ODBHistory.Value into  #temp_ODBHistory   from ODBHistory )
--select * from #temp_ODBHistory

ALTER TABLE ODBHistory drop CONSTRAINT FK_ODBHistory_Places
go
ALTER TABLE GeoFence_Places drop CONSTRAINT FK_GeoFence_Places_Places
 GO


--Drop PK and ADD On Partition
ALTER TABLE places DROP CONSTRAINT PK_places
GO
ALTER TABLE Places ADD CONSTRAINT PK_places PRIMARY KEY CLUSTERED 
(	
	
	gpsID,
	_date
) ON PS_PLACES(_date)
go
--ALTER TABLE Places REBUILD WITH (IGNORE_DUP_KEY = ON)
go



CREATE TABLE [dbo].[temp_error](
	[gpsID] [int] NOT NULL,
	[_date] [datetime]  NOT NULL,
	[speed] [float] NULL,
	[height] [float] NULL,
	[lat] [float] NULL,
	[lng] [float] NULL,
	[alarm] [char](2) NULL,
	[temperature] [float] NULL,
	[rfid] [nvarchar](50) NULL,
	[sos] [bit] NULL,
	[input1] [bit] NULL,
	[input2] [bit] NULL,
	[input3] [bit] NULL,
	[input4] [bit] NULL,
	[output1] [bit] NULL,
	[output2] [bit] NULL,
	[output3] [bit] NULL,
	[output4] [bit] NULL,
	[batvoltage] [float] NULL,
	[involtage] [float] NULL,
	[ADC0] [float] NULL,
	[ADC1] [float] NULL,
	[eventHndID] [int] NULL,
	[fuel] [float] NULL
) 
go
ALTER TABLE [temp_error] ADD CONSTRAINT PK_temp_error PRIMARY KEY CLUSTERED 
(	
	
	gpsID,
	_date
)
go
ALTER TABLE [temp_error] REBUILD WITH (IGNORE_DUP_KEY = ON)
go
DECLARE @spaceUsed2 TABLE (
    name varchar(255), 
    rows int, 
    reserved varchar(50), 
    data varchar(50), 
    index_size varchar(50), 
    unused varchar(50))
INSERT INTO @spaceUsed2
exec SP_SPACEUSED temp_places 
declare @rows int
select top 1 @rows=rows from @spaceUsed2
select @rows

declare @x int, @x2 int,@y int
set @y=@rows /2000000
select @y
set @x=1
set @x2=0

while(@x2<=@y)
begin
set @x=1
	while(@x<=200)
	begin
		begin try
		insert into  Places(gpsID,_date,speed,height,lat,lng,alarm,temperature,rfid,sos,input1,input2,input3,input4,output1,output2,output3,output4,batvoltage,involtage,ADC0,ADC1,eventHndID,fuel)  
		(
		select  top 10000        gpsID,_date,speed,height,lat,lng,alarm,temperature,rfid,sos,input1,input2,input3,input4,output1,output2,output3,output4,batvoltage,involtage,ADC0,ADC1,eventHndID,fuel  
		from temp_places 
		)
		end try
		begin catch
		select ERROR_MESSAGE()
		insert into  temp_error(gpsID,_date,speed,height,lat,lng,alarm,temperature,rfid,sos,input1,input2,input3,input4,output1,output2,output3,output4,batvoltage,involtage,ADC0,ADC1,eventHndID,fuel)  
		(
		select  top 10000        gpsID,_date,speed,height,lat,lng,alarm,temperature,rfid,sos,input1,input2,input3,input4,output1,output2,output3,output4,batvoltage,involtage,ADC0,ADC1,eventHndID,fuel  
		from temp_places 
		)
		end catch
	delete top(10000) from temp_places 
	set @x+=1
	end

	
	set @x2+=1
	select @x2
end
go

ALTER TABLE [dbo].[GeoFence_Places]  WITH NOCHECK ADD  CONSTRAINT [FK_GeoFence_Places_Places] FOREIGN KEY([gpsID], [_date])
REFERENCES [dbo].[Places] ([gpsID], [_date])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[GeoFence_Places] CHECK CONSTRAINT [FK_GeoFence_Places_GeoFence]
GO 

ALTER TABLE [dbo].[ODBHistory]  WITH CHECK ADD  CONSTRAINT [FK_ODBHistory_Places] FOREIGN KEY([GPSId], [Date])
REFERENCES [dbo].[Places] ([gpsID], [_date])
GO

ALTER TABLE [dbo].[ODBHistory] CHECK CONSTRAINT [FK_ODBHistory_Places]
GO


drop table temp_places
drop table temp_error