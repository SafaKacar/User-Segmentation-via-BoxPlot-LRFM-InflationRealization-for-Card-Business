USE [DWH_Segmentation]
GO
/****** Object:  StoredProcedure [dbo].[spUserSegmentation]    Script Date: 4/20/2024 1:25:05 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [dbo].[spUserSegmentation] (@BaseDay_Dummy date, @Period as tinyint) as

declare
	 @BaseDay  as date  = cast(dateadd(month,datediff(month,0,@BaseDay_Dummy),0) as date)
	,@BaseDay1  as date  = cast(dateadd(month,-1,dateadd(month,datediff(month,0,@BaseDay_Dummy),0)) as date)
	,@StartDate as date

declare
	 @Param_FirstTxDate	  as date = '2013-01-01'
	,@Param_Indicator	  as date = dateadd(month,-1, @BaseDay)				
	,@Param_3Indicator	  as date = dateadd(month,-3, @BaseDay)
	,@Param_6Indicator	  as date = dateadd(month,-6, @BaseDay)
	,@Param_9Indicator	  as date = dateadd(month,-9, @BaseDay)
	,@Param_12Indicator	  as date = dateadd(month,-12,@BaseDay)
	
	if @Period = 0
		begin
		set @StartDate = @Param_FirstTxDate
		end
	else if @Period = 1
		begin
		set @StartDate = @Param_Indicator
		end	
	else if @Period = 3
		begin
		set @StartDate = @Param_3Indicator
		end
	else if @Period = 6
		begin
		set @StartDate = @Param_6Indicator
		end
	else if @Period = 9
		begin
		set @StartDate = @Param_9Indicator
		end
	else if @Period = 12
		begin
		set @StartDate = @Param_12Indicator
		end

drop table if exists #DailyUSDTRYExchangeRates
select distinct [Date], Price_USDTRY
into #DailyUSDTRYExchangeRates
from DWH_.dbo.DIM_BIExchanges with (nolock) where [Date]<'2024-02-06'
union all
select distinct Tarih [Date],([USD-A]+[USD-S])/2 Price_USDTRY from dwh_.dbo.DIM_DailyCurrency with (nolock) where Tarih>='2024-02-06'

drop table if exists [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable
select
	1 as 'Key'
	,@BaseDay 'CreateDate'
	,@BaseDay1 'YearMonth'
	,@Period 'Period'

	,l.CustomerKey
	--,case when u.IsPassiveAccount=1 then 0 else 1 end 'ActiveAccount'
	,max(cast(u.CreateDate as date)) 'RegisterDate'
	,iif(@Period in (1,3,6,9,12),null,cast(datediff(day,min(u.CreateDate),min(l.MinCreateDate))+1 as int)) 'RegisterDateToFirstTxDate'
	,min(l.MinCreateDate) 'FirstTxDate'
	,max(l.MaxCreateDate) 'LastTxDate'

	,cast(datediff(day,min(l.MinCreateDate),max(l.MaxCreateDate))+1 as int) 'Length'
	,cast(datediff(day,max(l.MaxCreateDate),@BaseDay) as int)-1 'Recency'
	,sum(l.IdCount)	'Frequency'
	,cast(sum(l.SumAmount) as decimal(18,10)) 'Monetary'
	,cast(sum(l.SumAmountUSD) as decimal(18,10)) 'MonetaryUSD'

	,coalesce(sum(l.SumAmount)/(nullif((sum(l.IdCount)*(1.0)),0)),0) 'AvgTicketSize'
	,coalesce(sum(l.SumAmountUSD)/(nullif((sum(l.IdCount)*(1.0)),0)),0) 'AvgTicketSizeUSD'

	,coalesce(sum(l.SumAmount)/(nullif((datediff(day,min(l.MinCreateDate),max(l.MaxCreateDate   ))+1),0)),0) 'AvgTxVolumePerDay' --Ortalama günlük işlem hacmi
	,coalesce(sum(l.SumAmountUSD)/(nullif((datediff(day,min(l.MinCreateDate),max(l.MaxCreateDate))+1),0)),0) 'AvgTxVolumePerDayUSD'

	,coalesce((sum(l.IdCount)*(1.0))/(nullif((datediff(day,min(l.MinCreateDate),max(l.MaxCreateDate))+1*(1.0)),0)),0) 'AvgTxCountPerDay'  --Ortalama günlük işlem sayısı

	,sum(l.CountDistinctDate) 'DistinctTxDayCount' --İşlem yapılan gün sayısı
	,coalesce(((datediff(day,min(l.MinCreateDate),max(l.MaxCreateDate))+1)*(1.0))/(nullif(sum(l.CountDistinctDate)*(1.0),0)),0) 'TxRangeOfDaysFrequency' --Ortalama kaç günde bir işlem yapıldığı
	
	--,sum(l.CountDistinctDate)*1.0 / cast(datediff(day,min(l.MinCreateDate),max(l.MaxCreateDate))+1 as int) ActiveDayCountRateOverLength

into [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable
from (
	select
		l.CustomerKey
		,count(case when l.IsCancellation=0 and l.OperatorKey is null then l.Id else null end) 'IdCount'
		,count(distinct cast((case when l.IsCancellation=0 and l.OperatorKey is null then l.CreateDate else null end) as date)) 'CountDistinctDate'
		,cast(min(case when l.IsCancellation=0 and l.OperatorKey is null then l.CreateDate else null end) as date) 'MinCreateDate'
		,cast(max(case when l.IsCancellation=0 and l.OperatorKey is null then l.CreateDate else null end) as date) 'MaxCreateDate'
		,sum(l.Amount)*(-1.0) 'SumAmount'
		,sum(l.Amount/er.Price_USDTRY)*(-1.0) 'SumAmountUSD'
	from DWH_.dbo.FACT_Transactions (nolock) l 
	left join #DailyUSDTRYExchangeRates (nolock) er on er.[Date]=cast(l.CreateDate as date)
	where l.FeatureType=2 and l.CardTxType=1 and l.CreateDate>=@StartDate and l.CreateDate<@BaseDay 
	group by l.CustomerKey
	having count(case when l.IsCancellation=0 and l.OperatorKey is null then l.Id else null end)>0
	union all

	select
		l.CustomerKey
		,count(case when l.IsCancellation=0 and l.OperatorKey is null then l.Id else null end) 'IdCount'
		,count(distinct cast((case when l.IsCancellation=0 and l.OperatorKey is null then l.CreateDate else null end) as date)) 'CountDistinctDate'
		,cast(min(case when l.IsCancellation=0 and l.OperatorKey is null then l.CreateDate else null end) as date) 'MinCreateDate'
		,cast(max(case when l.IsCancellation=0 and l.OperatorKey is null then l.CreateDate else null end) as date) 'MaxCreateDate'
		,sum(l.Amount)*(-1.0) 'SumAmount'
		,sum(l.Amount/er.Price_USDTRY)*(-1.0) 'SumAmountUSD'
	from Transactions2020Before.dbo.FACT_Transactions (nolock) l
	left join #DailyUSDTRYExchangeRates (nolock) er on er.[Date]=cast(l.CreateDate as date)
	where l.FeatureType=2 and l.CardTxType=1 and l.CreateDate>=@StartDate and l.CreateDate<@BaseDay
	group by l.CustomerKey
	having count(case when l.IsCancellation=0 and l.OperatorKey is null then l.Id else null end)>0
) l
inner join DWH_.dbo.DIM_Users (nolock) u on u.User_Key=l.CustomerKey
group by l.CustomerKey

drop table if exists #DailyUSDTRYExchangeRates
--LENGTH-----------------------------------------------------------------------------------------------------------------------------------

drop table if exists #length_temp
select distinct [Length]
into #length_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where [Length]>1

drop table if exists #length_outlier
select distinct
     1 as 'Key'
	,floor(PERCENTILE_CONT(0.05) within group (order by [Length]) over ()) 'LowerBound'
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by [Length]) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.95) within group (order by [Length]) over ()) 'UpperBound'
into #length_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock)
where [Length]>1

drop table if exists #length_points
select
	x.[Length]
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'LengthPoints'
into #length_points
from (
	select
		[Length]
		,row_number() over (order by [Length]) 'RN'
		,count(*) over () 'TotalCount'
	from #length_temp, #length_outlier
	where [Length]>=LowerBound and [Length]<=UpperBound
) x

drop table if exists #length
select
	[Length]
	,ntile(3) over (order by [Length])+1 'LengthScore'
	,ntile(2) over (order by [Length])-1 'LengthGroup'
into #length
from #length_temp, #length_outlier
where [Length]>=LowerBound and [Length]<=UpperBound

drop table if exists #length_temp


--RECENCY-----------------------------------------------------------------------------------------------------------------------------------

drop table if exists #recency_temp
select distinct Recency
into #recency_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where Recency>3 and Recency<121

drop table if exists #recency_outlier
select distinct
     1 as 'Key'
	,floor(PERCENTILE_CONT(0.05) within group (order by Recency) over ()) 'LowerBound'
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by Recency) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.95) within group (order by Recency) over ()) 'UpperBound'
into #recency_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock)
where Recency>3 and Recency<121

drop table if exists #recency_points
select
	x.Recency
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'RecencyPoints'
into #recency_points
from (
	select
		Recency
		,row_number() over (order by Recency desc) 'RN'
		,count(*) over () 'TotalCount'
	from #recency_temp, #recency_outlier
	where Recency>=LowerBound and Recency<=UpperBound
) x

drop table if exists #recency
select
	Recency
	,ntile(3) over (order by Recency desc)+1 'RecencyScore'
	,ntile(2) over (order by Recency desc)-1 'RecencyGroup'
into #recency
from #recency_temp, #recency_outlier
where Recency>=LowerBound and Recency<=UpperBound

drop table if exists #recency_temp

--FREQUENCY-----------------------------------------------------------------------------------------------------------------------------------

drop table if exists #frequency_temp
select distinct Frequency
into #frequency_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where Frequency>2

drop table if exists #frequency_outlier
select distinct
     1 as 'Key'
	,floor(PERCENTILE_CONT(0.05) within group (order by Frequency) over ()) 'LowerBound'
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by Frequency) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.95) within group (order by Frequency) over ()) 'UpperBound'
into #frequency_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where Frequency>2

drop table if exists #frequency_points
select
	x.Frequency
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'FrequencyPoints'
into #frequency_points
from (
	select
		Frequency
		,row_number() over (order by Frequency) 'RN'
		,count(*) over () 'TotalCount'
	from #frequency_temp, #frequency_outlier
	where Frequency>=LowerBound and Frequency<=UpperBound
) x

drop table if exists #frequency
select
	Frequency
	,ntile(3) over (order by Frequency)+1 'FrequencyScore'
	,ntile(2) over (order by Frequency)-1 'FrequencyGroup'
into #frequency
from #frequency_temp, #frequency_outlier
where Frequency>=LowerBound and Frequency<=UpperBound

drop table if exists #frequency_temp


--MONETARY-----------------------------------------------------------------------------------------------------------------------------------

--Monetary
drop table if exists #monetary_temp
select distinct cast(Monetary as int) 'MonetaryINT'
into #monetary_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where Monetary>=50

drop table if exists #monetary_outlier
select distinct
     1 as 'Key'
	,floor(PERCENTILE_CONT(0.05) within group (order by cast(Monetary as int)) over ()) 'LowerBound'
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by cast(Monetary as int)) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.95) within group (order by cast(Monetary as int)) over ()) 'UpperBound'
into #monetary_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where Monetary>=50

drop table if exists #monetary_points
select
	x.MonetaryINT
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'MonetaryPoints'
into #monetary_points
from (
	select
		MonetaryINT
		,row_number() over (order by MonetaryINT) 'RN'
		,count(*) over () 'TotalCount'
	from #monetary_temp, #monetary_outlier
	where MonetaryINT>=LowerBound and MonetaryINT<=UpperBound
) x

drop table if exists #monetary
select
	MonetaryINT
	,ntile(3) over (order by MonetaryINT)+1 'MonetaryScore'
	,ntile(2) over (order by MonetaryINT)-1 'MonetaryGroup'
into #monetary
from #monetary_temp, #monetary_outlier
where MonetaryINT>=LowerBound and MonetaryINT<=UpperBound

drop table if exists #monetary_temp

--MonetaryUSD
drop table if exists #monetary_usd_temp
select distinct cast(MonetaryUSD as int) 'MonetaryUSDINT'
into #monetary_usd_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where Monetary>50

drop table if exists #monetary_usd_outlier
select distinct
     1 as 'Key'
	,floor(PERCENTILE_CONT(0.05) within group (order by cast(MonetaryUSD as int)) over ()) 'LowerBound'
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by cast(MonetaryUSD as int)) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.95) within group (order by cast(MonetaryUSD as int)) over ()) 'UpperBound'
into #monetary_usd_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where Monetary>50

drop table if exists #monetary_usd_points
select
	x.MonetaryUSDINT
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'MonetaryUSDPoints'
into #monetary_usd_points
from (
	select
		MonetaryUSDINT
		,row_number() over (order by MonetaryUSDINT) 'RN'
		,count(*) over () 'TotalCount'
	from #monetary_usd_temp, #monetary_usd_outlier
	where MonetaryUSDINT>=LowerBound and MonetaryUSDINT<=UpperBound
) x

drop table if exists #monetary_usd
select
	MonetaryUSDINT
	,ntile(3) over (order by MonetaryUSDINT)+1 'MonetaryUSDScore'
	,ntile(2) over (order by MonetaryUSDINT)-1 'MonetaryUSDGroup'
into #monetary_usd
from #monetary_usd_temp, #monetary_usd_outlier
where MonetaryUSDINT>=LowerBound and MonetaryUSDINT<=UpperBound

drop table if exists #monetary_usd_temp

--AVG TICKET SIZE-----------------------------------------------------------------------------------------------------------------------------------

--AvgTicketSize
drop table if exists #avgticketsize_temp
select distinct cast(AvgTicketSize as int) 'AvgTicketSizeINT'
into #avgticketsize_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where AvgTicketSize>=10

drop table if exists #avgticketsize_outlier
select distinct
     1 as 'Key'
	,floor(PERCENTILE_CONT(0.05) within group (order by cast(AvgTicketSize as int)) over ()) 'LowerBound'
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by cast(AvgTicketSize as int)) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.95) within group (order by cast(AvgTicketSize as int)) over ()) 'UpperBound'
into #avgticketsize_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where AvgTicketSize>=10

drop table if exists #avgticketsize_points
select
	x.AvgTicketSizeINT
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'AvgTicketSizePoints'
into #avgticketsize_points
from (
	select
		AvgTicketSizeINT
		,row_number() over (order by AvgTicketSizeINT) 'RN'
		,count(*) over () 'TotalCount'
	from #avgticketsize_temp, #avgticketsize_outlier
	where AvgTicketSizeINT>=LowerBound and AvgTicketSizeINT<=UpperBound
) x

drop table if exists #avgticketsize
select
	AvgTicketSizeINT
	,ntile(3) over (order by AvgTicketSizeINT)+1 'AvgTicketSizeScore'
	,ntile(2) over (order by AvgTicketSizeINT)-1 'AvgTicketSizeGroup'
into #avgticketsize
from #avgticketsize_temp, #avgticketsize_outlier
where AvgTicketSizeINT>=LowerBound and AvgTicketSizeINT<=UpperBound

drop table if exists #avgticketsize_temp

--AvgTicketSizeUSD
drop table if exists #avgticketsize_usd_temp
select distinct cast(AvgTicketSizeUSD as int) 'AvgTicketSizeUSDINT'
into #avgticketsize_usd_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where AvgTicketSize>=10

drop table if exists #avgticketsize_usd_outlier
select distinct
     1 as 'Key'
	,floor(PERCENTILE_CONT(0.05) within group (order by cast(AvgTicketSizeUSD as int)) over ()) 'LowerBound'
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by cast(AvgTicketSizeUSD as int)) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.95) within group (order by cast(AvgTicketSizeUSD as int)) over ()) 'UpperBound'
into #avgticketsize_usd_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where AvgTicketSize>=10

drop table if exists #avgticketsize_usd_points
select
	x.AvgTicketSizeUSDINT
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'AvgTicketSizeUSDPoints'
into #avgticketsize_usd_points
from (
	select
		AvgTicketSizeUSDINT
		,row_number() over (order by AvgTicketSizeUSDINT) 'RN'
		,count(*) over () 'TotalCount'
	from #avgticketsize_usd_temp, #avgticketsize_usd_outlier
	where AvgTicketSizeUSDINT>=LowerBound and AvgTicketSizeUSDINT<=UpperBound
) x

drop table if exists #avgticketsize_usd
select
	AvgTicketSizeUSDINT
	,ntile(3) over (order by AvgTicketSizeUSDINT)+1 'AvgTicketSizeUSDScore'
	,ntile(2) over (order by AvgTicketSizeUSDINT)-1 'AvgTicketSizeUSDGroup'
into #avgticketsize_usd
from #avgticketsize_usd_temp, #avgticketsize_usd_outlier
where AvgTicketSizeUSDINT>=LowerBound and AvgTicketSizeUSDINT<=UpperBound

drop table if exists #avgticketsize_usd_temp

--AVG TX VOLUME PER DAY-----------------------------------------------------------------------------------------------------------------------------------

--AvgTxVolumePerDay
drop table if exists #avgtxvolumeperday_temp
select distinct cast(AvgTxVolumePerDay as int) 'AvgTxVolumePerDayINT'
into #avgtxvolumeperday_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where AvgTxVolumePerDay>=0.15

drop table if exists #avgtxvolumeperday_outlier
select distinct
    1 as 'Key'
	,floor(PERCENTILE_CONT(0.05) within group (order by cast(AvgTxVolumePerDay as int)) over ()) 'LowerBound'
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by cast(AvgTxVolumePerDay as int)) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.95) within group (order by cast(AvgTxVolumePerDay as int)) over ()) 'UpperBound'
into #avgtxvolumeperday_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where AvgTxVolumePerDay>=0.15

drop table if exists #avgtxvolumeperday_points
select
	x.AvgTxVolumePerDayINT
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'AvgTxVolumePerDayPoints'
into #avgtxvolumeperday_points
from (
	select
		AvgTxVolumePerDayINT
		,row_number() over (order by AvgTxVolumePerDayINT) 'RN'
		,count(*) over () 'TotalCount'
	from #avgtxvolumeperday_temp, #avgtxvolumeperday_outlier
	where AvgTxVolumePerDayINT>=LowerBound and AvgTxVolumePerDayINT<=UpperBound
) x

drop table if exists #avgtxvolumeperday
select
	AvgTxVolumePerDayINT
	,ntile(3) over (order by AvgTxVolumePerDayINT)+1 'AvgTxVolumePerDayScore'
	,ntile(2) over (order by AvgTxVolumePerDayINT)-1 'AvgTxVolumePerDayGroup'
into #avgtxvolumeperday
from #avgtxvolumeperday_temp, #avgtxvolumeperday_outlier
where AvgTxVolumePerDayINT>=LowerBound and AvgTxVolumePerDayINT<=UpperBound

drop table if exists #avgtxvolumeperday_temp

--AvgTicketSizePerDayUSD
drop table if exists #avgtxvolumeperday_usd_temp
select distinct cast(AvgTxVolumePerDayUSD as int) 'AvgTxVolumePerDayUSDINT'
into #avgtxvolumeperday_usd_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where AvgTxVolumePerDay>=0.015

drop table if exists #avgtxvolumeperday_usd_outlier
select distinct
     1 as 'Key'
	,floor(PERCENTILE_CONT(0.01) within group (order by cast(AvgTxVolumePerDayUSD as int)) over ()) 'LowerBound' ---------------------------------------
	--,ceiling(PERCENTILE_CONT(0.50) within group (order by cast(AvgTxVolumePerDayUSD as int)) over ()) 'MiddlePoint'
	,ceiling(PERCENTILE_CONT(0.99) within group (order by cast(AvgTxVolumePerDayUSD as int)) over ()) 'UpperBound' -------------------------------------
into #avgtxvolumeperday_usd_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
where AvgTxVolumePerDay>=0.015

drop table if exists #avgtxvolumeperday_usd_points
select
	x.AvgTxVolumePerDayUSDINT
	,cast(round(1.05+((x.RN-1)*(99.95-1.05)/(x.TotalCount-1)),2) as decimal(18,2)) 'AvgTxVolumePerDayUSDPoints'
into #avgtxvolumeperday_usd_points
from (
	select
		AvgTxVolumePerDayUSDINT
		,row_number() over (order by AvgTxVolumePerDayUSDINT) 'RN'
		,count(*) over () 'TotalCount'
	from #avgtxvolumeperday_usd_temp, #avgtxvolumeperday_usd_outlier
	where AvgTxVolumePerDayUSDINT>=LowerBound and AvgTxVolumePerDayUSDINT<=UpperBound
) x

drop table if exists #avgtxvolumeperday_usd
select
	AvgTxVolumePerDayUSDINT
	,ntile(3) over (order by AvgTxVolumePerDayUSDINT)+1 'AvgTxVolumePerDayUSDScore'
	,ntile(2) over (order by AvgTxVolumePerDayUSDINT)-1 'AvgTxVolumePerDayUSDGroup'
into #avgtxvolumeperday_usd
from #avgtxvolumeperday_usd_temp, #avgtxvolumeperday_usd_outlier
where AvgTxVolumePerDayUSDINT>=LowerBound and AvgTxVolumePerDayUSDINT<=UpperBound

drop table if exists #avgtxvolumeperday_usd_temp

-----------------------------------------------------------------------------------------

drop table if exists [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2
select
	t.*

	--LRFM (0/1)
	,case when l.LengthGroup is null and t.[Length]<lo.LowerBound then 0
		  when l.LengthGroup is null and t.[Length]>lo.UpperBound then 1
		  else l.LengthGroup end 'LengthGroup'
	,case when r.RecencyGroup is null and t.Recency<ro.LowerBound then 1
		  when r.RecencyGroup is null and t.Recency>ro.UpperBound then 0
		  else r.RecencyGroup end 'RecencyGroup'
	,case when f.FrequencyGroup is null and t.Frequency<fo.LowerBound then 0
		  when f.FrequencyGroup is null and t.Frequency>fo.UpperBound then 1
		  else f.FrequencyGroup end 'FrequencyGroup'
	,case when m.MonetaryGroup is null and cast(t.Monetary as int)<mo.LowerBound then 0
		  when m.MonetaryGroup is null and cast(t.Monetary as int)>mo.UpperBound then 1
		  else m.MonetaryGroup end 'MonetaryGroup'
	,case when mu.MonetaryUSDGroup is null and cast(t.MonetaryUSD as int)<muo.LowerBound then 0
		  when mu.MonetaryUSDGroup is null and cast(t.MonetaryUSD as int)>muo.UpperBound then 1
		  else mu.MonetaryUSDGroup end 'MonetaryUSDGroup'
	,case when ts.AvgTicketSizeGroup is null and cast(t.AvgTicketSize as int)<tso.LowerBound then 0
		  when ts.AvgTicketSizeGroup is null and cast(t.AvgTicketSize as int)>tso.UpperBound then 1
		  else ts.AvgTicketSizeGroup end 'AvgTicketSizeGroup'
	,case when tsu.AvgTicketSizeUSDGroup is null and cast(t.AvgTicketSizeUSD as int)<tsuo.LowerBound then 0
		  when tsu.AvgTicketSizeUSDGroup is null and cast(t.AvgTicketSizeUSD as int)>tsuo.UpperBound then 1
		  else tsu.AvgTicketSizeUSDGroup end 'AvgTicketSizeUSDGroup'
	,case when vpd.AvgTxVolumePerDayGroup is null and cast(t.AvgTxVolumePerDay as int)<vpdo.LowerBound then 0
		  when vpd.AvgTxVolumePerDayGroup is null and cast(t.AvgTxVolumePerDay as int)>vpdo.UpperBound then 1
		  else vpd.AvgTxVolumePerDayGroup end 'AvgTxVolumePerDayGroup'
	,case when vpdu.AvgTxVolumePerDayUSDGroup is null and cast(t.AvgTxVolumePerDayUSD as int)<vpduo.LowerBound then 0
		  when vpdu.AvgTxVolumePerDayUSDGroup is null and cast(t.AvgTxVolumePerDayUSD as int)>vpduo.UpperBound then 1
		  else vpdu.AvgTxVolumePerDayUSDGroup end 'AvgTxVolumePerDayUSDGroup'
	
	--LRFM (1-5)
	
	,case when l.LengthScore is null and t.[Length]<lo.LowerBound then 1
		  when l.LengthScore is null and t.[Length]>lo.UpperBound then 5
		  else l.LengthScore end 'LengthScore'
	,case when r.RecencyScore is null and t.Recency<ro.LowerBound then 5
		  when r.RecencyScore is null and t.Recency>ro.UpperBound then 1
		  else r.RecencyScore end 'RecencyScore'
	,case when f.FrequencyScore is null and t.Frequency<fo.LowerBound then 1
		  when f.FrequencyScore is null and t.Frequency>fo.UpperBound then 5
		  else f.FrequencyScore end 'FrequencyScore'
	,case when m.MonetaryScore is null and cast(t.Monetary as int)<mo.LowerBound then 1
		  when m.MonetaryScore is null and cast(t.Monetary as int)>mo.UpperBound then 5
		  else m.MonetaryScore end 'MonetaryScore'
	,case when mu.MonetaryUSDScore is null and cast(t.MonetaryUSD as int)<muo.LowerBound then 1
		  when mu.MonetaryUSDScore is null and cast(t.MonetaryUSD as int)>muo.UpperBound then 5
		  else mu.MonetaryUSDScore end 'MonetaryUSDScore'
	,case when ts.AvgTicketSizeScore is null and cast(t.AvgTicketSize as int)<tso.LowerBound then 1
		  when ts.AvgTicketSizeScore is null and cast(t.AvgTicketSize as int)>tso.UpperBound then 5
		  else ts.AvgTicketSizeScore end 'AvgTicketSizeScore'
	,case when tsu.AvgTicketSizeUSDScore is null and cast(t.AvgTicketSizeUSD as int)<tsuo.LowerBound then 1
		  when tsu.AvgTicketSizeUSDScore is null and cast(t.AvgTicketSizeUSD as int)>tsuo.UpperBound then 5
		  else tsu.AvgTicketSizeUSDScore end 'AvgTicketSizeUSDScore'
	,case when vpd.AvgTxVolumePerDayScore is null and cast(t.AvgTxVolumePerDay as int)<vpdo.LowerBound then 1
		  when vpd.AvgTxVolumePerDayScore is null and cast(t.AvgTxVolumePerDay as int)>vpdo.UpperBound then 5
		  else vpd.AvgTxVolumePerDayScore end 'AvgTxVolumePerDayScore'
	,case when vpdu.AvgTxVolumePerDayUSDScore is null and cast(t.AvgTxVolumePerDayUSD as int)<vpduo.LowerBound then 1
		  when vpdu.AvgTxVolumePerDayUSDScore is null and cast(t.AvgTxVolumePerDayUSD as int)>vpduo.UpperBound then 5
		  else vpdu.AvgTxVolumePerDayUSDScore end 'AvgTxVolumePerDayUSDScore'

	--LRFM (0-100)
	,case when lp.LengthPoints is null and t.[Length]<lo.LowerBound then 1.00
		  when lp.LengthPoints is null and t.[Length]>lo.UpperBound then 100.00
		  else lp.LengthPoints end 'LengthPoints'
	,case when rp.RecencyPoints is null and t.Recency<ro.LowerBound then 100.00
		  when rp.RecencyPoints is null and t.Recency>ro.UpperBound then 1.00
		  else rp.RecencyPoints end 'RecencyPoints'
	,case when fp.FrequencyPoints is null and t.Frequency<fo.LowerBound then 1.00
		  when fp.FrequencyPoints is null and t.Frequency>fo.UpperBound then 100.00
		  else fp.FrequencyPoints end 'FrequencyPoints'
	,case when mp.MonetaryPoints is null and cast(t.Monetary as int)<mo.LowerBound then 1.00
		  when mp.MonetaryPoints is null and cast(t.Monetary as int)>mo.UpperBound then 100.00
		  else mp.MonetaryPoints end 'MonetaryPoints'
	,case when mup.MonetaryUSDPoints is null and cast(t.MonetaryUSD as int)<muo.LowerBound then 1.00
		  when mup.MonetaryUSDPoints is null and cast(t.MonetaryUSD as int)>muo.UpperBound then 100.00
		  else mup.MonetaryUSDPoints end 'MonetaryUSDPoints'
	,case when tsp.AvgTicketSizePoints is null and cast(t.AvgTicketSize as int)<tso.LowerBound then 1.00
		  when tsp.AvgTicketSizePoints is null and cast(t.AvgTicketSize as int)>tso.UpperBound then 100.00
		  else tsp.AvgTicketSizePoints end 'AvgTicketSizePoints'
	,case when tsup.AvgTicketSizeUSDPoints is null and cast(t.AvgTicketSizeUSD as int)<tsuo.LowerBound then 1.00
		  when tsup.AvgTicketSizeUSDPoints is null and cast(t.AvgTicketSizeUSD as int)>tsuo.UpperBound then 100.00
		  else tsup.AvgTicketSizeUSDPoints end 'AvgTicketSizeUSDPoints'
	,case when vpdp.AvgTxVolumePerDayPoints is null and cast(t.AvgTxVolumePerDay as int)<vpdo.LowerBound then 1.00
		  when vpdp.AvgTxVolumePerDayPoints is null and cast(t.AvgTxVolumePerDay as int)>vpdo.UpperBound then 100.00
		  else vpdp.AvgTxVolumePerDayPoints end 'AvgTxVolumePerDayPoints'
	,case when vpdup.AvgTxVolumePerDayUSDPoints is null and cast(t.AvgTxVolumePerDayUSD as int)<vpduo.LowerBound then 1.00
		  when vpdup.AvgTxVolumePerDayUSDPoints is null and cast(t.AvgTxVolumePerDayUSD as int)>vpduo.UpperBound then 100.00
		  else vpdup.AvgTxVolumePerDayUSDPoints end 'AvgTxVolumePerDayUSDPoints'

into [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable (nolock) t
left join #length (nolock) l on l.[Length]=t.[Length]
left join #length_points (nolock) lp on lp.[Length]=t.[Length]
left join #length_outlier (nolock) lo on lo.[Key]=t.[Key]

left join #recency (nolock) r on r.Recency=t.Recency
left join #recency_points (nolock) rp on rp.Recency=t.Recency
left join #recency_outlier (nolock) ro on ro.[Key]=t.[Key]

left join #frequency (nolock) f on f.Frequency=t.Frequency
left join #frequency_points (nolock) fp on fp.Frequency=t.Frequency
left join #frequency_outlier (nolock) fo on fo.[Key]=t.[Key]

left join #monetary (nolock) m on m.MonetaryINT=cast(t.Monetary as int)
left join #monetary_points (nolock) mp on mp.MonetaryINT=cast(t.Monetary as int)
left join #monetary_outlier (nolock) mo on mo.[Key]=t.[Key]

left join #monetary_usd (nolock) mu on mu.MonetaryUSDINT=cast(t.MonetaryUSD as int)
left join #monetary_usd_points (nolock) mup on mup.MonetaryUSDINT=cast(t.MonetaryUSD as int)
left join #monetary_usd_outlier (nolock) muo on muo.[Key]=t.[Key]

left join #avgticketsize (nolock) ts on ts.AvgTicketSizeINT=cast(t.AvgTicketSize as int)
left join #avgticketsize_points (nolock) tsp on tsp.AvgTicketSizeINT=cast(t.AvgTicketSize as int)
left join #avgticketsize_outlier (nolock) tso on tso.[Key]=t.[Key]

left join #avgticketsize_usd (nolock) tsu on tsu.AvgTicketSizeUSDINT=cast(t.AvgTicketSizeUSD as int)
left join #avgticketsize_usd_points (nolock) tsup on tsup.AvgTicketSizeUSDINT=cast(t.AvgTicketSizeUSD as int)
left join #avgticketsize_usd_outlier (nolock) tsuo on tsuo.[Key]=t.[Key]

left join #avgtxvolumeperday (nolock) vpd on vpd.AvgTxVolumePerDayINT=cast(t.AvgTxVolumePerDay as int)
left join #avgtxvolumeperday_points (nolock) vpdp on vpdp.AvgTxVolumePerDayINT=cast(t.AvgTxVolumePerDay as int)
left join #avgtxvolumeperday_outlier (nolock) vpdo on vpdo.[Key]=t.[Key]

left join #avgtxvolumeperday_usd (nolock) vpdu on vpdu.AvgTxVolumePerDayUSDINT=cast(t.AvgTxVolumePerDayUSD as int)
left join #avgtxvolumeperday_usd_points (nolock) vpdup on vpdup.AvgTxVolumePerDayUSDINT=cast(t.AvgTxVolumePerDayUSD as int)
left join #avgtxvolumeperday_usd_outlier (nolock) vpduo on vpduo.[Key]=t.[Key]

drop table if exists
#length, #length_points, #length_outlier,
#recency, #recency_points, #recency_outlier,
#frequency, #frequency_points, #frequency_outlier,
#monetary, #monetary_points, #monetary_outlier,
#monetary_usd, #monetary_usd_points, #monetary_usd_outlier,
#avgticketsize, #avgticketsize_points, #avgticketsize_outlier,
#avgticketsize_usd, #avgticketsize_usd_points, #avgticketsize_usd_outlier,
#avgtxvolumeperday, #avgtxvolumeperday_points, #avgtxvolumeperday_outlier,
#avgtxvolumeperday_usd, #avgtxvolumeperday_usd_points, #avgtxvolumeperday_usd_outlier

drop table if exists [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable

--ELITE SEGMENT

--MONETARY-----------------------------------------------------------------------------------------------------------------------------------

--Monetary
drop table if exists #elite_monetary_temp
select distinct cast(Monetary as int) 'MonetaryINT'
into #elite_monetary_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where MonetaryScore=5

drop table if exists #elite_monetary_outlier
select distinct
     1 as 'Key'
	,ceiling(PERCENTILE_CONT(0.99) within group (order by cast(Monetary as int)) over ()) 'UpperBound'
into #elite_monetary_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where MonetaryScore=5

drop table if exists #elite_monetary_points
select
	x.MonetaryINT
	,cast(round(1.00+((x.RN-1)*(99.95-1.00)/(x.TotalCount-1)),2) as decimal(18,2)) 'MonetaryPoints'
into #elite_monetary_points
from (
	select
		MonetaryINT
		,row_number() over (order by MonetaryINT) 'RN'
		,count(*) over () 'TotalCount'
	from #elite_monetary_temp, #elite_monetary_outlier
	where MonetaryINT<=UpperBound
) x

drop table if exists #elite_monetary
select
	MonetaryINT
	,ntile(3) over (order by MonetaryINT) 'MonetaryScore'
into #elite_monetary
from #elite_monetary_temp, #elite_monetary_outlier
where MonetaryINT<=UpperBound

drop table if exists #elite_monetary_temp

--MonetaryUSD
drop table if exists #elite_monetary_usd_temp
select distinct cast(MonetaryUSD as int) 'MonetaryUSDINT'
into #elite_monetary_usd_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where MonetaryScore=5

drop table if exists #elite_monetary_usd_outlier
select distinct
     1 as 'Key'
	,ceiling(PERCENTILE_CONT(0.99) within group (order by cast(MonetaryUSD as int)) over ()) 'UpperBound'
into #elite_monetary_usd_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where MonetaryScore=5

drop table if exists #elite_monetary_usd_points
select
	x.MonetaryUSDINT
	,cast(round(1.00+((x.RN-1)*(99.95-1.00)/(x.TotalCount-1)),2) as decimal(18,2)) 'MonetaryUSDPoints'
into #elite_monetary_usd_points
from (
	select
		MonetaryUSDINT
		,row_number() over (order by MonetaryUSDINT) 'RN'
		,count(*) over () 'TotalCount'
	from #elite_monetary_usd_temp, #elite_monetary_usd_outlier
	where MonetaryUSDINT<=UpperBound
) x

drop table if exists #elite_monetary_usd
select
	MonetaryUSDINT
	,ntile(3) over (order by MonetaryUSDINT) 'MonetaryUSDScore'
into #elite_monetary_usd
from #elite_monetary_usd_temp, #elite_monetary_usd_outlier
where MonetaryUSDINT<=UpperBound

drop table if exists #elite_monetary_usd_temp

--AvgTicketSize
drop table if exists #elite_avgticketsize_temp
select distinct cast(AvgTicketSize as int) 'AvgTicketSizeINT'
into #elite_avgticketsize_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where AvgTicketSizeScore=5

drop table if exists #elite_avgticketsize_outlier
select distinct
     1 as 'Key'
	,ceiling(PERCENTILE_CONT(0.99) within group (order by cast(AvgTicketSize as int)) over ()) 'UpperBound'
into #elite_avgticketsize_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where AvgTicketSizeScore=5

drop table if exists #elite_avgticketsize_points
select
	x.AvgTicketSizeINT
	,cast(round(1.00+((x.RN-1)*(99.95-1.00)/(x.TotalCount-1)),2) as decimal(18,2)) 'AvgTicketSizePoints'
into #elite_avgticketsize_points
from (
	select
		AvgTicketSizeINT
		,row_number() over (order by AvgTicketSizeINT) 'RN'
		,count(*) over () 'TotalCount'
	from #elite_avgticketsize_temp, #elite_avgticketsize_outlier
	where AvgTicketSizeINT<=UpperBound
) x

drop table if exists #elite_avgticketsize
select
	AvgTicketSizeINT
	,ntile(3) over (order by AvgTicketSizeINT) 'AvgTicketSizeScore'
into #elite_avgticketsize
from #elite_avgticketsize_temp, #elite_avgticketsize_outlier
where AvgTicketSizeINT<=UpperBound

drop table if exists #elite_avgticketsize_temp

--AvgTicketSizeUSD
drop table if exists #elite_avgticketsize_usd_temp
select distinct cast(AvgTicketSizeUSD as int) 'AvgTicketSizeUSDINT'
into #elite_avgticketsize_usd_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where AvgTicketSizeUSDScore=5

drop table if exists #elite_avgticketsize_usd_outlier
select distinct
     1 as 'Key'
	,ceiling(PERCENTILE_CONT(0.99) within group (order by cast(AvgTicketSizeUSD as int)) over ()) 'UpperBound'
into #elite_avgticketsize_usd_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where AvgTicketSizeUSDScore=5

drop table if exists #elite_avgticketsize_usd_points
select
	x.AvgTicketSizeUSDINT
	,cast(round(1.00+((x.RN-1)*(99.95-1.00)/(x.TotalCount-1)),2) as decimal(18,2)) 'AvgTicketSizeUSDPoints'
into #elite_avgticketsize_usd_points
from (
	select
		AvgTicketSizeUSDINT
		,row_number() over (order by AvgTicketSizeUSDINT) 'RN'
		,count(*) over () 'TotalCount'
	from #elite_avgticketsize_usd_temp, #elite_avgticketsize_usd_outlier
	where AvgTicketSizeUSDINT<=UpperBound
) x

drop table if exists #elite_avgticketsize_usd
select
	AvgTicketSizeUSDINT
	,ntile(3) over (order by AvgTicketSizeUSDINT) 'AvgTicketSizeUSDScore'
into #elite_avgticketsize_usd
from #elite_avgticketsize_usd_temp, #elite_avgticketsize_outlier
where AvgTicketSizeUSDINT<=UpperBound

drop table if exists #elite_avgticketsize_usd_temp

--AvgTxVolumePerDay
drop table if exists #elite_avgtxvolumeperday_temp
select distinct cast(AvgTxVolumePerDay as int) 'AvgTxVolumePerDayINT'
into #elite_avgtxvolumeperday_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where AvgTxVolumePerDayScore=5

drop table if exists #elite_avgtxvolumeperday_outlier
select distinct
     1 as 'Key'
	,ceiling(PERCENTILE_CONT(0.99) within group (order by cast(AvgTxVolumePerDay as int)) over ()) 'UpperBound'
into #elite_avgtxvolumeperday_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where AvgTxVolumePerDayScore=5

drop table if exists #elite_avgtxvolumeperday_points
select
	x.AvgTxVolumePerDayINT
	,cast(round(1.00+((x.RN-1)*(99.95-1.00)/(x.TotalCount-1)),2) as decimal(18,2)) 'AvgTxVolumePerDayPoints'
into #elite_avgtxvolumeperday_points
from (
	select
		AvgTxVolumePerDayINT
		,row_number() over (order by AvgTxVolumePerDayINT) 'RN'
		,count(*) over () 'TotalCount'
	from #elite_avgtxvolumeperday_temp, #elite_avgtxvolumeperday_outlier
	where AvgTxVolumePerDayINT<=UpperBound
) x

drop table if exists #elite_avgtxvolumeperday
select
	AvgTxVolumePerDayINT
	,ntile(3) over (order by AvgTxVolumePerDayINT) 'AvgTxVolumePerDayScore'
into #elite_avgtxvolumeperday
from #elite_avgtxvolumeperday_temp, #elite_avgtxvolumeperday_outlier
where AvgTxVolumePerDayINT<=UpperBound

drop table if exists #elite_avgtxvolumeperday_temp

--AvgTxVolumePerDayUSD
drop table if exists #elite_avgtxvolumeperday_usd_temp
select distinct cast(AvgTxVolumePerDay as int) 'AvgTxVolumePerDayUSDINT'
into #elite_avgtxvolumeperday_usd_temp
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where AvgTxVolumePerDayUSDScore=5

drop table if exists #elite_avgtxvolumeperday_usd_outlier
select distinct
     1 as 'Key'
	,ceiling(PERCENTILE_CONT(0.99) within group (order by cast(AvgTxVolumePerDayUSD as int)) over ()) 'UpperBound'
into #elite_avgtxvolumeperday_usd_outlier
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
where AvgTxVolumePerDayUSDScore=5

drop table if exists #elite_avgtxvolumeperday_usd_points
select
	x.AvgTxVolumePerDayUSDINT
	,cast(round(1.00+((x.RN-1)*(99.95-1.00)/(x.TotalCount-1)),2) as decimal(18,2)) 'AvgTxVolumePerDayUSDPoints'
into #elite_avgtxvolumeperday_usd_points
from (
	select
		AvgTxVolumePerDayUSDINT
		,row_number() over (order by AvgTxVolumePerDayUSDINT) 'RN'
		,count(*) over () 'TotalCount'
	from #elite_avgtxvolumeperday_usd_temp, #elite_avgtxvolumeperday_usd_outlier
	where AvgTxVolumePerDayUSDINT<=UpperBound
) x

drop table if exists #elite_avgtxvolumeperday_usd
select
	AvgTxVolumePerDayUSDINT
	,ntile(3) over (order by AvgTxVolumePerDayUSDINT) 'AvgTxVolumePerDayUSDScore'
into #elite_avgtxvolumeperday_usd
from #elite_avgtxvolumeperday_usd_temp, #elite_avgtxvolumeperday_usd_outlier
where AvgTxVolumePerDayUSDINT<=UpperBound

drop table if exists #elite_avgtxvolumeperday_usd_temp

----------------------------------------------------------------------------------------------------------------------

drop table if exists [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable3
select
	t.*
	,case when t.MonetaryScore=5 and m.MonetaryScore is null and cast(t.Monetary as int)>mo.UpperBound then 4
		  when t.MonetaryScore=5 and m.MonetaryScore is not null then m.MonetaryScore
		  else 0 end 'EliteSegmentScore_Monetary'	
	,case when t.MonetaryUSDScore=5 and mu.MonetaryUSDScore is null and cast(t.MonetaryUSD as int)>muo.UpperBound then 4
		  when t.MonetaryUSDScore=5 and mu.MonetaryUSDScore is not null then mu.MonetaryUSDScore
		  else 0 end 'EliteSegmentScore_MonetaryUSD'
	,case when t.MonetaryScore=5 and ts.AvgTicketSizeScore is null and cast(t.AvgTicketSize as int)>tso.UpperBound then 4
		  when t.MonetaryScore=5 and ts.AvgTicketSizeScore is not null then ts.AvgTicketSizeScore
		  else 0 end 'EliteSegmentScore_AvgTicketSize'	
	,case when t.MonetaryScore=5 and tsu.AvgTicketSizeUSDScore is null and cast(t.AvgTicketSizeUSD as int)>tsuo.UpperBound then 4
		  when t.MonetaryScore=5 and tsu.AvgTicketSizeUSDScore is not null then tsu.AvgTicketSizeUSDScore
		  else 0 end 'EliteSegmentScore_AvgTicketSizeUSD'
	,case when t.MonetaryScore=5 and vpd.AvgTxVolumePerDayScore is null and cast(t.AvgTxVolumePerDay as int)>vpdo.UpperBound then 4
		  when t.MonetaryScore=5 and vpd.AvgTxVolumePerDayScore is not null then vpd.AvgTxVolumePerDayScore
		  else 0 end 'EliteSegmentScore_AvgTxVolumePerDay'
	,case when t.MonetaryScore=5 and vpdu.AvgTxVolumePerDayUSDScore is null and cast(t.AvgTxVolumePerDayUSD as int)>vpduo.UpperBound then 4
		  when t.MonetaryScore=5 and vpdu.AvgTxVolumePerDayUSDScore is not null then vpdu.AvgTxVolumePerDayUSDScore
		  else 0 end 'EliteSegmentScore_AvgTxVolumePerDayUSD'

	,case when t.MonetaryScore=5 and mp.MonetaryPoints is null and cast(t.Monetary as int)>mo.UpperBound then 100.00
		  when t.MonetaryScore=5 and mp.MonetaryPoints is not null then mp.MonetaryPoints
		  else 0 end 'EliteSegmentPoints_Monetary'	
	,case when t.MonetaryUSDScore=5 and mup.MonetaryUSDPoints is null and cast(t.MonetaryUSD as int)>muo.UpperBound then 100.00
		  when t.MonetaryUSDScore=5 and mup.MonetaryUSDPoints is not null then mup.MonetaryUSDPoints
		  else 0 end 'EliteSegmentPoints_MonetaryUSD'
	,case when t.MonetaryScore=5 and tsp.AvgTicketSizePoints is null and cast(t.AvgTicketSize as int)>tso.UpperBound then 100.00
		  when t.MonetaryScore=5 and tsp.AvgTicketSizePoints is not null then tsp.AvgTicketSizePoints
		  else 0 end 'EliteSegmentPoints_AvgTicketSize'	
	,case when t.MonetaryScore=5 and tsup.AvgTicketSizeUSDPoints is null and cast(t.AvgTicketSizeUSD as int)>tsuo.UpperBound then 100.00
		  when t.MonetaryScore=5 and tsup.AvgTicketSizeUSDPoints is not null then tsup.AvgTicketSizeUSDPoints
		  else 0 end 'EliteSegmentPoints_AvgTicketSizeUSD'
	,case when t.MonetaryScore=5 and vpdp.AvgTxVolumePerDayPoints is null and cast(t.AvgTxVolumePerDay as int)>vpdo.UpperBound then 100.00
		  when t.MonetaryScore=5 and vpdp.AvgTxVolumePerDayPoints is not null then vpdp.AvgTxVolumePerDayPoints
		  else 0 end 'EliteSegmentPoints_AvgTxVolumePerDay'

	,case when t.MonetaryScore=5 and vpdup.AvgTxVolumePerDayUSDPoints is null and cast(t.AvgTxVolumePerDayUSD as int)>vpduo.UpperBound then 100.00
		  when t.MonetaryScore=5 and vpdup.AvgTxVolumePerDayUSDPoints is not null then vpdup.AvgTxVolumePerDayUSDPoints
		  else 0 end 'EliteSegmentPoints_AvgTxVolumePerDayUSD'

into [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable3
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2 (nolock) t
left join #elite_monetary (nolock) m on m.MonetaryINT=cast(t.Monetary as int)
left join #elite_monetary_points (nolock) mp on mp.MonetaryINT=cast(t.Monetary as int)
left join #elite_monetary_outlier (nolock) mo on mo.[Key]=t.[Key]

left join #elite_monetary_usd (nolock) mu on mu.MonetaryUSDINT=cast(t.MonetaryUSD as int)
left join #elite_monetary_usd_points (nolock) mup on mup.MonetaryUSDINT=cast(t.MonetaryUSD as int)
left join #elite_monetary_usd_outlier (nolock) muo on muo.[Key]=t.[Key]

left join #elite_avgticketsize (nolock) ts on ts.AvgTicketSizeINT=cast(t.AvgTicketSize as int)
left join #elite_avgticketsize_points (nolock) tsp on tsp.AvgTicketSizeINT=cast(t.AvgTicketSize as int)
left join #elite_avgticketsize_outlier (nolock) tso on tso.[Key]=t.[Key]

left join #elite_avgticketsize_usd (nolock) tsu on tsu.AvgTicketSizeUSDINT=cast(t.AvgTicketSizeUSD as int)
left join #elite_avgticketsize_usd_points (nolock) tsup on tsup.AvgTicketSizeUSDINT=cast(t.AvgTicketSizeUSD as int)
left join #elite_avgticketsize_usd_outlier (nolock) tsuo on tsuo.[Key]=t.[Key]

left join #elite_avgtxvolumeperday (nolock) vpd on vpd.AvgTxVolumePerDayINT=cast(t.AvgTxVolumePerDay as int)
left join #elite_avgtxvolumeperday_points (nolock) vpdp on vpdp.AvgTxVolumePerDayINT=cast(t.AvgTxVolumePerDay as int)
left join #elite_avgtxvolumeperday_outlier (nolock) vpdo on vpdo.[Key]=t.[Key]

left join #elite_avgtxvolumeperday_usd (nolock) vpdu on vpdu.AvgTxVolumePerDayUSDINT=cast(t.AvgTxVolumePerDayUSD as int)
left join #elite_avgtxvolumeperday_usd_points (nolock) vpdup on vpdup.AvgTxVolumePerDayUSDINT=cast(t.AvgTxVolumePerDayUSD as int)
left join #elite_avgtxvolumeperday_usd_outlier (nolock) vpduo on vpduo.[Key]=t.[Key]

drop table if exists
#elite_monetary, #elite_monetary_points, #elite_monetary_outlier,
#elite_monetary_usd, #elite_monetary_usd_points, #elite_monetary_usd_outlier,
#elite_avgticketsize, #elite_avgticketsize_points, #elite_avgticketsize_outlier,
#elite_avgticketsize_usd, #elite_avgticketsize_usd_points, #elite_avgticketsize_usd_outlier,
#elite_avgtxvolumeperday, #elite_avgtxvolumeperday_points, #elite_avgtxvolumeperday_outlier,
#elite_avgtxvolumeperday_usd, #elite_avgtxvolumeperday_usd_points, #elite_avgtxvolumeperday_usd_outlier

drop table if exists [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable2

drop table if exists [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable4
select
	t.CreateDate
	,t.YearMonth
	,t.[Period]
	,0 as 'Currency'
	,t.CustomerKey
	,t.RegisterDate
	,t.RegisterDateToFirstTxDate
	,t.FirstTxDate
	,t.LastTxDate
	,t.[Length]
	,t.Recency
	,t.Frequency
	,t.Monetary
	,t.AvgTicketSize
	,t.AvgTxVolumePerDay
	,t.AvgTxCountPerDay
	,t.DistinctTxDayCount
	,t.TxRangeOfDaysFrequency
	,t.LengthGroup 'L'
	,t.RecencyGroup 'R'
	,t.FrequencyGroup 'F'
	,t.MonetaryGroup 'M'
	,t.AvgTicketSizeGroup 'TicketSize'
	,t.AvgTxVolumePerDayGroup 'VolumePerDay'
	,concat(cast(t.LengthGroup as char(1)),cast(t.RecencyGroup as char(1))) 'LR'
	,concat(cast(t.FrequencyGroup as char(1)),cast(t.MonetaryGroup as char(1))) 'FM'
	,concat(cast(t.LengthGroup as char(1)),cast(t.RecencyGroup as char(1)),cast(t.FrequencyGroup as char(1)),cast(t.MonetaryGroup as char(1))) 'LRFM'
	,concat(cast(t.FrequencyGroup as char(1)),cast(t.AvgTicketSizeGroup as char(1))) 'FM_ByTicketSize'
	,concat(cast(t.LengthGroup as char(1)),cast(t.RecencyGroup as char(1)),cast(t.FrequencyGroup as char(1)),cast(t.AvgTicketSizeGroup as char(1))) 'LRFM_ByTicketSize'
	,concat(cast(t.FrequencyGroup as char(1)),cast(t.AvgTxVolumePerDayGroup as char(1))) 'FM_ByVolumePerDay'
	,concat(cast(t.LengthGroup as char(1)),cast(t.RecencyGroup as char(1)),cast(t.FrequencyGroup as char(1)),cast(t.AvgTxVolumePerDayGroup as char(1))) 'LRFM_ByVolumePerDay'
	,t.LengthScore
	,t.RecencyScore
	,t.FrequencyScore
	,t.MonetaryScore
	,t.AvgTicketSizeScore
	,t.AvgTxVolumePerDayScore
	,concat(cast(t.RecencyScore as char(1)),cast(t.FrequencyScore as char(1)),cast(t.MonetaryScore as char(1))) 'RFM_Score'
	,concat(cast(t.RecencyScore as char(1)),cast(t.FrequencyScore as char(1)),cast(t.AvgTicketSizeScore as char(1))) 'RFM_Score_ByTicketSize'
	,concat(cast(t.RecencyScore as char(1)),cast(t.FrequencyScore as char(1)),cast(t.AvgTxVolumePerDayScore as char(1))) 'RFM_Score_ByVolumePerDay'
	,t.LengthPoints
	,t.RecencyPoints
	,t.FrequencyPoints
	,t.MonetaryPoints
	,t.AvgTicketSizePoints
	,t.AvgTxVolumePerDayPoints
	,t.EliteSegmentScore_Monetary 'ESByMonetary'
	,t.EliteSegmentScore_AvgTicketSize 'ESByAvgTicketSize'
	,t.EliteSegmentScore_AvgTxVolumePerDay 'ESByAvgTxVolumePerDay'
	,t.EliteSegmentPoints_Monetary 'ESPointsByMonetary'
	,t.EliteSegmentPoints_AvgTicketSize 'ESPointsByAvgTicketSize'
	,t.EliteSegmentPoints_AvgTxVolumePerDay 'ESPointsByAvgTxVolumePerDay'

into [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable4
from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable3 (nolock) t

union all

select
	t.CreateDate
	,t.YearMonth
	,t.[Period]
	,1 as 'Currency'
	,t.CustomerKey
	,t.RegisterDate
	,t.RegisterDateToFirstTxDate
	,t.FirstTxDate
	,t.LastTxDate
	,t.[Length]
	,t.Recency
	,t.Frequency
	,t.MonetaryUSD 'Monetary'
	,t.AvgTicketSizeUSD 'AvgTicketSize'
	,t.AvgTxVolumePerDayUSD 'AvgTxVolumePerDay'
	,t.AvgTxCountPerDay
	,t.DistinctTxDayCount
	,t.TxRangeOfDaysFrequency
	,t.LengthGroup 'L'
	,t.RecencyGroup 'R'
	,t.FrequencyGroup 'F'
	,t.MonetaryUSDGroup 'M'
	,t.AvgTicketSizeUSDGroup 'TicketSize'
	,t.AvgTxVolumePerDayUSDGroup 'VolumePerDay'
	,concat(cast(t.LengthGroup as char(1)),cast(t.RecencyGroup as char(1))) 'LR'
	,concat(cast(t.FrequencyGroup as char(1)),cast(t.MonetaryUSDGroup as char(1))) 'FM'
	,concat(cast(t.LengthGroup as char(1)),cast(t.RecencyGroup as char(1)),cast(t.FrequencyGroup as char(1)),cast(t.MonetaryUSDGroup as char(1))) 'LRFM'
	,concat(cast(t.FrequencyGroup as char(1)),cast(t.AvgTicketSizeUSDGroup as char(1))) 'FM_ByTicketSize'
	,concat(cast(t.LengthGroup as char(1)),cast(t.RecencyGroup as char(1)),cast(t.FrequencyGroup as char(1)),cast(t.AvgTicketSizeUSDGroup as char(1))) 'LRFM_ByTicketSize'
	,concat(cast(t.FrequencyGroup as char(1)),cast(t.AvgTxVolumePerDayUSDGroup as char(1))) 'FM_ByVolumePerDay'
	,concat(cast(t.LengthGroup as char(1)),cast(t.RecencyGroup as char(1)),cast(t.FrequencyGroup as char(1)),cast(t.AvgTxVolumePerDayUSDGroup as char(1))) 'LRFM_ByVolumePerDay'
	,t.LengthScore
	,t.RecencyScore
	,t.FrequencyScore
	,t.MonetaryUSDScore 'MonetaryScore'
	,t.AvgTicketSizeUSDScore 'AvgTicketSizeScore'
	,t.AvgTxVolumePerDayUSDScore 'AvgTxVolumePerDayScore'
	,concat(cast(t.RecencyScore as char(1)),cast(t.FrequencyScore as char(1)),cast(t.MonetaryUSDScore as char(1))) 'RFM_Score'
	,concat(cast(t.RecencyScore as char(1)),cast(t.FrequencyScore as char(1)),cast(t.AvgTicketSizeUSDScore as char(1))) 'RFM_Score_ByTicketSize'
	,concat(cast(t.RecencyScore as char(1)),cast(t.FrequencyScore as char(1)),cast(t.AvgTxVolumePerDayUSDScore as char(1))) 'RFM_Score_ByVolumePerDay'
	,t.LengthPoints
	,t.RecencyPoints
	,t.FrequencyPoints
	,t.MonetaryUSDPoints 'MonetaryPoints'
	,t.AvgTicketSizeUSDPoints 'AvgTicketSizePoints'
	,t.AvgTxVolumePerDayUSDPoints 'AvgTxVolumePerDayPoints'
	,t.EliteSegmentScore_MonetaryUSD 'ESByMonetary'
	,t.EliteSegmentScore_AvgTicketSizeUSD 'ESByAvgTicketSize'
	,t.EliteSegmentScore_AvgTxVolumePerDayUSD 'ESByAvgTxVolumePerDay'
	,t.EliteSegmentPoints_MonetaryUSD 'ESPointsByMonetary'
	,t.EliteSegmentPoints_AvgTicketSizeUSD 'ESPointsByAvgTicketSize'
	,t.EliteSegmentPoints_AvgTxVolumePerDayUSD 'ESPointsByAvgTxVolumePerDay'

from [DWH_Segmentation].[DBA_skacar].UserSegmantationFictDroppableTable3 (nolock) t