case_closed_df = filter_df_closed.\
    withColumn('Case_Close_Date',
               when(to_date(filter_df_closed.EFFTV_END_TS) == '9999-12-31','null').
               otherwise(to_date(filter_df_closed.EFFTV_END_TS,'yyyy-MM-dd'))
               ).\
    groupBy('Case_Close_Date','Owner_POID','vndr_case_id','Case_Status_Cd').count().\
    select(col('Owner_POID'),col('Case_Close_Date'),when(col('Case_Status_Cd') == 'OPEN','open').when(col('Case_Status_Cd') == 'CLSD','closed').alias('Case_Status'),col('vndr_case_id').alias('VGI_ASGND_CASE_ID'),col('count').alias('Case_close_Ats'))

ats_counts = case_closed_df.\
    join(case_opened_df,
         ['Owner_POID','VGI_Assigned_Case_ID'],
         'outer').\
    select(col('Owner_POID'),col('Case_Open_Date'),col('Case_Close_Date'),col('VGI_ASGND_CASE_ID'),col('Case_Status'))





-----------------------------------------------------------------------------------------------------------------------
VALIDATION QUERIES:
    
    
 cognos table:
    
    SELECT COUNT(*)
    ,POID
    ,VGI_ASSGND_CASE_ID
FROM profduction_table
GROUP BY POID,VGI_ASSGND_CASE_ID
order by POID,VGI_ASSGND_CASE_ID

hive/aws table:
    
    
    SELECT COUNT(*)
    ,POID
    ,VGI_ASSGND_CASE_ID
FROM (your select statement logic)
group by POID,VGI_ASSGND_CASE_ID
order by POID,VGI_ASSGND_CASE_ID



-------Check counts for a POID

SELECT COUNT(*)
    ,POID
    ,VGI_ASSGND_CASE_ID
FROM profduction_table
WHERE POID IN (list_of_poids_you_need)
GROUP BY POID,VGI_ASSGND_CASE_ID
order by POID,VGI_ASSGND_CASE_ID



 SELECT COUNT(*)
    ,POID
    ,VGI_ASSGND_CASE_ID
FROM (your select statement logic) a
WHERE a.POID in (list_of_poids_you_need)
group by POID,VGI_ASSGND_CASE_ID
order by POID,VGI_ASSGND_CASE_ID


-------Check number of VGI_ASSGND_CASE_ID's for a POID


SELECT COUNT(VGI_ASSGND_CASE_ID)
    ,POID
FROM profduction_table
WHERE POID IN (list_of_poids_you_need)
GROUP BY POID
order by POID


 SELECT COUNT(VGI_ASSGND_CASE_ID)
    ,POID
FROM (your select statement logic) a
WHERE a.POID in (list_of_poids_you_need)
group by POID
order by POID
