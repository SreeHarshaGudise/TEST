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


