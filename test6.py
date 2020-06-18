trpt_vndr_clnt = spark.read.csv("/home/harsha/Downloads/trpt_vndr_clnt.csv", inferSchema = True, header = True)
trpt_case_clnt = spark.read.csv("/home/harsha/Downloads/trpt_case_clnt.csv", inferSchema = True, header = True)


trpt_vndr_clnt = trpt_vndr_clnt.withColumnRenamed('VGI_ASGND_CASE_ID','vndr_case_id').withColumnRenamed('CREW_OWNR_PO_ID','Owner_POID')


#joins
interim_df = trpt_vndr_clnt.join(trpt_case_clnt,[(trpt_case_clnt.VGI_ASGND_CASE_ID == trpt_vndr_clnt.vndr_case_id) ],"inner")


#filter
filter_df = interim_df.filter((to_date(interim_df.EFFTV_BGN_TS) >= trunc(add_months(current_date(),-12),"month"))
                              &
                              (interim_df.CASE_RLSHP_TYP_CD == 'CLNT')
                              &
                              (interim_df.ORG_OWNR_PO_ID != 1225977965)
                              &
                              (interim_df.Owner_POID > 0)
                              &
                              (interim_df.Owner_POID.isNotNull())
                              &
                              (interim_df.VNDR_CASE_TYP_CD == 'SALE')
                              &
                              (to_date(interim_df.EFFTV_BGN_TS) == '2020-05-29')
                              & (interim_df.Owner_POID == 1519420110)
                              )


case_opened_df = filter_df.\
    withColumn('Case_Open_Date',to_date(filter_df.EFFTV_BGN_TS,'yyyy-MM-dd')).\
    groupBy('Case_Open_Date','Owner_POID','vndr_case_id').count().\
    select(col('Case_Open_Date'),col('Owner_POID'),col('vndr_case_id').alias('VGI_ASGND_CASE_ID'),col('count').alias('Case_open_Ats'))



#Case Closed Date Ats
interim_df_closed = trpt_vndr_clnt.join(trpt_case_clnt,[(trpt_case_clnt.VGI_ASGND_CASE_ID == trpt_vndr_clnt.vndr_case_id) ],"inner")


#filters
filter_df_closed = interim_df_closed.filter(((when(to_date(interim_df_closed.EFFTV_END_TS) == '9999-12-31','null')
                                              .otherwise(to_date(interim_df_closed.EFFTV_END_TS)) >=
                                              trunc(add_months(current_date(),-12),"month")
                                              )
                                             &
                                             (to_date(interim_df_closed.EFFTV_BGN_TS) >= trunc(add_months(current_date(),-24),"month"))
                                             )
                                            &
                                            (interim_df_closed.CASE_RLSHP_TYP_CD == 'CLNT')
                                            &
                                            (interim_df_closed.ORG_OWNR_PO_ID != 1225977965)
                                            &
                                            (interim_df_closed.Owner_POID > 0)
                                            &
                                            (interim_df_closed.Owner_POID.isNotNull())
                                            &
                                            (interim_df_closed.VNDR_CASE_TYP_CD == 'SALE')
                                            &
                                            (when(to_date(interim_df_closed.EFFTV_END_TS) == '9999-12-31','null').
                                             otherwise(to_date(interim_df_closed.EFFTV_END_TS)) == '2020-05-29')
                                            &
                                            (interim_df_closed.Owner_POID == 1519420110)
                                            )


case_closed_df = filter_df_closed.\
    withColumn('Case_Close_Date',
               when(to_date(filter_df_closed.EFFTV_END_TS) == '9999-12-31','null').
               otherwise(to_date(filter_df_closed.EFFTV_END_TS,'yyyy-MM-dd'))
               ).\
    groupBy('Case_Close_Date','Owner_POID','vndr_case_id').count().\
    select(col('Owner_POID'),col('Case_Close_Date'),col('vndr_case_id').alias('VGI_ASGND_CASE_ID'),col('count').alias('Case_close_Ats'))



ats_counts = case_closed_df.\
    join(case_opened_df,
         (case_closed_df.Owner_POID == case_opened_df.Owner_POID),
         'outer').\
    drop(case_closed_df.Owner_POID,case_closed_df.VGI_ASGND_CASE_ID).\
    select(col('Owner_POID'),col('Case_Open_Date'),col('VGI_ASGND_CASE_ID'),col('Case_Close_Date'),col('Case_open_Ats'),col('Case_close_Ats'))









-----------------------------------------------------------------------------------------------
Ats_counts = 
case_open_df.
join(case_closed_df,
    (case_closed_df.Owner_POID == case_open_df.Owner_POID)
    ,'inner'
    ).
withColumn(
    'vgi_assigned_case_id',
    when(case_open_df.VGI_Assigned_Case_ID.isNull(),case_close_df.VGI_Assigned_Case_ID).
    when(case_close_df.VGI_Assigned_Case_ID.isNull(),case_open_df.VGI_Assigned_Case_ID)
    ).
withColumn(
    'owner_POID',
    when(case_open_df.Owner_POID.isNull(),case_close_df.Owner_POID).
    when(case_close_df.Owner_POID.isNull(),case_open_df.Owner_POID)
    ).
select(
    col('vgi_assigned_case_id'),
    col('Case_Open_Date'),
    col('Case_Closed_Date'),
    col('owner_POID')
    )

