--------------------------------------------------------------------------------------------------------------
Ris Case Open
--------------------------------------------------------------------------------------------------------------


ris_interim_df = trpt_vndr_clnt.join(trpt_case_clnt,['VGI_ASGND_CASE_ID'],"inner")


#filters
filter_ris_df = ris_interim_df.filter(
                              (ris_interim_df.CASE_RLSHP_TYP_CD == 'CLNT')
                              &
                              (ris_interim_df.ORG_OWNR_PO_ID = 1225977965)
                              &
                              (ris_interim_df.Owner_POID > 0)
                              &
                              (ris_interim_df.Owner_POID.isNotNull())
                              &
                              (to_date(ris_interim_df.EFFTV_BGN_TS,'yyyy-MM-dd').between('2020-05-27','2020-05-29'))
                              )


case_opened_df = filter_ris_df.\
    withColumn('Case_Open_Date',to_date(filter_ris_df.EFFTV_BGN_TS,'yyyy-MM-dd')).\
    groupBy('Case_Open_Date','CREW_OWNR_PO_ID').count().\
    select(
    col('Case_Open_Date'),
    col('CREW_OWNR_PO_ID').alias('Owner_POID'),
    col('VGI_ASGND_CASE_ID'),
    col('count').alias('Case_opened_RIS')
)


---------------------------------------------------------------------------------------------------------
## Ris Case Closed
---------------------------------------------------------------------------------------------------------


interim_ris_df_closed = trpt_vndr_clnt.join(trpt_case_clnt,['VGI_ASGND_CASE_ID'],"inner")



#filters
filter_ris_df_closed = interim_ris_df_closed.filter(
                                            (interim_df_closed.CASE_RLSHP_TYP_CD == 'CLNT')
                                            &
                                            (interim_df_closed.ORG_OWNR_PO_ID = 1225977965)
                                            &
                                            (interim_df_closed.Owner_POID > 0)
                                            &
                                            (interim_df_closed.Owner_POID.isNotNull())
                                            &
                                            (interim_df_closed.VNDR_CASE_TYP_CD == 'SALE')
                                            &
                                            (when(to_date(interim_df_closed.EFFTV_END_TS) == '9999-12-31','null').
                                             otherwise(to_date(interim_df_closed.EFFTV_END_TS)).between('2020-05-27','2020-05-09'))
                                            )



case_closed_df = filter_ris_df_closed.\
    withColumn('Case_Closed_Date',
               when(to_date(filter_ris_df_closed.EFFTV_END_TS) == '9999-12-31','null').
               otherwise(to_date(filter_ris_df_closed.EFFTV_END_TS,'yyyy-MM-dd'))
               ).\
    groupBy('Case_Closed_Date','VGI_ASGND_CASE_ID').count().\
    select(col('CREW_OWNR_PO_ID').alias('Owner_POID'),col('Case_Closed_Date'),col('count').alias('Cases_Closed_RIS'))


----------------------------------------------------------------------------------------------------------------------
# RIS CASE CLOSED SUM
----------------------------------------------------------------------------------------------------------------------

interim_ris_sum_df_closed = trpt_vndr_clnt.join(trpt_case_clnt,['VGI_ASGND_CASE_ID'],"inner")


#filters



filter_ris_df_closed = interim_ris_sum_df_closed.\
    filter(((when(to_date(interim_df_closed.EFFTV_END_TS) == '9999-12-31','null').
             otherwise(to_date(interim_df_closed.EFFTV_END_TS)) >=
             trunc(add_months(current_date(),-12),"month")
             )
            &
            (to_date(interim_df_closed.EFFTV_BGN_TS) >=
             trunc(add_months(current_date(),-24),"month"))
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
           (when(
               interim_ris_sum_df_closed.CASE_STATUS_CD == 'OPEN','Open'
           ).
            when(
               interim_ris_sum_df_closed.CASE_STATUS_CD == 'CLSD','Closed'
           ) == 'Closed'
            )
           &
           (when(
               to_date(interim_ris_sum_df_closed.EFFTV_END_TS) == '9999-12-31','null'
           ).otherwise(
               to_date(interim_ris_sum_df_closed.EFFTV_END_TS)
           ).between('2020-05-27','2020-05-29')
            )
           )

case_sum_closed_df = filter_ris_df_closed.\
    withColumn('Case_Close_Date',
               when(to_date(filter_ris_df_closed.EFFTV_END_TS) == '9999-12-31','null').
               otherwise(to_date(filter_ris_df_closed.EFFTV_END_TS,'yyyy-MM-dd'))
               ).\
    withColumn('CycleTime',when(col('CASE_STATUS_CD') == 'OPEN',datediff(to_date(lit(date.today())),from_unixtime(unix_timestamp(filter_ris_df_closed.EFFTV_BGN_TS,'MM/dd/yyyy'))))
                .otherwise(datediff
                       (when(from_unixtime(unix_timestamp(filter_ris_df_closed.EFFTV_END_TS,'MM/dd/yyyy')) == '9999-12-31','null'),
                        from_unixtime(unix_timestamp(filter_ris_df_closed.Case_Open_Date,'MM/dd/yyyy'))
                        )
                   ))
    groupBy('Case_Close_Date','CREW_OWNR_PO_ID').\
    agg(sum('CycleTime').alias('CycleTime')
        ).\
    select(
        col('CREW_OWNR_PO_ID').alias('Owner_POID'),
        col('Case_Close_Date'),col('CycleTime')
    )
