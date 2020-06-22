ris_interim_df = trpt_vndr_clnt.join(trpt_case_clnt,['VGI_ASGND_CASE_ID'],"inner")


#filters
filter_ris_df = ris_interim_df.filter(
                              (ris_interim_df.CASE_RLSHP_TYP_CD == 'CLNT')
                              &
                              (ris_interim_df.ORG_OWNR_PO_ID == 1225977965)
                              &
                              (ris_interim_df.CREW_OWNR_PO_ID > 0)
                              &
                              (ris_interim_df.CREW_OWNR_PO_ID.isNotNull())
                              )


case_opened_df = filter_ris_df.\
    withColumn('Case_Open_Date',to_date(filter_ris_df.EFFTV_BGN_TS,'yyyy-MM-dd')).\
    select(
    col('Case_Open_Date'),
    col('CREW_OWNR_PO_ID').alias('Owner_POID'),
    col('VGI_ASGND_CASE_ID')
)


# RIS CASE CLOSED

interim_ris_sum_df_closed = trpt_vndr_clnt.join(trpt_case_clnt,['VGI_ASGND_CASE_ID'],"inner")


#filters



filter_ris_df_closed = interim_ris_sum_df_closed.\
    filter((interim_ris_sum_df_closed.CASE_RLSHP_TYP_CD == 'CLNT')
           &
           (interim_ris_sum_df_closed.ORG_OWNR_PO_ID == 1225977965)
           &
           (interim_ris_sum_df_closed.CREW_OWNR_PO_ID > 0)
           &
           (interim_ris_sum_df_closed.CREW_OWNR_PO_ID.isNotNull())
           )

case_closed_df = filter_ris_df_closed.\
    withColumn('Case_Closed_Date',
               when(to_date(filter_ris_df_closed.EFFTV_END_TS) == '9999-12-31','null').
               otherwise(to_date(filter_ris_df_closed.EFFTV_END_TS,'yyyy-MM-dd'))).\
    withColumn('CycleTime',
               when(filter_ris_df_closed.CASE_STATUS_CD == 'OPEN',
                    datediff(to_date(lit(current_date())),
                             to_date(unix_timestamp(filter_ris_df_closed.EFFTV_BGN_TS,'yyyy-MM-dd').cast("timestamp"))))\
                    .otherwise(
                   datediff(
                       when(to_date(unix_timestamp(filter_ris_df_closed.EFFTV_END_TS,'yyyy-MM-dd').cast("timestamp")) == '9999-12-31','null'
                            ).otherwise(to_date(unix_timestamp(filter_ris_df_closed.EFFTV_END_TS,'yyyy-MM-dd').cast("timestamp"))),to_date(filter_ris_df_closed.EFFTV_BGN_TS,'yyyy-MM-dd'))))\
    .select(col('CREW_OWNR_PO_ID').alias('Owner_POID'),col('VGI_ASGND_CASE_ID'),when(col('CASE_STATUS_CD') == 'OPEN','open').when(col('CASE_STATUS_CD')=='CLSD','closed').alias('caseStatus'),col('Case_Closed_Date'),col('CycleTime')
            )




#final_df

final_df = case_opened_df.join(case_closed_df,['Owner_POID','VGI_ASGND_CASE_ID'],'inner')
