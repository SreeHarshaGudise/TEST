trpt_vndr_case = all_tables['']['']
client_information = all_tables['']['']

#rename Columns in tables
vendor_sales_case = trpt_vndr_case.\
    withColumnRenamed('VGI_ASSGND_CASE_ID','VGI_Assigned_Case_ID').\
    withColumnRenamed('EFFTV_BGN_TS','Case_Open_Date').\
    withColumnRenamed('CREW_OWNR_PO_ID','Owner_POID')


#Joins
pre_df = vendor_sales_case.\
    join(client_information,
         [(client_Information.CASE_RLSHP_TYP_CD == 'CNT') &
          (vendor_sales_case.CREW_OWNR_PO_ID == 1225977965)
          (vendor_sales_case.CREW_OWNR_PO_ID > 0) &
          (vendor_sales_case.CREW_OWNR_PO_ID.isNotNull()) &
          (vendor_sales_case.VNDR_CASE_TYP_CD=='SALE') &
          (Client_Information.VGI_ASSGND_CASE_ID == vendor_sales_case.VGI_Assigned_Case_ID)
          ],
         'inner'
         )

#columns
bdg_atscase = pre_df.select(
    col('VGI_Assigned_Case_ID'),
    from_unixtime(unix_timestamp(pre_df.Case_Open_Date, 'yyyy-MM-dd')).alias('Case_Open_Date'),
    when(from_unixtime(unix_timestamp(pre_df.EFFTV_END_TS, 'yyyy-MM-dd'))=='9999-12-31','null').
        otherwise(from_unixtime(unix_timestamp(pre_df.EFFTV_END_TS, 'yyyy-MM-dd'))).alias('Case_Closed_Date'),
    when(pre_df.CASE_STATUS_CD == 'OPEN','Open').when(pre_df.CASE_STATUS_CD=='CLSD','Closed').alias('Case_Status'),
    when(pre_df.CASE_STATUS_CD == 'OPEN',datediff(date.today(),from_unixtime(unix_timestamp(pre_df.Case_Open_Date, 'yyyy-MM-dd'))))
        .otherwise(datediff
                       (when(from_unixtime(unix_timestamp(pre_df.EFFTV_END_TS, 'yyyy-MM-dd'))=='9999-12-31','null'),
                        from_unixtime(unix_timestamp(pre_df.Case_Open_Date, 'yyyy-MM-dd'))
                        )
                   ).alias('CycleTime'),
    col('Owner_POID')).distinct()
