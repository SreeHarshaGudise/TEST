trpt_vndr_case = all_tables['']['']
client_information = all_tables['']['']

#rename Columns in tables
vendor_sales_case = trpt_vndr_case.\
    withColumnRenamed('VGI_ASSGND_CASE_ID','VGI_Assigned_Case_ID').\
    withColumnRenamed('EFFTV_BGN_TS','Case_Open_Date').\
    withColumnRenamed('CREW_OWNR_PO_ID','Owner_POID')


#Joins
pre_df = vendor_sales_case.\
    join(client_Information,
         [(client_Information.CASE_RLSHP_TYP_CD == 'CNT') &
          (vendor_sales_case.CREW_OWNR_PO_ID == 1225977965)
          (vendor_sales_case.CREW_OWNR_PO_ID > 0) &
          (vendor_sales_case.CREW_OWNR_PO_ID.isNotNull()) &
          (vendor_sales_case.VNDR_CASE_TYP_CD=='SALE') &
          (Client_Information.VGI_ASSGND_CASE_ID == vendor_sales_case.VGI_Assigned_Case_ID)
          ],
         'inner'
         )


#calculated columns
final_df = ris_df\
    .withColumn('Case_Status',when(col('CASE_STATUS_CD') == 'OPEN','Open').when(col('CASE_STATUS_CD') =='CLSD','Closed'))\
    .withColumn('CycleTime',
                when(col('CASE_STATUS_CD') == 'CLSD',
                     datediff(
                         from_unixtime(unix_timestamp(col('EFFTV_END_TS'),'MM/dd/yyyy')),
                         from_unixtime(unix_timestamp(col('Case_Open_Date'),'MM/dd/yyyy'))
                     )
                     ).otherwise(datediff(to_date(lit(date.today())),from_unixtime(unix_timestamp(col('Case_Open_Date'),'MM/dd/yyyy')))))\
    .withColumn('Case_Closed_Date',
                when(
                    to_date(from_unixtime(unix_timestamp(col('EFFTV_END_TS'),'MM/dd/yyyy'))) == '9999-12-31','null').\
                otherwise(to_date(from_unixtime(unix_timestamp(col('EFFTV_END_TS'), 'MM/dd/yyyy')))
                          )
                )


#columns
bdg_atscase = final_df.select(
    col('VGI_Assigned_Case_ID'),
    col('Case_Closed_Date'),
    col('Case_Status'),
    col('CycleTime'),
    col('Owner_POID')).distinct()



#-- Demo points
#*  The agenda would be extracting ats data which has details about cases information and its cycle time for a respective POID.
#*   In order to acquire bdg_ats  case data, following tables are needed:
#    * Client Information
#    * vndr case 
#*  We joined both tables on assigned case id column where respective crew owner POID shouldn't be null and 
#   client relationship cd should be CNT, case type be SALE.
#*  Logic for Cycletime is number of days between dates when the case has been opened and closed. If the case status 
#   is still open for the day we calculate the difference between current date and case opened date.
#*  We are extracting this ats data by implementing in NEW WAYS OF CODING technique.







Where
((cast(vendor_sales_case.EFFTV_END_TS as date) >= current_date() - INTERVAL '12' DAY - day(current_date())) OR ((CASE WHEN CAST(Vendor_sales_case.EFFTV_END_TS as date) = '9999-12-31' then null else cast(vendor_sales_Case.EFFTV_END_TS as date) end >= current_date() - INTERVAL '12' DAY - day(current_date())) and (cast(Vendor_Sales_Case.EFFTV_BGN_TS as date) >= CURRENT_DATE - INTERVAL '24' DAY) and (Client_Information.CASE_RLSHP_TYP_CD = 'CLNT') and
(Vendor_Sales_Case.CREW_OWNER_PO_ID > 0) and
(Vendor_Sales_Case.CREW_OWNER_PO_ID IS NOT NULL)
And
(Vendor_Sales_Case.VNDR_CASE_TYP_CD = 'SALE')
