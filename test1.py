rmscallsbygroup = \
    clntcal.\
        join(gparty,[(clntcal.callid == gparty.callid)],'inner').\
        join(tagntsg,[(gparty.id == tagntsg.id) & (gparty.agentid == tagntsg.agentid)],'inner').\
        join(routeres,[(clntcal.callid == routeres.callid)],'inner').\
        join(rigsgmnt.alias('h1'), 
             [(tagntsg.staff_group == col('h1.name')) 
              & (col('h1.type') == 'SG') 
              & (col('h1.department') == 'RMS') 
              & to_date(tagntsg.callstarttime_est).between(col('h1.startdate'),col('h1.enddate'))
              ], 'inner').\
        join(rigsgmnt.alias('h2'), 
             [(routeres.queue == col('h2.name')) 
              & (col('h2.type') == 'Queue') 
              & (to_date(tagntsg.callstarttime_est).between(col('h2.startdate'),col('h2.enddate')))
              ], 'inner').\
        join(gcagnt,
             [(gparty.agentid == gcagnt.id) 
              & (gcagnt.created <= clntcal.callstarttime_est) 
              & ((gcagnt.deleted >= clntcal.callstarttime_est) | (gcagnt.deleted.isNull()))
              ],'left').\
        filter((clntcal.data_date == '20200409') & (gparty.data_date == '20200409')).\
        withColumn('uid',upper(trim(col('username')))).alias('uid').\
        select(col('rootinteractionid'),
               col('callid'),
               col('callstarttime_est'),
               col('poid'),
               col('callfrom'),
               col('calltypedesc'),
               col('agent_id'),
               col('uid'),
               col('staff_group'),
               col('queue'),
               col('h1.segment').alias('sg_segment'),
               col('h1.department').alias('sg_department'),
               col('h2.segment').alias('queue_segment'),
               col('h2.department').alias('queue_department')
               )
