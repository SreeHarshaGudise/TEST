rmscallsbygroup = \
    clntcal.\
        join(gparty,[(clntcal.callid == gparty.callid)],'inner').drop(gparty.callid).\
        join(tagntsg,[(gparty.id == tagntsg.id) & (gparty.agentid == tagntsg.agentid)],'inner').drop(tagntsg.agentid).\
        join(routeres,[(clntcal.callid == routeres.callid)],'inner').drop(routeres.callid).\
        join(rigsgmnt.alias('h1'),
             [(tagntsg.staff_group == col('h1.name'))
              & (col('h1.type') == 'SG')
              & (col('h1.department') == 'RMS')
              & to_date(clntcal.callstarttime_est).between(col('h1.startdate'),col('h1.enddate'))
              ], 'inner').\
        withColumn('h1_department',col('h1.department')).\
        withColumn('h1_segment',col('h1.segment')).\
        join(rigsgmnt.alias('h2'),
             [(routeres.queue == col('h2.name'))
              & (col('h2.type') == 'Queue')
              & (to_date(clntcal.callstarttime_est).between(col('h2.startdate'),col('h2.enddate')))
              ], 'inner').\
        withColumn('h2_segment',col('h2.segment')).\
        withColumn('h2_department',col('h2.department')).\
        join(gcagnt,
             [(gparty.agentid == gcagnt.id)
              & (gcagnt.created <= clntcal.callstarttime_est)
              & ((gcagnt.deleted >= clntcal.callstarttime_est) | (gcagnt.deleted.isNull()))
              ],'left').\
        filter((clntcal.data_date == '2019-09-05') & (gparty.data_date == '2019-09-05')).\
        withColumn('uid',upper(trim(col('username')))).\
        select(col('rootinteractionid'),
               col('callid'),
               col('callstarttime_est'),
               col('poid'),
               col('callfrom'),
               col('calltypedesc'),
               col('agentid'),
               col('uid'),
               col('staff_group'),
               col('queue'),
               col('h1_segment').alias('sg_segment'),
               col('h1_department').alias('sg_department'),
               col('h2_segment').alias('queue_segment'),
               col('h2_department').alias('queue_department')
               )
