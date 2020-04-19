--UPDATED CODE


--filter grouteres, gparty
grouteres = grouteres.\
    filter((col('data_date') > begin_yyyymmdd)
           & (col('rtargetagentselected').isNotNull())
           & (col('gsys_ext_vch2').isNotNull())).\
    withColumn('ranker',
               row_number().
               over(Window.
                    partitionBy(grouteres.callid).
                    orderBy(grouteres.id.desc(),grouteres.created_ts.desc())
                    )
               )

gparty = gparty.\
    filter((col('data_date') == '20200409')
           & (col('agentid') > 0)
           & (col('parentlinktype') == 4)).\
    withColumn('ranker',
               row_number().
               over(Window.
                    partitionBy(gparty.callid).
                    orderBy(gparty.ccevent.asc(),gparty.created_ts.desc())
                    )
               )

bstphn = clntcal.\
    filter(col('data_date') > begin_yyyymmdd)

enhncpoid = irfusr.\
    join(mdsgmnt,[(mdsgmnt.ixn_resource_id == irfusr.interaction_resource_id)],'inner').\
    filter((irfusr.cal_date > begin_yyyymmdd) & (irfusr.poid.isNotNull()))





#rename
tagntsg = tagntsg.withColumnRenamed('id','tsg_id').withColumnRenamed('agentid','tsg_agentid')

routeres = routeres.withColumnRenamed('callid','rtres_callid')

h1_rigsgmnt = rigsgmnt.withColumnRenamed('segment','h1_segment').withColumnRenamed('name','h1_name').withColumnRenamed('type','h1_type').withColumnRenamed('department','h1_department').withColumnRenamed('startdate','h1_startdate').withColumnRenamed('enddate','h1_enddate')

h2_rigsgmnt = rigsgmnt.withColumnRenamed('segment','h2_segment').withColumnRenamed('name','h2_name').withColumnRenamed('type','h2_type').withColumnRenamed('department','h2_department').withColumnRenamed('startdate','h2_startdate').withColumnRenamed('enddate','h2_enddate')

gcagnt = gcagnt.withColumnRenamed('id','gagn_id').withColumnRenamed('created','gagn_created').withColumnRenamed('deleted','gagn_deleted')




-- Routeres dataframe

routeres = grouteres.\
    join(gcxgrpendpnt,
         [(grouteres.gsys_ext_vch2 == gcxgrpendpnt.endpointid)
          & (grouteres.created_ts.between(gcxgrpendpnt.created_ts,coalesce(gcxgrpendpnt.deleted_ts,lit('9999999999'))))
          ], 'inner').\
    join(gcgrp,
         [(gcxgrpendpnt.groupid == gcgrp.id)
          & (grouteres.created_ts.between(gcgrp.created_ts,coalesce(gcgrp.deleted_ts,lit('9999999999'))))
          ], 'inner').\
    filter(grouteres.ranker == 1).\
    select(col('callid'),col('name').alias('queue'))

gparty = gparty.\
    filter(col('ranker') == 1).\
    select(col('callid').alias('gp_callid'),
           col('agentid').alias('gp_agentid'),
           col('id').alias('gp_id'))

bestphone = bstphn.\
    groupby('rootinteractionid').\
    agg({'callfrom':'max'}).\
    withColumnRenamed('''max(callfrom)''',"best_callfrom").\
    select(col('rootinteractionid').alias('bstphn_rootinteractionid'),col('best_callfrom'))

enhancepoid = enhncpoid.\
    groupby('media_server_ixn_guid').\
    agg({'poid':'max'}).\
    withColumnRenamed('''max(poid)''',"e_poid").\
    select(col('media_server_ixn_guid'),col('e_poid')).\
    dropDuplicates()



--rmscallsbygroup

rmscallsbygroup = clntcal.\
    join(bestphone,
         [(clntcal.rootinteractionid == bestphone.bstphn_rootinteractionid)],'inner').\
    join(gparty,
         [(clntcal.callid == gparty.gp_callid)],'inner').\
    join(tagntsg,
         [(gparty.gp_id == tagntsg.tsg_id)],'inner').\
    join(routeres,
         [(clntcal.callid == routeres.rtres_callid)],'inner').\
    join(h1_rigsgmnt,
         [(tagntsg.staff_group == upper(trim(h1_rigsgmnt.h1_name)))
          & (h1_rigsgmnt.h1_type == 'SG')
          & (h1_rigsgmnt.h1_department == 'RMS')
          & (to_date(clntcal.callstarttime_est).between(h1_rigsgmnt.h1_startdate,h1_rigsgmnt.h1_enddate))
          ],'inner').\
    join(h2_rigsgmnt,
         [(upper(trim(routeres.queue)) == upper(trim(h2_rigsgmnt.h2_name)))
          & (h2_rigsgmnt.h2_type == 'Queue')
          & (to_date(clntcal.callstarttime_est).between(h2_rigsgmnt.h2_startdate,h2_rigsgmnt.h2_enddate))
          ],'inner').\
    join(gcagnt,
         [(gparty.gp_agentid == gcagnt.gagn_id)
          & (gcagnt.gagn_created <= clntcal.callstarttime_est)
          & ((gcagnt.gagn_deleted >= clntcal.callstarttime_est) | (gcagnt.gagn_deleted.isNull()))
          ],'left').\
    join(enhancepoid,[(clntcal.callid == enhancepoid.media_server_ixn_guid)],'left').\
    filter(clntcal.data_date > begin_yyyymmdd).\
    withColumn('uid',upper(trim(col('username')))).\
    withColumn('callstarttime_unix',unix_timestamp(col('callstarttime_est'))).\
    select(col('rootinteractionid'),
           col('callid'),
           col('callstarttime_est'),
           col('callstarttime_unix'),
           when(col('poid') > 0, col('poid')).otherwise(col('e_poid')).alias('poid'),
           col('callfrom'),
           col('calltypedesc'),
           col('gp_agentid').alias('agent_id'),
           col('uid'),
           col('staff_group'),
           col('h1_segment').alias('sg_segment'),
           col('h1_department').alias('sg_department'),
           col('queue'),
           col('h2_segment').alias('queue_segment'),
           col('h2_department').alias('queue_department')
           )
