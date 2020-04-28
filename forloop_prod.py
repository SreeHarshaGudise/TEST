--- PRODUCTION SCRIPT FOR LOOP CODE



for partition in load_partition_list:
    load_df = block_df.filter(col(part_key)==partition)
    # Below line helps to debug to check how many rows for each partition date has.
    print(str(partition)+' has '+str(load_df.count()))
    location = 's3://'+ known.bucket_name + '/' + known.blockname + "/" + known.target_table_partition_key + '=' + str(partition)
    # Below line helps to debug with how does partition location looks like for each partition key
    print('partition_location :: ' + location)
    datasink, partition_location = load_layer_custom_partition(load_df,
                                                               location,
                                                               known.output_format,
                                                               known.mode,
                                                               known.block_name,
                                                               logger,
                                                               log_filepath
                                                               )
