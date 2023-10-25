# Python Programming
Basic Tutorial for Python Programming


    try:
        # Extracting information from configuration
        # config = get_config(dataset,"delta_load")
        current_year = date.today().strftime("%Y")
        try:
            source_bucket = config.get("src_bucket")
            if len(source_bucket) == 0:
                print(f"Error encountered in delta_load_app function. Empty key src_bucket found in config.")
                sys.exit(1)

            target_bucket = config.get("trg_bucket")
            if len(target_bucket) == 0:
                print(f"Error encountered in delta_load_app function. Empty key trg_bucket found in config.")
                sys.exit(1)

            year_list = config.get("years")
            if len(year_list) == 0:
                print(f"Error encountered in delta_load_app function. Empty key years found in config.")
                sys.exit(1)

            year = config.get("delta_load_year")
            if len(year) == 0:
                print(f"Error encountered in delta_load_app function. Empty key year found in config.")
                sys.exit(1)

            main_table = config["main_table"]
            if len(main_table) == 0:
                print(f"Error encountered in delta_load_app function. Empty key main_table found in config.")
                sys.exit(1)

            ambest_dict = config["ambest"]
            if len(ambest_dict) == 0:
                print(f"Error encountered in delta_load_app function. Empty key ambest found in config.")
                sys.exit(1)

            tables_list = config["tables_list"]
            if len(tables_list) == 0:
                print(f"Error encountered in delta_load_app function. Empty key tables_list found in config.")
                sys.exit(1)

            load_start_date = config["load_start_date"]
            if len(load_start_date) == 0:
                print(f"Error encountered in delta_load_app function. Empty key load_start_date found in config.")
                sys.exit(1)
            load_start_date_formatted = datetime.strptime(load_start_date, "%m-%d-%Y").strftime("%Y-%m-%d").replace("-", "")

            load_end_date = config["load_end_date"]
            if len(load_end_date) == 0:
                print(f"Error encountered in delta_load_app function. Empty key load_end_date found in config.")
                sys.exit(1)
            load_end_date_formatted = datetime.strptime(load_end_date, "%m-%d-%Y").strftime("%Y-%m-%d").replace("-", "")

        except Exception as e:
            print(f"Error encountered in delta_load_app function.\nError: {e}")
            log.write_log(run_id =run_id,start_time = str(datetime.now()),dataset = dataset,run_type = run_type,
            log_type = ERROR,table_name = "",  
            ambest = "",source_count = 0,target_count = 0,count_match = "",
            error_code = f"Error encountered in delta_load_cv function while reading configs",
            error_message = format_exc()            
            )
            sys.exit(1)

        source_count = 0
        total_source_count_dict = {}
        total_target_count_dict = {}
        total_source_count_dict[main_table] = 0
        total_target_count_dict[main_table] = 0
        


        # Processing all the tables
        file_paths_dict = {}
        main_table_file_paths = get_table_files_paths(dataset, main_table,config)
        file_paths_dict[main_table] = main_table_file_paths

        for table_name in tables_list:
            total_source_count_dict[table_name] = 0
            total_target_count_dict[table_name] = 0
            file_paths = get_table_files_paths(dataset, table_name,config)
            file_paths_dict[table_name] = file_paths  

        print(file_paths_dict)

        # Initializing source and destination dataframes for each table
        source_dataframes_dict = initialize_dataframes_dict(main_table, tables_list)
        target_dataframes_dict = initialize_dataframes_dict(main_table, tables_list)

        # Loading the dataframes with file paths
        load_dataframes(source_dataframes_dict, file_paths_dict, main_table, tables_list)

        # Creating temp View for the main table
        source_dataframes_dict[main_table].createOrReplaceTempView(main_table)
        #dummy
        # c1 = spark.sql(f"SELECT count(*) FROM {main_table}")
        # print("c1",c1.count())

        # Creating temp View for all other tables
        for table in tables_list:
            if not file_paths_dict[table]:
                print(f"Skipping creating temp view for {table} as no source file found.")
            else:
                source_dataframes_dict[table].createOrReplaceTempView(table)

        ambest_counter = 0

        length = len(ambest_dict)
        last_ambest_value = list(ambest_dict)[length - 1]
        print(f'Last ambest of the ambest_dict is: {last_ambest_value}')

        # list to store the data of each table count which is dictionary
        table_count_info_list = []
        ambest_dict_parallel = {}
        for ambest_value, ambest_start_date in ambest_dict.items():
            print(f'Processing the Ambest: {ambest_value} ......')

            try:
                if (ambest_value is None) or (len(ambest_value) == 0):
                    print(f"Invalid ambest value.")
                    continue
                ambest_start_date = datetime.strptime(ambest_start_date[0], "%m-%d-%Y").strftime("%Y-%m-%d")
            except Exception as e:
                print(f"Error encountered in delta_load_app function. Ambest Start Date not in standard format for {ambest_value} ambest.\nError: {e}")
                log.write_log(run_id=run_id,start_time=str(datetime.now()),dataset=dataset,run_type=run_type,
                log_type=ERROR,table_name="",ambest="",source_count=0,target_count=0,count_match="",
                error_code=f"Error encountered in delta_load_app function. Wrong ambest_start_date format for {ambest_value}.",
                error_message=format_exc()
                )

            src_main_table_df = spark.sql(f"SELECT * FROM {main_table} WHERE ambest_nbr = '{ambest_value}' AND loss_dt >= '{ambest_start_date}' AND date_insert >= '{load_start_date_formatted}' AND date_insert <= '{load_end_date_formatted}';")

            if (src_main_table_df.isEmpty()):
                print(f"Record count for the Ambest: {ambest_value} in the main_table {main_table} is: 0 and skipping other transactional tables ....")
            else:
                ambest_dict_parallel[ambest_value] = ambest_start_date
                main_table_ambest_rec_count = src_main_table_df.count()
            
                print(f"Record count for the ambest: {ambest_value} in the main_table {main_table} is: {main_table_ambest_rec_count} and processing other transactional tables")

                ambest_counter = ambest_counter + 1

        # Writing data
        if target_dataframes_dict[main_table] is not None:
            # main_table_rec_count = target_dataframes_dict[main_table].count()
            # total_source_count_dict[main_table] += main_table_rec_count
            # main_table_target_path = f"{target_bucket}/{main_table}/year={year}"
            print(f"Writing data for the table {main_table}.....")

            new_tables_list = [main_table] + tables_list
            print('---> first new_tables_list ',new_tables_list)
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                            # Start the load operations
                            future_to_tables = {executor.submit(run_notebook1,dataset,run_id,run_type,load_type,main_table,
                                                ambest_value,load_start_date_formatted,load_end_date_formatted,
                                                tables_list,file_paths_dict,target_bucket,year,
                                                ambest_counter,last_ambest_value,table_name,main_table_num,False,total_target_count_dict[table_name]):
                                                table_name for main_table_num,table_name in enumerate(new_tables_list)}
                            for future in concurrent.futures.as_completed(future_to_tables):
                                part1 = future_to_tables[future]
                                try:
                                    result_return_part1 = future.result()
                                    result_return_part1 = json.loads(result_return_part1)
                                    print("Json data",result_return_part1)
                                    ambest_counter += int(result_return_part1["ambest_counter"])
                                    src_list = list(result_return_part1["total_source_count_dict"].items())[0]
                                    src_key,src_val = src_list[0],src_list[1]
                                    test_src_dict.append(result_return_part1["total_source_count_dict"])
                                    print("total_source_count_dict before update",total_source_count_dict)
                                    total_source_count_dict[src_key] += src_val
                                    print("total_source_count_dict after update",total_source_count_dict)
                                    trg_list = list(result_return_part1["total_source_count_dict"].items())[0]
                                    trg_key,trg_val = trg_list[0],trg_list[1]
                                    print("total_target_count_dict before update",total_target_count_dict)
                                    test_trg_dict.append(result_return_part1["total_target_count_dict"])
                                    total_target_count_dict[trg_key] += trg_val
                                    # total_target_count_dict.update(result_return_part1["total_target_count_dict"])
                                    print("total_target_count_dict after update",total_target_count_dict)
                                except Exception as exc:
                                    print('%r generated an exception: %s' % (part1, exc))
                                else:
                                    print('%r page is %d bytes' % (part1, len(data)))
            except Exception as e:
                print("Not working",e)
                    
            
            print(f"Number of records of the destination table {main_table} for {ambest_counter} ambest: {spark.read.format('parquet').load(main_table_target_path).count()}")

            for table_name in tables_list:
                if not file_paths_dict[table_name]:
                    print(f"Skipping writing for {table_name} as no source file found.")
                    continue

                if target_dataframes_dict[table_name] is not None : 
                    test_src_dict,test_trg_dict = [],[]
                    try:
                            print("For current table is",table_name,"creating parallel notebooks")
                            # Code for comparison
                            print("ambest_list",ambest_dict_parallel)
                            new_tables_list = [main_table] + tables_list
                            print('---> second new_tables_list ',new_tables_list)
                            with concurrent.futures.ThreadPoolExecutor(max_workers=len(new_tables_list)) as executor:
                                # Start the load operations
                                future_to_tables = {executor.submit(run_notebook1,dataset,run_id,run_type,load_type,main_table,
                                            ambest_dict_parallel,load_start_date_formatted,load_end_date_formatted,
                                            tables_list,file_paths_dict,target_bucket,year,ambest_counter,
                                            last_ambest_value,table_name,main_table_num,True,0): table_name for main_table_num,table_name in enumerate(new_tables_list)}
                                print("future_to_tables",future_to_tables)
                                for future in concurrent.futures.as_completed(future_to_tables):
                                    part2 = future_to_tables[future]
                                    try:
                                        result_return_part2 = future.result()
                                        result_return_part2 = json.loads(result_return_part2)
                                        print("Json data",result_return_part2)
                                        ambest_counter += int(result_return_part2["ambest_counter"])
                                        src_list = list(result_return_part2["total_source_count_dict"].items())[0]
                                        src_key,src_val = src_list[0],src_list[1]
                                        test_src_dict.append(result_return_part2["total_source_count_dict"])
                                        print("total_source_count_dict before update",total_source_count_dict)
                                        total_source_count_dict[src_key] += src_val
                                        print("total_source_count_dict after update",total_source_count_dict)
                                        trg_list = list(result_return_part2["total_source_count_dict"].items())[0]
                                        trg_key,trg_val = trg_list[0],trg_list[1]
                                        print("total_target_count_dict before update",total_target_count_dict)
                                        test_trg_dict.append(result_return_part2["total_target_count_dict"])
                                        total_target_count_dict[trg_key] += trg_val
                                        # total_target_count_dict.update(result_return_part2["total_target_count_dict"])
                                        print("total_target_count_dict after update",total_target_count_dict)
                                    except Exception as exc:
                                        print('%r generated an exception: %s' % (part2, exc))
                                    else:
                                        print('%r page is %d bytes' % (part2, len(result_return_part2)))
                        
                    except Exception as e:
                        print("Not working",e)
                        # print("test_src_dict",test_src_dict)
                        # print("test_trg_dict",test_trg_dict)
                        # print("total_source_count_dict",total_source_count_dict)
                        # print("total_target_count_dict",total_target_count_dict)
                        ambest_counter = 0
                        ambest_dict_parallel = {}
                        break
        else:
            print(f"No data found for table {main_table}. Skipping writing.")
        
        print("total_source_count_dict:",total_source_count_dict)
        print("total_target_count_dict:",total_target_count_dict)
        # for table in tables_list:
        for key in total_source_count_dict:
            if key in total_target_count_dict and total_source_count_dict[key] == total_target_count_dict[key]:
                count_match = "YES"
            else:
                count_match = "No"
            dict_format = [key,str(total_source_count_dict[key]),str(total_target_count_dict[key]),count_match]
            log.write_log(run_id =run_id,start_time = str(datetime.now()),dataset = dataset,run_type = run_type,
                            log_type = f"{TABLE_RECORD_COUNT}",
                            table_name = key,ambest = "",source_count = str(total_source_count_dict[key]),
                            target_count = str(total_target_count_dict[key]),
                            count_match = count_match
                            )
            table_count_info_list.append(dict_format)
        
        print("final counts:",table_count_info_list)
        return table_count_info_list

    except Exception as e:
        print(f"Error encountered un delta_load_app function.\nError: {e}")
        log.write_log(run_id=run_id,start_time=str(datetime.now()),dataset=dataset,run_type=run_type,
        log_type=ERROR,table_name="",ambest="",source_count=0,target_count=0,count_match="",
        error_code=f"Error encountered un delta_load_app function",
        error_message=format_exc()
        )
        raise Exception(f"Error: {e}")

    
    
