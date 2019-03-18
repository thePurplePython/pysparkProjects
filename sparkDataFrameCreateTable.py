def sparkDataFrameCreateTable(df, T = ''):
	"""
	this function inputs a dataframe and returns the sql create parquet table statment executed by spark sql w/ all cols and data types mapped from source dataframe
  this function serves as an alternative to the df.write.format("parquet").saveAsTable(...) bug not working with partitioned hive tables as source df read
  https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_rn_spark_ki.html#ki_sparksql_dataframe_saveastable
	: df => source dataframe object
	: T => target database.table string name object
	example ...
	'''
	df = spark.range(10)
	sparkDataFrameCreateTable(df, "junk.test")
	1 1 ('id', 'bigint')
	CREATE TABLE IF NOT EXISTS junk.test (id bigint) STORED AS PARQUET
	'''
	"""
    cols = df.dtypes
    ddl = []
    ddl.append("CREATE TABLE IF NOT EXISTS {} (".format(T))
    kv =  df.dtypes
    num = len(df.dtypes)
    count = 1
    for i in kv:
        print(count, num, i)
        if count == num:
            total = str(i[0]) + str(" ") + str(i[1])
        else:
            total = str(i[0]) + str(" ") + str(i[1]) + str(", ")
        ddl.append(total)
        count = count + 1
    ddl.append(") STORED AS PARQUET")
    schemaMap = ''.join(ddl)
    print(schemaMap)
    execSQL = spark.sql(schemaMap)
    return execSQL
