def sparkClusterYarnTuning(nodes, cores, ram, exec_slots = 5):
    """
    returns optimized spark submit application arguments customized to spark cluster on YARN specs
    params ...
    : nodes => # of data nodes in cluster
    : cores => # of cores / each node
    : ram => # of memory / each node
    : exec_slots => threads within each jvm executor [hardcoded for 5]
    returns ...
    : spark_submit ...
        => exec_num => number of executors --num-executors
        => exec_slots => available slots for tasks to run simultaneously --executor-cores
        => exec_mem => memory of each executor --executor-memory
    """
    cluster_total_cores = nodes * (cores - 1)
    exec_num = int(cluster_total_cores / exec_slots) - 1
    exec_mem = int(((ram - 1) / ((exec_num + 1) / nodes)) * (1 - 0.07))
    spark_submit = print("--num-executors " + str(exec_num) \
                         + " --num-executor-cores " + str(exec_slots) \
                         + " --executor-memory " + str(exec_mem))
    return spark_submit
