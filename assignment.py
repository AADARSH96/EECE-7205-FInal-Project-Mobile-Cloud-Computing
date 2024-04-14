def primary_assignment(task_core_table):
    for task, info in task_core_table.items():
        t_l_min = min(info["core_speed"])
        if sum(info["T_re"]) < t_l_min:  # EQ 11
            task_core_table[task]["core"] = "Cloud"  # CLoud
        else:
            task_core_table[task]["core"] = "Local"  # Local
    return task_core_table


def new_primary_assignment(task_core_table):
    for task, info in task_core_table.items():
        t_l_min = min(info["core_speed"])
        if 3 < t_l_min:  # EQ 11
            task_core_table[task]["core"] = "Cloud"  # CLoud
        else:
            task_core_table[task]["core"] = "Local"  # Local
    return task_core_table
