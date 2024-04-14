def calculate_priority(task, priority_dict, task_core_table):
    if task in priority_dict:
        return priority_dict[task]

    if task_core_table[task]["type"] == "exit":
        priority_dict[task] = task_core_table[task]["weight"]
        return priority_dict
    max_successor_priority = max(
        calculate_priority(s, priority_dict, task_core_table) for s in task_core_table[task]["successors"])
    task_priority = task_core_table[task]["weight"] + max_successor_priority
    priority_dict[task] = task_priority
    return priority_dict


def task_prioritization(task_core_table):
    for task, info in task_core_table.items():
        if task_core_table[task]["core"] == "Cloud":
            info["weight"] = sum(info["T_re"])
        else:
            info["weight"] = sum(info["core_speed"]) / len(info["core_speed"])

    priority_dict = {}
    task_core_table = dict(sorted(task_core_table.items(), reverse=True))
    for task, info in task_core_table.items():
        priority_dict = calculate_priority(task, priority_dict, task_core_table)
    for task, priority in priority_dict.items():
        task_core_table[task]["priority"] = priority
    return dict(sorted(task_core_table.items(), key=lambda item: item[1]["priority"], reverse=True))
