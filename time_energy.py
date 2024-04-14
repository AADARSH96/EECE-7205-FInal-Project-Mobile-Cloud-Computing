def calculate_time(task_core_table):
    # Find the maximum finish time among exit tasks
    return max(info["ft"] for info in task_core_table.values() if info["type"] == "exit")


def calculate_energy(task_core_table):
    total_energy = 0
    power = [1, 2, 4, 0.5]
    # Iterate through each task in the table
    for info in task_core_table.values():
        # Calculate energy consumption for local tasks
        if info["core"] == "Local":
            task_energy = info["core_speed"][info["core_assigned"]] * power[info["core_assigned"]]
        # Calculate energy consumption for cloud tasks
        elif info["core"] == "Cloud":
            task_energy = info["T_re"][0] * power[3]
        # Accumulate total energy
        total_energy += task_energy
    return total_energy
