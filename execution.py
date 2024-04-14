from pprint import pprint


def execution_unit_selection(n_cores, task_core_table):
    n = len(task_core_table.keys())  # Number of nodes for iteration

    # Initialize sequences for each core and the cloud.
    core1_seq = []
    core2_seq = []
    core3_seq = []
    cloud_seq = []

    # Track the earliest ready time for each core and the cloud.
    core_ready = [0] * (n_cores + 1)
    # Schedule each node based on priority.
    for task, info in task_core_table.items():  # Iterate in reverse order.
        # Calculate ready times and finish times for each node.
        if task_core_table[task]["type"] == "entry":  # If the node has no parents, it can start immediately.
            min_load_core = core_ready.index(min(core_ready))
            # Schedule the parentless node on the earliest available resource
            task_core_table[task]["l_rt"] = core_ready[min_load_core]
            task_core_table[task]["ws_rt"] = core_ready[min_load_core]
            task_core_table[task]["ws_ft"] = task_core_table[task]["ws_rt"] + task_core_table[task]["T_re"][0]
            task_core_table[task]["c_rt"] = task_core_table[task]["ws_ft"]
            core_ready[min_load_core] = task_core_table[task]["c_rt"]
        else:  # If the node has parents, calculate its ready time based on their finish times.
            # Calculations for local and cloud ready times.
            task_core_table[task]["l_rt"] = max(
                [max(task_core_table[predecessor]["l_ft"], task_core_table[predecessor]["wr_ft"]) for predecessor in
                 task_core_table[task]["predecessors"]], default=0)

            task_core_table[task]["ws_rt"] = max(
                [max(task_core_table[predecessor]["l_ft"], task_core_table[predecessor]["wr_ft"]) for predecessor in
                 task_core_table[task]["predecessors"]], default=0)

            task_core_table[task]["ws_ft"] = max(task_core_table[task]["ws_rt"], core_ready[3]) + \
                                             task_core_table[task]["T_re"][0]

            task_core_table[task]["c_rt"] = max(task_core_table[task]["ws_ft"], max([(task_core_table[predecessor][
                                                                                          "wr_ft"] -
                                                                                      task_core_table[task]["T_re"][2])
                                                                                     for predecessor in
                                                                                     task_core_table[task][
                                                                                         "predecessors"]], default=0))

        if task_core_table[task]["core"] == "Cloud":
            # Scheduling for a node assigned to the cloud.
            task_core_table[task]["wr_rt"] = task_core_table[task]["c_rt"] + task_core_table[task]["T_re"][1]
            task_core_table[task]["wr_ft"] = task_core_table[task]["wr_rt"] + task_core_table[task]["T_re"][2]
            task_core_table[task]["ft"] = task_core_table[task]["wr_ft"]
            task_core_table[task]["l_ft"] = 0
            core_ready[3] = task_core_table[task]["ws_ft"]
            task_core_table[task]["start_time"][3] = task_core_table[task]["ws_rt"]
            task_core_table[task]["core_assigned"] = 3  # Assign to cloud
            task_core_table[task]["core"] = "Cloud"
        else:
            # Find the most suitable core for scheduling.
            finish_time = float('inf')
            index = -1
            for j in range(n_cores):
                ready_time = max(task_core_table[task]["l_rt"], core_ready[j])
                if finish_time > ready_time + task_core_table[task]["core_speed"][j]:
                    finish_time = ready_time + task_core_table[task]["core_speed"][j]
                    index = j
            task_core_table[task]["l_rt"] = finish_time - task_core_table[task]["core_speed"][index]
            task_core_table[task]["start_time"][index] = task_core_table[task]["l_rt"]
            task_core_table[task]["l_ft"] = finish_time
            task_core_table[task]["wr_rt"] = task_core_table[task]["c_rt"] + task_core_table[task]["T_re"][1]
            task_core_table[task]["wr_ft"] = task_core_table[task]["wr_rt"] + task_core_table[task]["T_re"][2]

            # Decide whether to schedule the node on the selected core or in the cloud.
            if task_core_table[task]["l_ft"] <= task_core_table[task]["wr_ft"]:
                task_core_table[task]["ft"] = task_core_table[task]["l_ft"]
                task_core_table[task]["start_time"][index] = task_core_table[task]["l_rt"]
                task_core_table[task]["wr_ft"] = 0
                core_ready[index] = task_core_table[task]["ft"]
                task_core_table[task]["core_assigned"] = index
                task_core_table[task]["core"] = "Local"
            else:
                task_core_table[task]["ft"] = task_core_table[task]["wr_ft"]
                task_core_table[task]["l_ft"] = 0
                core_ready[3] = task_core_table[task]["ft"]
                task_core_table[task]["start_time"][3] = task_core_table[task]["ws_rt"]
                task_core_table[task]["core_assigned"] = 3  # Assign to cloud
                task_core_table[task]["core"] = "Cloud"
        if task_core_table[task]["core_assigned"] == 0:
            core1_seq.append(task)
        elif task_core_table[task]["core_assigned"] == 1:
            core2_seq.append(task)
        elif task_core_table[task]["core_assigned"] == 2:
            core3_seq.append(task)
        elif task_core_table[task]["core_assigned"] == 3:
            cloud_seq.append(task)
    seq = [core1_seq, core2_seq, core3_seq, cloud_seq]
    return seq, task_core_table


def new_execution_unit_selection(n_cores, task_core_table):
    n = len(task_core_table.keys())  # Number of nodes for iteration
    entry_tasks = [task for task, info in task_core_table.items() if info['type'] == 'entry']
    # Initialize sequences for each core and the cloud.
    core1_seq = []
    core2_seq = []
    core3_seq = []
    cloud_seq = []

    # Track the earliest ready time for each core and the cloud.
    core_ready = [0] * (n_cores + 1)
    # Schedule each node based on priority.
    for task, info in task_core_table.items():  # Iterate in reverse order.
        # Calculate ready times and finish times for each node.
        if task_core_table[task]["type"] == "entry":  # If the node has no parents, it can start immediately.
            min_load_core = core_ready.index(min(core_ready))
            # Schedule the parentless node on the earliest available resource
            task_core_table[task]["l_rt"] = core_ready[min_load_core]
            task_core_table[task]["ws_rt"] = core_ready[min_load_core]
            task_core_table[task]["ws_ft"] = task_core_table[task]["ws_rt"] + task_core_table[task]["T_re"][0]
            task_core_table[task]["c_rt"] = task_core_table[task]["ws_ft"]
            core_ready[min_load_core] = task_core_table[task]["c_rt"]
        else:  # If the node has parents, calculate its ready time based on their finish times.
            # Calculations for local and cloud ready times.
            task_core_table[task]["l_rt"] = max(
                [max(task_core_table[predecessor]["l_ft"], task_core_table[predecessor]["wr_ft"]) for predecessor in
                 task_core_table[task]["predecessors"]], default=0)

            task_core_table[task]["ws_rt"] = max(
                [max(task_core_table[predecessor]["l_ft"], task_core_table[predecessor]["wr_ft"]) for predecessor in
                 task_core_table[task]["predecessors"]], default=0)

            task_core_table[task]["ws_ft"] = max(task_core_table[task]["ws_rt"], core_ready[3]) + \
                                             task_core_table[task]["T_re"][0]

            task_core_table[task]["c_rt"] = max(task_core_table[task]["ws_ft"], max([(task_core_table[predecessor][
                                                                                          "wr_ft"] -
                                                                                      task_core_table[task]["T_re"][2])
                                                                                     for predecessor in
                                                                                     task_core_table[task][
                                                                                         "predecessors"]], default=0))

        if task_core_table[task]["core"] == "Cloud":
            if ((task_core_table[task]["type"] == "entry") and (len(entry_tasks) == 1)) or (
                    (task_core_table[task]["type"] == "entry") and (task == entry_tasks[0])):
                # Scheduling for a node assigned to the cloud.
                task_core_table[task]["wr_rt"] = task_core_table[task]["c_rt"] + task_core_table[task]["T_re"][1]
                task_core_table[task]["wr_ft"] = task_core_table[task]["wr_rt"] + task_core_table[task]["T_re"][2]
                task_core_table[task]["ft"] = task_core_table[task]["wr_ft"]
                task_core_table[task]["l_ft"] = 0
                core_ready[3] = task_core_table[task]["ws_ft"]
                task_core_table[task]["start_time"][3] = task_core_table[task]["ws_rt"]
                task_core_table[task]["core_assigned"] = 3  # Assign to cloud
                task_core_table[task]["core"] = "Cloud"
            elif (len(entry_tasks) > 1) or (task_core_table[task]["type"] != "entry"):
                task_core_table[task]["ws_rt"] = task_core_table[cloud_seq[-1]]["ws_ft"]
                task_core_table[task]["ws_ft"] = task_core_table[task]["ws_rt"] + task_core_table[task]["T_re"][0]
                task_core_table[task]["c_rt"] = task_core_table[task]["ws_ft"]
                task_core_table[task]["c_ft"] = task_core_table[task]["ws_ft"] + task_core_table[task]["T_re"][1]
                task_core_table[task]["wr_rt"] = task_core_table[task]["c_rt"] + task_core_table[task]["T_re"][1]
                task_core_table[task]["wr_ft"] = task_core_table[task]["wr_rt"] + task_core_table[task]["T_re"][2]
                task_core_table[task]["ft"] = task_core_table[task]["wr_ft"]
                task_core_table[task]["l_ft"] = 0
                core_ready[3] = task_core_table[task]["ws_ft"]
                task_core_table[task]["start_time"][3] = task_core_table[task]["ws_rt"]
                task_core_table[task]["core_assigned"] = 3  # Assign to cloud
                task_core_table[task]["core"] = "Cloud"
        else:
            # Find the most suitable core for scheduling.
            finish_time = float('inf')
            index = -1
            for j in range(n_cores):
                ready_time = max(task_core_table[task]["l_rt"], core_ready[j])
                if finish_time > ready_time + task_core_table[task]["core_speed"][j]:
                    finish_time = ready_time + task_core_table[task]["core_speed"][j]
                    index = j
            task_core_table[task]["l_rt"] = finish_time - task_core_table[task]["core_speed"][index]
            task_core_table[task]["start_time"][index] = task_core_table[task]["l_rt"]
            task_core_table[task]["l_ft"] = finish_time
            task_core_table[task]["wr_rt"] = task_core_table[task]["c_rt"] + task_core_table[task]["T_re"][1]
            task_core_table[task]["wr_ft"] = task_core_table[task]["wr_rt"] + task_core_table[task]["T_re"][2]

            # Decide whether to schedule the node on the selected core or in the cloud.
            if task_core_table[task]["l_ft"] <= task_core_table[task]["wr_ft"]:
                task_core_table[task]["ft"] = task_core_table[task]["l_ft"]
                task_core_table[task]["start_time"][index] = task_core_table[task]["l_rt"]
                task_core_table[task]["wr_ft"] = 0
                core_ready[index] = task_core_table[task]["ft"]
                task_core_table[task]["core_assigned"] = index
                task_core_table[task]["core"] = "Local"
            else:
                if ((task_core_table[task]["type"] == "entry") and (len(entry_tasks) == 1)) or (
                        (task_core_table[task]["type"] == "entry") and (task == entry_tasks[0])):
                    # Scheduling for a node assigned to the cloud.
                    task_core_table[task]["wr_rt"] = task_core_table[task]["c_rt"] + task_core_table[task]["T_re"][1]
                    task_core_table[task]["wr_ft"] = task_core_table[task]["wr_rt"] + task_core_table[task]["T_re"][2]
                    task_core_table[task]["ft"] = task_core_table[task]["wr_ft"]
                    task_core_table[task]["l_ft"] = 0
                    core_ready[3] = task_core_table[task]["ws_ft"]
                    task_core_table[task]["start_time"][3] = task_core_table[task]["ws_rt"]
                    task_core_table[task]["core_assigned"] = 3  # Assign to cloud
                    task_core_table[task]["core"] = "Cloud"
                elif (len(entry_tasks) > 1) or (task_core_table[task]["type"] != "entry"):
                    task_core_table[task]["ws_rt"] = task_core_table[cloud_seq[-1]]["ws_ft"]
                    task_core_table[task]["ws_ft"] = task_core_table[task]["ws_rt"] + task_core_table[task]["T_re"][0]
                    task_core_table[task]["c_rt"] = task_core_table[task]["ws_ft"]
                    task_core_table[task]["c_ft"] = task_core_table[task]["ws_ft"] + task_core_table[task]["T_re"][1]
                    task_core_table[task]["wr_rt"] = task_core_table[task]["c_rt"] + task_core_table[task]["T_re"][1]
                    task_core_table[task]["wr_ft"] = task_core_table[task]["wr_rt"] + task_core_table[task]["T_re"][2]
                    task_core_table[task]["ft"] = task_core_table[task]["wr_ft"]
                    task_core_table[task]["l_ft"] = 0
                    core_ready[3] = task_core_table[task]["ws_ft"]
                    task_core_table[task]["start_time"][3] = task_core_table[task]["ws_rt"]
                    task_core_table[task]["core_assigned"] = 3  # Assign to cloud
                    task_core_table[task]["core"] = "Cloud"
        if task_core_table[task]["core_assigned"] == 0:
            core1_seq.append(task)
        elif task_core_table[task]["core_assigned"] == 1:
            core2_seq.append(task)
        elif task_core_table[task]["core_assigned"] == 2:
            core3_seq.append(task)
        elif task_core_table[task]["core_assigned"] == 3:
            cloud_seq.append(task)
    seq = [core1_seq, core2_seq, core3_seq, cloud_seq]
    return seq, task_core_table
