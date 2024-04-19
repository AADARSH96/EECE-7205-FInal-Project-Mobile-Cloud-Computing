import pandas as pd
import networkx as nx
from assignment import primary_assignment, new_primary_assignment
from priority import task_prioritization
from execution import execution_unit_selection, new_execution_unit_selection
from time_energy import calculate_time, calculate_energy
from tabulate import tabulate
from plot import draw_schedule
import matplotlib.pyplot as plt

n_cores = 3
T_send = 3
T_cloud = 1
T_receive = 1
T_re = T_send + T_cloud + T_receive
columns = ["core_speed", "successors", "predecessors", "core",
           "T_re", "weight", "type", "l_rt", "l_ft",
           "ws_rt", "ws_ft", "c_rt", "c_ft", "wr_rt", "wr_ft",
           "start_time", "ft", "core_assigned", "priority"]


def create_graph(edges):
    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    return graph


def process_test_case(test_case, graph, core_speed):
    nodes = len(graph.nodes)
    successors = {node: list(graph.successors(node)) for node in graph.nodes}
    predecessors = {node: list(graph.predecessors(node)) for node in graph.nodes}

    initial_task_core_table = {}
    for task in range(1, nodes + 1):
        initial_task_core_table[task] = {
            "core_speed": core_speed[task - 1],
            "successors": successors[task],
            "predecessors": predecessors[task],
            "core": "Local",  # Assuming all tasks are initially assigned to local cores
            "T_re": [T_send, T_cloud, T_receive],
            "weight": 0,
            "type": "",
            "l_rt": -1,
            "l_ft": 0,
            "ws_rt": -1,
            "ws_ft": 0,
            "c_rt": -1,
            "c_ft": 0,
            "wr_rt": -1,
            "wr_ft": 0,
            "start_time": [-1, -1, -1, -1],
            "ft": 0,
            "core_assigned": 0
        }

    for task in [key for key, value in predecessors.items() if not value]:
        initial_task_core_table[task]["type"] = "entry"
    for task in [key for key, value in successors.items() if not value]:
        initial_task_core_table[task]["type"] = "exit"

    initial_task_core_table = primary_assignment(initial_task_core_table)
    initial_task_core_table = task_prioritization(initial_task_core_table)
    initial_core_seqs, initial_task_core_table = execution_unit_selection(n_cores, initial_task_core_table)

    tasks = []
    for task, info in initial_task_core_table.items():
        if initial_task_core_table[task]["core"] == "Cloud":
            tasks.append({"node id": task,
                          "assignment": initial_task_core_table[task]["core_assigned"] + 1,
                          "c_rt": initial_task_core_table[task]["c_rt"],
                          "c_ft": initial_task_core_table[task]["c_rt"] + initial_task_core_table[task]["T_re"][1],
                          "ws_rt": initial_task_core_table[task]["ws_rt"],
                          "ws_ft": initial_task_core_table[task]["ws_rt"] + initial_task_core_table[task]["T_re"][0],
                          "wr_rt": initial_task_core_table[task]["wr_rt"],
                          "wr_ft": initial_task_core_table[task]["wr_rt"] + initial_task_core_table[task]["T_re"][2]})
        else:
            tasks.append({"node id": task, "assignment": initial_task_core_table[task]["core_assigned"] + 1,
                          "l_st": initial_task_core_table[task]["start_time"][
                              initial_task_core_table[task]["core_assigned"]],
                          "l_ft": initial_task_core_table[task]["start_time"][
                                      initial_task_core_table[task]["core_assigned"]] +
                                  initial_task_core_table[task]["core_speed"][
                                      initial_task_core_table[task]["core_assigned"]]})

    # Total time and energy at the end of initial scheduling
    initial_time = calculate_time(initial_task_core_table)
    initial_energy = calculate_energy(initial_task_core_table)
    print(f"INITIAL TIME: {initial_time} | INITIAL ENERGY: {initial_energy}")
    print()
    df_task_core_table = pd.DataFrame.from_dict(initial_task_core_table, orient='index')
    df_task_core_table.columns = columns
    df_task_core_table = df_task_core_table[["core_speed", "successors", "predecessors", "core",
                                             "T_re", "weight", "type", "core_assigned", "priority"]]
    print_results(df_task_core_table)
    draw_schedule(tasks)
    new_task_core_table = {}
    for task in range(1, nodes + 1):
        new_task_core_table[task] = {
            "core_speed": core_speed[task - 1],
            "successors": successors[task],
            "predecessors": predecessors[task],
            "core": "Local",  # Assuming all tasks are initially assigned to local cores
            "T_re": [T_send, T_cloud, T_receive],
            "weight": 0,
            "type": "",
            "l_rt": -1,
            "l_ft": 0,
            "ws_rt": -1,
            "ws_ft": 0,
            "c_rt": -1,
            "c_ft": 0,
            "wr_rt": -1,
            "wr_ft": 0,
            "start_time": [-1, -1, -1, -1],
            "ft": 0,
            "core_assigned": 0
        }
    for task in [key for key, value in predecessors.items() if not value]:
        new_task_core_table[task]["type"] = "entry"
    for task in [key for key, value in successors.items() if not value]:
        new_task_core_table[task]["type"] = "exit"

    new_task_core_table = new_primary_assignment(new_task_core_table)
    new_task_core_table = task_prioritization(new_task_core_table)
    new_core_seqs, new_task_core_table = new_execution_unit_selection(n_cores, new_task_core_table)
    tasks = []
    for task, info in new_task_core_table.items():
        if new_task_core_table[task]["core"] == "Cloud":
            tasks.append({"node id": task,
                          "assignment": new_task_core_table[task]["core_assigned"] + 1,
                          "c_rt": new_task_core_table[task]["c_rt"],
                          "c_ft": new_task_core_table[task]["c_rt"] + new_task_core_table[task]["T_re"][1],
                          "ws_rt": new_task_core_table[task]["ws_rt"],
                          "ws_ft": new_task_core_table[task]["ws_rt"] + new_task_core_table[task]["T_re"][0],
                          "wr_rt": new_task_core_table[task]["wr_rt"],
                          "wr_ft": new_task_core_table[task]["wr_rt"] + new_task_core_table[task]["T_re"][2]})
        else:
            tasks.append({"node id": task, "assignment": new_task_core_table[task]["core_assigned"] + 1,
                          "l_st": new_task_core_table[task]["start_time"][new_task_core_table[task]["core_assigned"]],
                          "l_ft": new_task_core_table[task]["start_time"][new_task_core_table[task]["core_assigned"]] +
                                  new_task_core_table[task]["core_speed"][new_task_core_table[task]["core_assigned"]]})
    new_time = calculate_time(new_task_core_table)
    new_energy = calculate_energy(new_task_core_table)
    print(f"NEW TIME: {new_time} | NEW ENERGY: {new_energy}")
    print()
    df_task_core_table = pd.DataFrame.from_dict(new_task_core_table, orient='index')
    df_task_core_table.columns = columns
    df_task_core_table = df_task_core_table[["core_speed", "successors", "predecessors", "core",
                                             "T_re", "weight", "type", "core_assigned", "priority"]]
    print_results(df_task_core_table)
    draw_schedule(tasks)
    task_info = {
        f"TEST_CASE_{test_case}": {"initial_time": initial_time, "initial_energy": initial_energy, "new_time": new_time,
                                   "new_energy": new_energy}}
    return pd.DataFrame.from_dict(task_info, orient='index')


def print_results(df_task_core_table):
    print(tabulate(df_task_core_table, headers=["core_speed", "successors", "predecessors", "core",
                                                "T_re", "weight", "type", "core_assigned", "priority"]))
    print(f"{'*' * 115}")


def main():
    print(f"No.of cores: {n_cores}")
    print(f"Time for RF send for all tasks: {T_send}")
    print(f"Time for cloud execution for all tasks: {T_cloud}")
    print(f"Time for RF receive for all tasks: {T_receive}\n")

    test_cases = [
        [
            ([(1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (2, 8), (2, 9), (3, 7), (4, 8), (4, 9),
              (5, 9), (6, 8), (7, 10), (8, 10), (9, 10)]),
            [
                [9, 7, 5], [8, 6, 5], [6, 5, 4], [7, 5, 3], [5, 4, 2],
                [7, 6, 4], [8, 5, 3], [6, 4, 2], [5, 3, 2], [7, 4, 2]
            ]
        ],
        [
            ([(1, 2), (1, 3), (1, 4), (2, 5), (3, 8), (3, 7), (2, 7), (4, 8), (4, 7),
              (5, 6), (6, 10), (7, 9), (7, 10), (8, 9), (9, 10)]),
            [
                [9, 7, 5], [8, 6, 5], [6, 5, 4], [7, 5, 3], [5, 4, 2],
                [7, 6, 4], [8, 5, 3], [6, 4, 2], [5, 3, 2], [7, 4, 2]
            ]
        ],
        [
            ([(1, 2), (1, 3), (1, 4), (2, 5), (3, 6), (3, 7), (2, 7), (2, 7), (3, 7), (3, 8), (4, 8), (4, 9), (5, 9),
              (5, 10), (6, 10), (6, 11), (7, 10), (7, 12), (8, 12), (8, 13), (9, 13), (9, 14), (10, 14), (10, 15),
              (11, 15), (11, 16), (12, 17), (13, 17), (14, 18), (13, 18), (14, 19), (15, 19), (16, 19), (19, 20),
              (17, 20), (18, 20)]),
            [
                [9, 7, 5], [8, 6, 5], [6, 5, 4], [7, 5, 3], [5, 4, 2], [7, 6, 4], [8, 5, 3], [6, 4, 2], [5, 3, 2],
                [7, 4, 2], [6, 4, 3], [5, 3, 2], [8, 6, 4], [7, 5, 3], [6, 4, 3], [5, 3, 2], [7, 5, 4], [8, 6, 5],
                [6, 5, 4], [7, 6, 5]
            ]
        ],
        [
            ([
                (1, 7), (2, 7), (3, 7), (3, 8), (4, 8), (4, 9), (5, 9), (5, 10), (6, 10), (10, 11), (6, 11),
                (7, 12), (8, 12), (8, 13), (9, 13), (9, 14), (10, 14), (10, 15), (11, 15), (11, 16),
                (12, 17), (13, 17), (13, 18), (14, 18), (14, 19), (15, 19), (16, 19), (17, 20), (18, 20),
                (19, 20)
            ]),
            [
                [9, 7, 5], [8, 6, 5], [6, 5, 4], [7, 5, 3], [5, 4, 2], [7, 6, 4], [8, 5, 3], [6, 4, 2], [5, 3, 2],
                [7, 4, 2], [12, 3, 3], [12, 8, 4], [11, 3, 2], [12, 11, 4], [13, 4, 2], [9, 7, 3], [9, 3, 3],
                [13, 9, 2], [10, 5, 3], [12, 5, 4]

            ]
        ],
        [
            ([(1, 7), (2, 7), (3, 7), (3, 8), (4, 8), (4, 9), (5, 9), (5, 10), (6, 10), (10, 11), (6, 11), (8, 12),
              (7, 12), (9, 13), (8, 13), (10, 14), (9, 14), (11, 15), (10, 15), (11, 16),
              (13, 17), (12, 17), (14, 18), (13, 18), (16, 19), (15, 19), (14, 19), (12, 20)]),
            [

                [9, 7, 5], [8, 6, 5], [6, 5, 4], [7, 5, 3], [5, 4, 2], [7, 6, 4], [8, 5, 3], [6, 4, 2], [5, 3, 2],
                [7, 4, 2], [12, 3, 3], [12, 8, 4], [11, 3, 2], [12, 11, 4], [13, 4, 2], [9, 7, 3], [9, 3, 3],
                [13, 9, 2], [10, 5, 3], [12, 5, 4]
            ]
        ]
    ]

    task_df = pd.DataFrame()
    for test in range(5):
        print(f"TEST_CASE {test + 1}:")
        graph = create_graph(test_cases[test][0])
        core_speed = test_cases[test][1]
        temp_df = process_test_case(test + 1, graph, core_speed)
        task_df = pd.concat([task_df, temp_df])
    return task_df


if __name__ == "__main__":
    task_df = main()
