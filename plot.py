import matplotlib.pyplot as plt
import numpy as np


def plot_schedule(tasks):
    # Define the y-axis for different cores and cloud processes
    y_axis = {
        "Core 1": 6,
        "Core 2": 5,
        "Core 3": 4,
        "Wireless Sending": 3,
        "Cloud": 2,
        "Wireless Receiving": 1
    }

    x_axis = {1: "Core 1", 2: "Core 2", 3: "Core 3", 4: "Wireless Sending", 5: "Cloud",
                          6: "Wireless Receiving"}

    max_finish_time = 0
    task_data = []
    for task in tasks:
        if task["assignment"] < 4:
            max_finish_time = max(task["l_ft"], max_finish_time)

            start = task["l_st"]
            duration = task["l_ft"] - task["l_st"]
            bar = x_axis[task["assignment"]]
            y_pos = y_axis[bar]
            task_data.append((start, duration, y_pos, task["node id"]))
        else:
            max_finish_time = max(task["c_ft"], max_finish_time)
            cloud_start = task["c_rt"]
            cloud_duration = task["c_ft"] - task["c_rt"]
            y_pos = y_axis[x_axis[5]]
            task_data.append((cloud_start, cloud_duration, y_pos, task["node id"]))

            ws_start = task["ws_rt"]
            ws_duration = task["ws_ft"] - task["ws_rt"]
            y_pos = y_axis[x_axis[4]]
            task_data.append((ws_start, ws_duration, y_pos, task["node id"]))

            wr_rtart = task["wr_rt"]
            wr_duration = task["wr_ft"] - task["wr_rt"]
            y_pos = y_axis[x_axis[6]]
            task_data.append((wr_rtart, wr_duration, y_pos, task["node id"]))

    # Create the Matplotlib figure
    fig, ax = plt.subplots(figsize=(15, 8))

    # Plot the task bars
    for start, duration, y_pos, node_id in task_data:
        ax.broken_barh([(start, duration)], (y_pos - 0.2, 0.4), facecolors="white", edgecolor="black", linewidth=2)
        ax.text(start + duration / 2, y_pos, str(node_id), ha="center", va="center", color="black")

    # Set the yticks to be the names of the cores and cloud processes
    ax.set_yticks(list(y_axis.values()))
    ax.set_yticklabels(list(y_axis.keys()))

    # Set labels and grid
    ax.set_xlabel("Time")
    ax.set_xticks(np.arange(0, max_finish_time + 2, 1))
    ax.set_xlim(0, max_finish_time + 2)

    # Show the plot
    plt.show()
