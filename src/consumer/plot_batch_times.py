import matplotlib.pyplot as plt

# Read the log file
batch_ids = []
execution_times = []

with open("batch_execution_times.log", "r") as f:
    for line in f:
        try:
            parts = line.strip().split(":")
            batch = int(parts[0].split()[1])
            time_taken = float(parts[1].strip().split()[0])
            batch_ids.append(batch)
            execution_times.append(time_taken)
        except (IndexError, ValueError):
            continue  # skip malformed lines

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(batch_ids, execution_times, marker='o', linestyle='-', color='skyblue')
plt.title("Spark Streaming Batch Execution Times")
plt.xlabel("Batch ID")
plt.ylabel("Execution Time (seconds)")
plt.grid(True)
plt.tight_layout()
plt.savefig("execution_times_plot.png")
plt.show()
