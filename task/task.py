class Task:
    def __init__(self, name, time, group, dependencies):
        self.name = name
        self.time = time
        self.group = group
        self.dependencies = set(dependencies)
        self.start_time = -1  # Track when this task starts