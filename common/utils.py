import os
from typing import List
from task.task import Task

def print_schedule(schedule):
    print("| Time  | Tasks being Executed | Group Name ")
    print("|-------|-----------------------|------------")
    for row in schedule:
        if row['Tasks being Executed']:  # This line skips empty task sets
            print(f"| {row['Time']}     | {row['Tasks being Executed']:<21} | {row['Group Name']}")

def _has_cycle(tasks):
    visited = set()
    dfs_stack = []

    def dfs(task_name):
        if task_name in dfs_stack:
            return True  # Found a cycle
        
        if task_name in visited:
            return False
        
        visited.add(task_name)
        dfs_stack.append(task_name)

        for dependency in tasks[task_name].dependencies:
            if dfs(dependency):
                return True  # Propagate cycle detection upwards
        
        dfs_stack.pop()
        return False

    for task_name in tasks:
        if task_name not in visited:
            if dfs(task_name):
                return True

    return False

def read_pipeline(filename: str):
    if not os.path.exists(filename):
        raise FileNotFoundError("Error: Incorrect file path!")
    if os.path.getsize(filename) == 0:
        raise ValueError("Error: File is empty")
    valid, msg = _validate_pipeline(filename)
    if not valid:
        raise ValueError(f"Error: {msg}")
    tasks = {}
    with open(filename, 'r') as f:
        while True:
            name = f.readline().strip()
            if name == 'END':
                break
            time = int(f.readline().strip())
            group = f.readline().strip()
            dependencies = f.readline().strip().split(',')
            if dependencies == ['']:
                dependencies = []
            tasks[name] = Task(name, time, group, dependencies)
    _check_for_undefined_tasks(tasks)
    if _has_cycle(tasks):
        raise TypeError("Error: Detected cyclic dependencies in the tasks.")
    return tasks

def _validate_pipeline(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
        
        # Check if the number of lines is 4 * n + 1
        if (len(lines) - 1) % 4 != 0:
            return False, "Invalid number of lines"
        
        # Check the last line is "END"
        if lines[-1].strip() != "END":
            return False, "Last line must be 'END'"
        
        for i in range(0, len(lines) - 1, 4):
            # Validate task name
            task_name = lines[i].strip()
            if not task_name:
                return False, "Task name can't be empty"
            
            # Validate execution time
            try:
                execution_time = int(lines[i + 1].strip())
                if execution_time < 0:
                    return False, f"Invalid execution time for task {task_name}"
            except ValueError:
                return False, f"Execution time must be a non-negative integer for task {task_name}"
            
            # Validate group name (can be empty, so no check required)
            
            # Validate dependencies (can be empty, so no check required)
            dependencies = lines[i + 3].strip()
            if dependencies:
                if not all(dep.isalnum() for dep in dependencies.split(',')):
                    return False, f"Invalid dependencies for task {task_name}"

        return True, "Valid input file"

def _check_for_undefined_tasks(task_list):
    defined_tasks = set(task_list.keys())

    for task, task_properties in task_list.items():
        for dep in task_properties.dependencies:
            if dep not in defined_tasks:
                raise ValueError(f"Undefined task '{dep}' found as a dependency for task '{task}'")

def get_min_execution_time(tasks, cpu_cores):
    time = 0
    schedule = []
    executing_tasks = {}
    ready_tasks = [task for task in tasks.values() if len(task.dependencies) == 0]
    waiting_tasks = [task for task in tasks.values() if task not in ready_tasks]
    groups_in_execution = set()

    while executing_tasks or ready_tasks or waiting_tasks:
        just_completed = [task_name for task_name, task in list(executing_tasks.items()) if task.time == 0]
        
        for completed_task in just_completed:
            task_group = executing_tasks[completed_task].group
            groups_in_execution.discard(task_group)
            del executing_tasks[completed_task]

            for task in list(waiting_tasks):
                task.dependencies.discard(completed_task)
                if len(task.dependencies) == 0:
                    ready_tasks.append(task)
                    waiting_tasks.remove(task)

        # Check which groups are currently executing
        current_groups = {executing_tasks[task_name].group for task_name in executing_tasks if executing_tasks[task_name].group}

        while ready_tasks and len(executing_tasks) < cpu_cores:
            # Get next task
            next_task = ready_tasks.pop(0)

            # If no groups are currently executing or the next task's group matches one that's executing
            if not current_groups or next_task.group in current_groups or not next_task.group:
                executing_tasks[next_task.name] = next_task
                if next_task.group:
                    groups_in_execution.add(next_task.group)
                    current_groups.add(next_task.group)
            else:
                waiting_tasks.append(next_task)

        current_executing_tasks = [task.name for task in executing_tasks.values() if task.time > 0]
        group_name = ','.join(sorted(set([task.group for task in executing_tasks.values() if task.time > 0 and task.group])))

        schedule.append({
            'Time': time + 1,
            'Tasks being Executed': ','.join(sorted(current_executing_tasks)),
            'Group Name': group_name
        })

        for task in executing_tasks.values():
            task.time -= 1

        if executing_tasks:
            time += 1

    return time, schedule
