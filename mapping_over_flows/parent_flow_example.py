import os
import subprocess
from prefect import Flow, task, unmapped
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

PARENT_FLOW_NAME = "parent_flow_example"
CHILD_FLOW_NAME = "child_flow_example"
PREFECT_PROJECT_NAME = "community"


@task(log_stdout=True)
def hello_world_parent():
    print(f"Hello from {PARENT_FLOW_NAME}!")
    return PARENT_FLOW_NAME


with Flow(PARENT_FLOW_NAME) as parent_flow:
    normal_non_subflow_task = hello_world_parent()
    first_child_flow_run_ids = create_flow_run.map(
        flow_name=[CHILD_FLOW_NAME, CHILD_FLOW_NAME, CHILD_FLOW_NAME],
        project_name=unmapped(PREFECT_PROJECT_NAME),
        parameters=unmapped(dict(user_input="Example parameter value")),
        task_args=dict(name="First subflow"),
        upstream_tasks=[normal_non_subflow_task],
    )
    first_child_flowrunview = wait_for_flow_run.map(
        first_child_flow_run_ids,
        raise_final_state=unmapped(True),
        stream_logs=unmapped(True),
        task_args=dict(name="Wait for the first subflow"),
    )

if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    subprocess.run(
        f"prefect register --project {PREFECT_PROJECT_NAME} -p {dir_path}", shell=True,
    )
    subprocess.run(
        f"prefect run --name {PARENT_FLOW_NAME} --project {PREFECT_PROJECT_NAME}",
        shell=True,
    )
    subprocess.run(
        f"prefect agent local start", shell=True,
    )
