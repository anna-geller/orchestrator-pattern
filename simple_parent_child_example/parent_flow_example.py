import os
import subprocess
from prefect import Flow, task
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
    first_child_flow_run_id = create_flow_run(
        flow_name=CHILD_FLOW_NAME,
        project_name=PREFECT_PROJECT_NAME,
        parameters=dict(user_input="First child flow run"),
        task_args=dict(name="First subflow"),
        upstream_tasks=[normal_non_subflow_task],
    )
    first_child_flowrunview = wait_for_flow_run(
        first_child_flow_run_id,
        raise_final_state=True,
        stream_logs=True,
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
