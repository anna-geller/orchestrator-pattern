import platform
import prefect
from prefect import Flow, Parameter, task


CHILD_FLOW_NAME = "child_flow_example"


@task(log_stdout=True)
def hello_world_child(x: str):
    print(f"Hello {x} from {CHILD_FLOW_NAME}!")
    print(
        f"Running this task with Prefect: {prefect.__version__} and Python {platform.python_version()}"
    )


with Flow(CHILD_FLOW_NAME) as flow:
    user_input = Parameter("user_input", default="Marvin")
    hw = hello_world_child(user_input)
