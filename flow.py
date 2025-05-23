from pathlib import Path

from dbt_common.events.base_types import EventLevel
from prefect import flow
from prefect_dbt import PrefectDbtSettings, PrefectDbtRunner


@flow
def dbt_flow(
    commands: list[str],
    project_dir: Path,
    profiles_dir: Path = Path.cwd(),
    log_level: EventLevel = EventLevel.INFO,
):
    settings = PrefectDbtSettings(
        log_level=log_level, project_dir=project_dir, profiles_dir=profiles_dir
    )
    runner = PrefectDbtRunner(settings=settings)
    for command in commands:
        runner.invoke(command.split(" "))

if __name__ == "__main__":
    dbt_flow(
        commands=["build"],
        project_dir=Path.cwd() / "demo",
    )