from otter import Runner


def main() -> None:
    runner = Runner()
    runner.register_tasks('pis.tasks')
    runner.run()
