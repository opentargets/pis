from otter import Runner


def main() -> None:
    runner = Runner('pis')
    runner.start()
    runner.register_tasks('pis.tasks')
    runner.run()
