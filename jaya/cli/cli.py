import click
from jaya.cli.deploy import commands as deploy_commands


@click.group()
def main():
    pass


main.add_command(deploy_commands.deploy)

if __name__ == '__main__':  # pragma: no cover
    main()
