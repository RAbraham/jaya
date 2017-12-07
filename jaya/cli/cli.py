import click
import sys
from jaya.deployment.jaya_deploy import deploy_file
from jaya.cli.deploy import commands as deploy_commands


# @click.command()
# @click.option('--config_file', required=True, help='Path to .conf file with AWS credentials etc.')
# @click.option('--file', required=True, help='Path to file which contains the pipeline')
# @click.option('--pipeline', required=True, help='Name of the pipeline')
# @click.option('--function', required=False, help='Path to file which contains the pipeline')
# def main(config_file, file, pipeline, function):  # pragma: no cover
#     """
#     Main program execution handler.
#     """
#
#     try:
#
#         deploy_file(config_file, file, pipeline, function)
#     except SystemExit as e:  # pragma: no cover
#         sys.exit(e.code)
#     except KeyboardInterrupt:  # pragma: no cover
#         sys.exit(130)
#     except Exception as e:
#
#         click.echo("Oh no! An " + click.style("error occurred", fg='red', bold=True) + "! :(")
#         click.echo("\n==============\n")
#         import traceback
#         traceback.print_exc()
#         click.echo("\n==============\n")
#
#         sys.exit(-1)
#
@click.group()
def main():
    pass


main.add_command(deploy_commands.deploy)

if __name__ == '__main__':  # pragma: no cover
    main()
