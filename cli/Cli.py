import click
import logging
import sys
from cli.CliAction import CliAction
from cli.CliOption import CliOption
from util.S3Helper import S3Helper
from util.S3File import S3PathParamType

str_missing_mandatory_parameter = "Parameter {param} is mandatory when using action '{action}'"

config = {}
SYMMETRIC_KEY_OPTION = CliOption('symmetric-key', 'Symmetric key provided to unload data.  If provided to this tool '
                                                  'then client side encryption is assumed')
RETRIEVE_DEST_OPTION = CliOption('dest', 'Target directory where to store files', mandatory=True)
MANIFEST_S3URL_OPTION = CliOption('manifest-s3url', 'S3 path to manifest file', mandatory=True)

A_LIST_ACTIONS = CliAction('list-actions', 'Returns the list of supported actions')
A_LIST_FILES = CliAction('list-files', 'List the files mentioned in the manifest')
A_RETRIEVE_FILES = CliAction('retrieve-files', 'Retrieve files and store locally', [SYMMETRIC_KEY_OPTION,
                                                                                    RETRIEVE_DEST_OPTION,
                                                                                    MANIFEST_S3URL_OPTION])
A_CAT_FILES = CliAction('cat-files', 'Concatenate the files in manifest and print on stdout', [SYMMETRIC_KEY_OPTION,
                                                                                               MANIFEST_S3URL_OPTION])

supported_actions_full = [ A_LIST_ACTIONS, A_LIST_FILES, A_RETRIEVE_FILES, A_CAT_FILES ]
supported_actions_names = [action.name for action in supported_actions_full]


@click.command()
@click.option('--debug', is_flag=True, help='Will print debug messages.')
@click.option('--region', help='Force the region to be used.')
@click.option('--action', type=click.Choice(supported_actions_names), help='The action performed by the tool')
@click.option('--' + RETRIEVE_DEST_OPTION.name, type=click.Path(True, False, True, True, True),
              help=RETRIEVE_DEST_OPTION.description)
@click.option('--'+SYMMETRIC_KEY_OPTION.name, help=SYMMETRIC_KEY_OPTION.description)
@click.option('--' + MANIFEST_S3URL_OPTION.name, type=S3PathParamType(), help=MANIFEST_S3URL_OPTION.description)
def cli_main(debug, region, action, symmetric_key, dest, manifest_s3url):
    """This is a CLI tool to interact with Redshift manifest files.

    For supported actions use '--action list-actions'
    """
    log_format = '%(levelname)s - %(asctime)s - %(name)s - %(message)s'
    if debug:
        click.echo("Debugging mode activated")
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)
        logger = logging.getLogger('botocore')
        logger.setLevel(logging.WARN)

    logging.debug('Region {r}'.format(r=region))

    if action is None:
        click.echo('NO ACTION SPECIFIED!')
        click.echo('Defaulting to list available actions.  Please specify action using --action <action>')
        action = 'list-actions'

    if action == A_LIST_ACTIONS.name:
        click.echo('Available actions:')
        for action in supported_actions_full:
            click.echo(action)
        sys.exit(0)
    else:
        # Possible S3 access needed, initialize helper to make sure Region info is used
        S3Helper(region)
        if manifest_s3url is None:
            raise (click.BadParameter(str_missing_mandatory_parameter.format(action=action,
                                                                             param=MANIFEST_S3URL_OPTION.name)))

        if action == A_RETRIEVE_FILES.name:
            ## Make sure destination parameter was given
            if dest is None:
                raise(click.BadParameter(str_missing_mandatory_parameter.format(action=action,
                                                                                param=RETRIEVE_DEST_OPTION.name)))

            S3Helper.retrieve_files_from_manifest_file(manifest_s3url, dest, symmetric_key=symmetric_key)
            logging.debug('File retrieve action completed.')
            sys.exit(0)

        elif action == A_CAT_FILES.name:
            S3Helper.retrieve_files_from_manifest_file(manifest_s3url, None, symmetric_key=symmetric_key)
            logging.debug('File cat action completed.')
            sys.exit(0)
    click.echo('Unsupported action: {a}'.format(a=action))
    sys.exit(404)
