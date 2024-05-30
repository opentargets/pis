import os

from loguru import logger
from yapsy.IPlugin import IPlugin

from platform_input_support.manifest import ManifestStatus, get_manifest_service
from platform_input_support.modules.common import create_folder
from platform_input_support.modules.common.google_spreadsheet import get_spreadsheet_handler


class Otar(IPlugin):
    """OTAR data collection step."""

    def __init__(self):
        """OTAR class constructor."""
        self.step_name = 'OTAR'

    def process(self, conf, output, cmd_conf=None):
        logger.info('[STEP] BEGIN, otar')
        manifest_step = get_manifest_service().get_step(self.step_name)
        gcp_credentials = conf.gcp_credentials
        dst_folder = os.path.join(output.prod_dir, conf.gs_output_dir)
        logger.debug(f'Prepare destination folder at {dst_folder}')
        create_folder(dst_folder)
        if gcp_credentials is None:
            logger.error('NO GCP credentials have been provided')
        # TODO - Parallelize this
        for sheet in conf.sheets:
            path_dst = os.path.join(dst_folder, sheet.output_filename)
            handler = get_spreadsheet_handler(
                sheet.id_spreadsheet, sheet.worksheet_name, path_dst, sheet.output_format, gcp_credentials
            )
            manifest_step.resources.append(handler.download())
        get_manifest_service().compute_checksums(manifest_step.resources)
        if not get_manifest_service().are_all_resources_complete(manifest_step.resources):
            manifest_step.status_completion = ManifestStatus.FAILED
            manifest_step.msg_completion = 'COULD NOT retrieve all the resources'
        # TODO - Validation
        if manifest_step.status_completion != ManifestStatus.FAILED:
            manifest_step.status_completion = ManifestStatus.COMPLETED
            manifest_step.msg_completion = 'The step has completed its execution'
        logger.info('[STEP] END, otar')
