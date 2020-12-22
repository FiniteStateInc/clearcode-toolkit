"""
    analysis_dag.py

    Functions to access the firmware analysis plugin DAG.
"""
import json
import logging
import os

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def load_dag(file_path):
    try:
        with open(file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        # Return an empty dict if the file doesn't exist. This happens at test-time.
        # Tests should mock out the DAG when necessary to ensure that we test as if
        # plugins exist.
        logger.warning(f'DAG file not found at {file_path}! Returning an empty dict.')
        return {}


# Load the dag for both the analysis and rollup worlds
dag = load_dag(os.path.join(SCRIPT_DIR, 'firmware_analysis_dag.json'))


def get_relevant_plugins_downstream_from(plugin):
    """
    Returns a list of plugins downstream from the input plugin (including the plugin itself)
    :param str plugin: the plugin name
    :return: list of plugin names
    """
    relevant_plugins = [plugin]
    for downstream_plugin in dag.get(plugin, []):
        relevant_plugins.extend(get_relevant_plugins_downstream_from(downstream_plugin))
    return relevant_plugins


def get_firmware_unpacked_plugins():
    fwp = get_relevant_plugins_downstream_from('firmware_unpacked')
    fwp.remove('firmware_unpacked')
    return fwp


def get_file_unpacked_plugins():
    fp = get_relevant_plugins_downstream_from('file_unpacked')
    fp.remove('file_unpacked')
    return fp


def get_file_unpacked_at_path_plugins():
    fp = get_relevant_plugins_downstream_from('file_unpacked_at_path')
    fp.remove('file_unpacked_at_path')
    return fp


def get_rollup_plugins():
    ac = get_relevant_plugins_downstream_from('AnalysisComplete')
    ac.remove('AnalysisComplete')
    return ac
