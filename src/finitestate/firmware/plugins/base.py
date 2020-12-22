"""
base.py

Base class definitions for all firmware plugins.

Classes:
    FSPluginOutputResult: Final status of successful plugin.
    FSPluginOutput: Return class from all successful plugins.
    UnexpectedOutputLocationError: Error class for output validation.
    FSMessagePriority: Enum representing standard Finite State-defined message priorities.
    FSContext: Input passed into plugin describing context/working environment.
    FSPluginBase: Base class for all plugins in the Finite State firmware processing pipeline.
"""
import inspect
import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from concurrent.futures import TimeoutError
from enum import Enum, unique
from pebble import ProcessPool
from typing import List

from finitestate.common.log import configure_logging

from finitestate.firmware.plugins.messages import FSPluginMessageBase
from finitestate.firmware.plugins.storage import FSStorageAdapterBase
from finitestate.firmware.status.fspluginoutputresult import FSPluginOutputResult


class FSPluginOutput(object):
    """Return class from all successful plugin invocations. All errors from the plugin handler
    should be raised, not returned.

    Args:
        result_code: Describes the successful run type of the plugin.
        downstream_messages: List of message to be sent to other plugins downstream.
            If `[]`, then NO downstream message will be sent.
            Can be different type of message than was received as input.
            Can not be None.
        error_info: String providng any information available regarding
            errors that occured. If no error occurred, this should be
            left as `None`.

    Raises:
        TypeError: If `downstream_messages` is not a list(FSPluginMessageBase).
    """
    def __init__(self,
                 result_code: FSPluginOutputResult,
                 downstream_messages: List[FSPluginMessageBase] = [],
                 error_info: str = None):
        self.result_code = result_code
        self.downstream_messages = downstream_messages
        self.error_info = error_info

    @property
    def downstream_messages(self) -> List[FSPluginMessageBase]:
        return self._downstream_messages

    @downstream_messages.setter
    def downstream_messages(self, value: List[FSPluginMessageBase]):
        if not isinstance(value, list):
            raise TypeError()
        self._downstream_messages = value


class FSPluginError(Exception):
    """Base class for all exceptions originating from plugin classes"""
    pass


class UnexpectedOutputLocationError(FSPluginError):
    """Error raised in the case that a plugin tries to write to output it
    hasn't registered in `output_locations`."""
    pass


class IllegalReturnTypeError(FSPluginError):
    """Error raised in the case that a plugin returns something other than FSPluginOutput"""
    pass


@unique
class FSMessagePriority(Enum):
    """Enum representing standard Finite State-defined message priorities."""
    LOW = 'low'
    MED = 'med'
    HIGH = 'high'
    RETRY = 'retry'

    def __str__(self):
        return self.value


class FSContext():
    """Input passed into plugin describing context/working environment.

    Args:
        priority: Describes the priority of the message received.
        work_dir: Describes the location all temporary files should be written. Defaults to '/tmp'.
        configuration: Describes all optional per-plugin parameters to use. Defaults to {}.
    """
    def __init__(self, priority: FSMessagePriority, work_dir: str = '/tmp', configuration: dict = None):
        self._priority = FSMessagePriority(str(priority))
        self._work_dir = work_dir
        self._configuration = configuration if configuration is not None else {}

    @property
    def priority(self) -> FSMessagePriority:
        """FSMessagePriority: Priority with which the plugin is invoked."""
        return self._priority

    @priority.setter
    def priority(self, value: FSMessagePriority):
        self._priority = FSMessagePriority(str(value))

    @property
    def work_dir(self) -> str:
        """str: Absolute path of a working directory where the plugin should
        put any required temporary files. This directory is automatically
        cleaned up after every call to the 'handler' function.
        """
        return self._work_dir

    @work_dir.setter
    def work_dir(self, value: str):
        self._work_dir = value

    @property
    def configuration(self) -> dict:
        """dict: Dictionary containing plugin-and-file/firmware-specific
        configuration that can be used by manually change behavior of the
        plugin.

        For example, the base address of a specific binary file can be
        specified which will be handled in specific ways by the ghidra_db
        plugin. For more info on available plugin configuration fields, refer
        to each individual plugin.
        """
        return self._configuration

    @configuration.setter
    def configuration(self, value: dict):
        self._configuration = value


class FSPluginBase(ABC):
    """Base class for all plugins in the Finite State firmware processing pipeline.
    This class handles override functionality and abstracts away storage details and
    messaging details from plugin logic. `output_locations` are checked for existing
    overrides, and any calls to `store` functions will use override output instead.

    Plugins implementing this class must implement:
        input_message_type: Class type to use to deserialize input message.
        output_locations: Expected output locations to store data to. Data can only
            be stored in locations listed here.
        handler: Main entry point for plugin message/event handling.

    Example minimal implementation::

        see `test_plugin_fake_plugin.py` for an example of implementation

    Attributes:
        logger: Standard logging created for plugin to use

    Args:
        storage_adapter: Concrete implementation of AWS/local/null/on-prem/etc. storage adapter to
            be used by this plugin to store/retrieve files and metadata.

    Raises:
        TypeError: If `storage_adapter` is not derived from type `FSStorageAdapterBase`.

    Todo:
        Implement a "FSMessageAdapter" to abstract away messaging bus as well
    """

    # --------------------
    # Abstract Methods/Properties (to be filled in by all deriving classes)

    @property
    @abstractmethod
    def input_message_type(self) -> type:
        """Class type to use to deserialize input message."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def output_locations(self) -> List[str]:
        """Expected output locations to store data to. Data can only be stored
        in locations listed here.
        """
        raise NotImplementedError()

    @abstractmethod
    def handler(self, input_message: FSPluginMessageBase, context: FSContext) -> FSPluginOutput:
        """Main entry point for plugin message/event handling. Handles the
        given input_message with the given context, and returns the results.
        The handler itself is responsible for outputting data to storage or
        other locations as necessary.
        """
        raise NotImplementedError()

    # --------------------
    # Concrete Generic/Base Functionality (only override if necessary)

    def __init__(self, storage_adapter: FSStorageAdapterBase):
        if not isinstance(storage_adapter, FSStorageAdapterBase):
            raise TypeError('storage_adapter not derived from type FSStorageAdapterBase')
        self.storage = storage_adapter
        self._overrides = {}
        self._initialize_logging()
        self.logger.debug(f'Initializing {self.plugin_name} plugin')

    def _initialize_logging(self):
        """Instantiates and configures the logger based on the 'LOG_CFG'
        environment variable. The only value currently accepted is 'LOCAL',
        which writes nicely-formatted logs to stdout.
        By default, 'LOCAL' config is used.
        """
        LOG_CFG = os.environ.get('LOG_CFG', 'LOCAL')
        configure_logging(LOG_CFG)
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def plugin_name(self):
        """Returns the 'plugin name', which in our system is the name of the _file_
        which contains the derived plugin class. This is used when storing/retrieving
        plugin configuration, and also by CI/CD (the 'generate_analysis_plugin_resources.py'
        script) when creating the scalable services.
        """
        return os.path.splitext(os.path.basename(os.path.abspath(inspect.getfile(self.__class__))))[0]

    def handle_message(self, input_message: FSPluginMessageBase, timeout: int, priority: FSMessagePriority) -> FSPluginOutput:
        """Main entry point for main.py message loops.
        Loads all overrides and configuration for the plugin, creates a
        FSContext, invokes the handler, and returns results.

        Args:
            input_message: Input message that triggered plugin execution.
            timeout: Time (in seconds) to let the plugin run before forcefully stopping it and throwing an exception.
            priority: Priority of the given input message

        Returns:
            Output from the plugin's handler method.

        Raises:
            Exception: Any unexpected errors encountered during message handling result in an exception.
            TimeoutException: Raised if the execution exceeds the specified timeout.
        """
        result = None

        # Load any overrides that exist
        self.storage.load_overrides(self.output_locations, input_message.storage_id)

        # If there are overrides for _all_ output types then there is no need to
        # actually execute the plugin handler itself.
        if not self.storage.all_overrides():
            # Setup the context for this plugin execution, including specifying a
            # temp directory and loading any custom plugin configuration specific
            # to the input message's primary object id.
            with tempfile.TemporaryDirectory() as working_dir:
                context = FSContext(priority)
                context.work_dir = working_dir
                self.logger.debug(f'Temp working dir: {context.work_dir}')

                # Load custom plugin configuration if it exists
                if self.storage.plugin_config_exists(self.plugin_name, input_message.storage_id):
                    context.configuration = self._load_configuration(input_message.storage_id)
                    self.logger.debug(f'Custom config found: {context.configuration}')

                # Invoke the handler in a thread-safe and timeout-monitored way.
                # Catch any exceptions that occur here since overrides have to
                # be written afterwards no matter what.
                try:
                    with ProcessPool() as pool:
                        future = pool.schedule(self.handler, args=[input_message, context], timeout=timeout)
                        result = future.result()
                        # Verify the plugin is returning properly
                        if result is None or not isinstance(result, FSPluginOutput) or result.result_code in [
                                FSPluginOutputResult.TIMEOUT, FSPluginOutputResult.ERROR
                        ]:
                            raise IllegalReturnTypeError()
                except TimeoutError:
                    self.logger.exception(f'Timeout occurred while handling message ({timeout} second limit exceeded):')
                    result = FSPluginOutput(FSPluginOutputResult.TIMEOUT, error_info=f'Timeout after {timeout} seconds')
                except Exception as e:
                    self.logger.exception(f'Error occured while handling message:')
                    result = FSPluginOutput(FSPluginOutputResult.ERROR, error_info=str(e))
        else:
            self.logger.debug('Overrides found for all output locations. Skipping execution of handler and storing them.')

        # Write all the overrides that were found to storage, if any
        if self.storage.has_overrides():
            self.storage.store_overrides()

            # In the presence of any overrides, also override the return value to specify that results were written
            # (AKA set the `result_code` to `EXECUTED_WITH_OVERRIDES`)
            # TODO: What if `input_message` is different type than what should be output from this plugin??
            if result is None:
                result = FSPluginOutput(FSPluginOutputResult.EXECUTED_WITH_OVERRIDES, [input_message])
            else:
                # Preserve any downstream_messages, if exists, else see TODO: above for blindly forwarding a `input_message`
                result.result_code = FSPluginOutputResult.EXECUTED_WITH_OVERRIDES
                result.downstream_messages = result.downstream_messages if (
                    result.downstream_messages is not None and len(result.downstream_messages) > 0) else [input_message]

        return result

    def handle_message_from_dict(self, input_message_dict: dict, timeout: int, priority: FSMessagePriority) -> FSPluginOutput:
        """Helper method that invokes the `handle_message()` function with a single
        dictionary. This automatically handles deserialization of the dictionary
        into the desired message class. This method is mainly provided for unit testing.
        """
        input_message = self.input_message_type.deserialize(json.dumps(input_message_dict))

        # Invoke the normal handler routines
        return self.handle_message(input_message, timeout, priority)
