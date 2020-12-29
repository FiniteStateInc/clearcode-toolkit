"""
    fs_log.py

    Loads and configures logging for FiniteState components.
"""

import logging
import logging.config
from structlog import configure, processors, stdlib, threadlocal

# --------------------------------
# Logging Configuration Profiles

# Available configuration profiles:
#  - LOCAL -> Sends logs to console for easy, simple debugging

log_cfgs = {
    # Local logging configuration
    'LOCAL': {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '[%(levelname)-8s] %(name)-8s | %(funcName)-12s | %(message)s'
            },
        },
        'handlers': {
            'default': {
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': logging.DEBUG,
            }
        }
    }
}


class UnknownLogConfig(Exception):
    pass


def configure_logging(cfg_type):
    # Get the logging config
    log_cfg = None
    if cfg_type.upper() in log_cfgs:
        log_cfg = log_cfgs[cfg_type.upper()]
    else:
        log_cfg = log_cfgs['LOCAL']
    logging.config.dictConfig(log_cfg)

    # Configure the logger
    configure(context_class=threadlocal.wrap_dict(dict),
              logger_factory=stdlib.LoggerFactory(),
              wrapper_class=stdlib.BoundLogger,
              processors=[
                  stdlib.filter_by_level, stdlib.add_logger_name, stdlib.add_log_level,
                  stdlib.PositionalArgumentsFormatter(),
                  processors.TimeStamper(fmt="iso"),
                  processors.StackInfoRenderer(), processors.format_exc_info,
                  processors.UnicodeDecoder(), stdlib.render_to_log_kwargs
              ])
