"""
Logging configuration
"""
import logging
import sys
from typing import Any, Dict, List, Tuple

from loguru import logger
from pydantic import BaseModel


class LoggingConfig(BaseModel):
    """Logging configuration"""
    LOGGER_NAME: str = "data_modeling_automation"
    LOG_FORMAT: str = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    LOG_LEVEL: str = "INFO"
    LOG_FILE_PATH: str = "logs/app.log"
    ROTATION: str = "20 MB"
    RETENTION: str = "1 month"


class InterceptHandler(logging.Handler):
    """
    Default handler from examples in loguru documentation.
    See https://loguru.readthedocs.io/en/stable/overview.html#entirely-compatible-with-standard-logging
    """

    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging(config: LoggingConfig = LoggingConfig()) -> None:
    """
    Configure logging with loguru
    """
    # Remove default logger
    logger.remove()
    
    # Add console logger
    logger.add(
        sys.stdout,
        enqueue=True,
        backtrace=True,
        level=config.LOG_LEVEL,
        format=config.LOG_FORMAT,
    )
    
    # Add file logger
    logger.add(
        config.LOG_FILE_PATH,
        rotation=config.ROTATION,
        retention=config.RETENTION,
        enqueue=True,
        backtrace=True,
        level=config.LOG_LEVEL,
        format=config.LOG_FORMAT,
    )
    
    # Intercept standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    # Replace logging handlers with loguru
    for name in logging.root.manager.loggerDict.keys():
        logging.getLogger(name).handlers = [InterceptHandler()]
    
    # Set logging level for specific loggers
    logging.getLogger("uvicorn.access").handlers = [InterceptHandler()]
    logging.getLogger("uvicorn.error").handlers = [InterceptHandler()]
    
    logger.info("Logging configured")


# Initialize logging
setup_logging()
