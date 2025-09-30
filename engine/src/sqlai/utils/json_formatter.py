import logging
import json
import datetime

class JsonFormatter(logging.Formatter):
    """
    A custom logging formatter that outputs logs as JSON.
    It captures standard log record attributes and allows for custom fields.
    Each formatted log entry will be a single JSON object on a single line,
    suitable for JSONL format.
    """
    # Precompute reserved LogRecord attributes
    _RESERVED = set(vars(logging.LogRecord(None, 0, "", 0, "", (), None)))

    def __init__(self, fmt=None, datefmt=None, style='%', **kwargs):
        super().__init__(fmt, datefmt, style)
        self.default_kwargs = kwargs
    
    def format(self, record):
        """
        Formats a log record into a JSON string.
        """
        log_entry = {
            "timestamp": datetime.datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(), # Handles msg % args
            "logger": record.name,
            "filename": record.filename,
            "lineno": record.lineno,
            **self.default_kwargs # Add any default fields from formatter init
        }
        custom = {k: v for k, v in record.__dict__.items() if k not in self._RESERVED}
        
        log_entry.update(custom)

        if hasattr(record, 'extra') and isinstance(record.extra, dict): 
            # Merge general extra data into the top-level log entry 
            log_entry.update(record.extra)

        # Serialize the log entry dictionary into a single-line JSON string.
        # ensure_ascii=False allows for direct inclusion of non-ASCII characters.
        return json.dumps(log_entry, ensure_ascii=False)
