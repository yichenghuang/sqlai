import datetime
import json
import logging
from typing import Dict, Tuple, List
from sqlai.core import SingletonMeta


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class JobTracker(metaclass=SingletonMeta):
    """Manages job statuses with progress and required completion time.

    Attributes:
        _jobs (Dict[str, Tuple[float, datetime.datetime]]): Dictionary mapping 
            job_id to a tuple of progress (0-100) and completion time.
    """
    
    def __init__(cls):
        """Initializes an empty JobTracker with no jobs."""
        cls._jobs: Dict[str, Tuple[float, datetime.datetime]] = {}

    def add_job(cls, job_id: str, complete_time: datetime.datetime) -> None:
        """Adds a new job with initial progress 0 and specified completion time.

        Args:
            job_id: Unique identifier for the job.
            complete_time: Required completion time for the job.

        Raises:
            ValueError: If job_id already exists or complete_time is not a datetime.
        """
        if job_id in cls._jobs:
            raise ValueError(f"Job ID '{job_id}' already exists.")
        if not isinstance(complete_time, datetime.datetime):
            raise ValueError("complete_time must be a datetime.datetime object.")
        cls._jobs[job_id] = (0.0, complete_time)

    def get_progress(cls, job_id: str) -> Tuple[float, datetime.datetime]:
        """Retrieves the current progress for a specified job.

        Args:
            job_id: Job identifier.

        Returns:
            Tuple[float, datetime.datetime]: A tuple containing the job's 
            progress between 0.0 and 100.0 as a float and the last updated 
            timestamp as a datetime object.
        """
        return cls._jobs[job_id]

    def get_complete_time(cls, job_id: str) -> datetime.datetime:
        """Retrieves the completion time for a specified job.

        Args:
            job_id: Job identifier.

        Returns:
            datetime.datetime: Completion time of the job.

        Raises:
            KeyError: If job_id does not exist.
        """
        _, complete_time = cls._jobs[job_id]
        return complete_time

    def update_progress(cls, job_id: str, new_progress: float) -> None:
        """Updates the progress for a specified job, ensuring it's between 0 and 100.

        Args:
            job_id: Job identifier.
            new_progress: New progress value (0-100).

        Raises:
            KeyError: If job_id does not exist.
            ValueError: If new_progress is out of range.
        """
        if job_id not in cls._jobs:
            raise KeyError(f"Job ID '{job_id}' does not exist.")
        if not 0 <= new_progress <= 100:
            raise ValueError("Progress must be between 0 and 100.")
        _, complete_time = cls._jobs[job_id]
        cls._jobs[job_id] = (new_progress, complete_time)

    def mark_complete(cls, job_id: str) -> None:
        """Marks a job as complete by setting progress to 100 and updating completion time.

        Args:
            job_id: Job identifier.

        Raises:
            KeyError: If job_id does not exist.
        """
        if job_id not in cls._jobs:
            raise KeyError(f"Job ID '{job_id}' does not exist.")
        cls._jobs[job_id] = (100.0, datetime.datetime.now())