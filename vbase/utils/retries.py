import logging
import time

def with_retries(operation_to_retry, log: logging.Logger, max_attempts=5, delay=2):
    """
    A function to retry an operation with exponential backoff on transient errors.
    
    :param operation_to_retry: The function/operation to retry
    :param max_attempts: Maximum number of retry attempts
    :param delay: Initial delay between retries (exponentially increased)
    """
    for attempt in range(1, max_attempts + 1):
        try:
            log.info("Attempt: %s", attempt)
            return operation_to_retry()
        except Exception as e:  # pylint: disable=broad-except
            log.error(e)
            if attempt == max_attempts:
                raise  # re-raise on final failure
            time.sleep(delay * 2 ** (attempt - 1))