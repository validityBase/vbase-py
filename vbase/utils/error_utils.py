"""Common error and validation utility functions
"""


def check_for_missing_env_vars(env_vars: dict):
    """Checks whether any required environment values are undefined.

    :param env_vars: The dictionary of environment variables.
    """
    # Check for missing environment variables since these are unrecoverable.
    missing_keys = [k for k, v in env_vars.items() if v is None]
    if missing_keys:
        missing_keys_str = ", ".join(missing_keys)
        raise EnvironmentError(
            f"Missing required environment variables: {missing_keys_str}"
        )
