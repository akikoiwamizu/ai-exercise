"""
Wrapper for accessing AWS credentials.

NOTE: DO NOT CHECK PASSWORD INFO INTO ANY GITHUB REPOS.
All credential information must be stored in a secure location.

CREDENTIAL FILE FORMAT: key = value
"""

import os


CREDENTIAL_DIR = "keys"


def read(credential_name: str, directory: str = CREDENTIAL_DIR) -> dict:
    """Read credentials using the parameters passed.

    Args:
      credential_name: The name of the credential info to read, inside
        the directory.
      directory: Directory for the credentials. This should be left as
        the default, except when testing.

    Returns:
      A dict with the credential info found.
    """
    creds = {}

    file_name = os.path.join(directory, credential_name + ".txt")

    if not os.path.exists(file_name):
        raise ValueError(f"Credential does not exist: {file_name}")

    with open(file_name, "r") as f:
        for line in f:
            # Strip comments and trailing whitespace.
            line = line.strip()

            # Skip comments and blanks.
            if line == "" or line.startswith("#"):
                continue

            # Extract out credential keys and values.
            if "=" not in line:
                raise ValueError(f"Line has no = symbol: {line}")

            (key, _, val) = line.partition("=")
            creds[key.strip()] = val.strip()

    return creds
