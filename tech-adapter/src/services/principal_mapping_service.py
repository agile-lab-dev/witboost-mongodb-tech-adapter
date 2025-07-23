from abc import ABC, abstractmethod
from typing import Dict, Set, Union

from loguru import logger

# Constants for subject prefixes
USER_PREFIX = "user:"


class MappingError(BaseException):
    def __init__(self, error: str):
        self.error = error


class Mapper(ABC):
    @abstractmethod
    def map(self, subjects: Set[str]) -> Dict[str, Union[str, MappingError]]:
        """
        Defines the main mapping logic for a set of subjects.

        Args:
            subjects: A set of strings representing the subjects to be mapped
                      (e.g., WitBoost users and groups).

        Returns:
            A dictionary where:
            - Keys are the original subject strings from the input set.
            - Values are either the successfully mapped principal (str) on success,
              or an Exception object on failure for that specific subject.
        """
        pass


class PrincipalMappingService(Mapper):
    def map(self, subjects: Set[str]) -> Dict[str, Union[str, MappingError]]:
        """
        Maps a set of subjects identifiers to their target representation.

        For each subjects, it attempts a mapping. If it fails, the corresponding
        value in the returned dictionary will be the exception object that was raised.

        Args:
            subjects: A set of strings, each prefixed with "user:".

        Returns:
            A dictionary mapping the original subject to its successfully mapped
            string or to the Exception object detailing the failure.
        """
        results: Dict[str, Union[str, MappingError]] = {}
        for ref in subjects:
            try:
                logger.info(f"Mapping subject: {ref}")
                results[ref] = self._map_subject(ref)
            except MappingError as e:
                # Catch our custom, expected errors
                logger.warning(f"Failed to map subject '{ref}': {e}")
                results[ref] = e
        return results

    def _map_subject(self, ref: str) -> str | MappingError:
        if ref.startswith(USER_PREFIX):
            user_part = ref[len(USER_PREFIX) :]
            return self._get_and_map_user(user_part)

        error_msg = f"The subject '{ref}' isn't a Witboost user."
        logger.error(error_msg)
        raise MappingError(error_msg)

    def _get_and_map_user(self, user: str) -> str:
        try:
            underscore_index = user.rfind("_")
            if underscore_index == -1:
                return user
            mail = f"{user[:underscore_index]}@{user[underscore_index + 1:]}"
            return mail
        except Exception as e:
            error_msg = f"An unexpected error occurred while mapping the Witboost user '{user}'. Details: {e}"
            logger.error(error_msg)
            raise MappingError(error_msg) from e
