from typing import Set, Tuple

from loguru import logger
from pymongo import MongoClient

from src.models.service_error import ServiceError
from src.services.principal_mapping_service import (
    PrincipalMappingService,
)
from src.settings.mongodb_settings import MongoDBSettings


class AclServiceError(ServiceError):
    errors: list[str]

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__(",".join(self.errors))


class AclService:
    def __init__(
        self,
        settings: MongoDBSettings,
    ):
        self.mongodb_settings = settings
        connection_string = self.mongodb_settings.connection_string

        self.client: MongoClient = MongoClient(connection_string)

    def apply_acls_to_principals(
        self,
        database: str,
        role: str,
        principals: Set[str],
    ) -> Tuple[list[str] | None, list[str]]:
        """Applies a set of ACLs to the specified MongoDB principals.

        This method creates ACL bindings for each combination of ACL and principal
        and applies them to MongoDB.

        Args:
            database (str): MongoDB database to apply ACLs to.
            role (str): MongoDB role to be applied as ACL.
            principals (Set[str]): Set of MongoDB principals to apply ACLs to.

        Returns:
            list[str], list[str]: List of error messages if any errors occurred during
            the process and a list of granted users.
            None, list[str]: If no errors occurred and the operation was successful
            and a list of granted users.
        """
        errors = []
        granted_users: list[str] = []

        users = self.client[self.mongodb_settings.users_database].command(
            {"usersInfo": 1, "filter": {"roles": {"role": role, "db": database}}}
        )
        dev_users = self.client[self.mongodb_settings.users_database].command(
            {"usersInfo": 1, "filter": {"roles": {"role": f"{database}_developer", "db": database}}}
        )

        users_with_role = {user["user"] for user in users.get("users", [])}
        users_with_dev_role = {user["user"] for user in dev_users.get("users", [])}

        for user in principals:
            if user not in users_with_role and user not in users_with_dev_role:
                try:
                    self.client[self.mongodb_settings.users_database].command(
                        {
                            "grantRolesToUser": user,
                            "roles": [{"role": role, "db": database}],
                        }
                    )
                    logger.info(f"Applied ACL {role} to {user} in database {database}.")
                    granted_users.append(user)
                except Exception as e:
                    error_message = f"Failed to apply ACL {role} or developer role to user {user}. Details: {str(e)}"
                    logger.exception(error_message)
                    errors.append(error_message)
            else:
                logger.warning(f"Principal {user} already has role {role} or developer role in database {database}.")
        return (errors, granted_users) if errors else (None, granted_users)

    def remove_all_acls_for_principals(
        self,
        database: str,
        role: str,
        principals: Set[str],
    ) -> Tuple[list[str] | None, list[PrincipalMappingService]]:
        """Removes all ACLs associated with a specific MongoDB collection.

        This method deletes all ACLs for the given collection using an ACL binding filter.

        Args:
            database (str): MongoDB database to remove ACLs from.
            role (str): MongoDB role to be removed.
            principals (Set[str]): Set of MongoDB principals to remove ACLs from.

        Returns:
            list[str], list[PrincipalMappingService]: List of error messages if any errors occurred during
            the process and a list of removed users.
            None, list[PrincipalMappingService]: If no errors occurred and the operation was successful
            and a list of removed users.
        """

        errors = []
        removed_users: list[PrincipalMappingService] = []

        users = self.client[self.mongodb_settings.users_database].command(
            {
                "usersInfo": 1,
                "filter": {"roles": {"role": role, "db": database}},
            }
        )

        users_with_role = users.get("users", [])

        if not users_with_role:
            logger.warning(f"No users found with role {role} in database {database}.")
            return None, removed_users

        for user in users_with_role:
            if user["user"] not in principals:
                try:
                    self.client[self.mongodb_settings.users_database].command(
                        {
                            "revokeRolesFromUser": user["user"],
                            "roles": [{"role": role, "db": database}],
                        }
                    )
                    logger.debug(f"Revoked role {role} from user {user['user']} in database {database}.")
                    removed_users.append(user["user"])
                except Exception as e:
                    error_message = f"Failed to revoke role {role} from user {user['user']}. Details: {str(e)}"
                    logger.exception(error_message)
                    errors.append(error_message)
            else:
                logger.debug(f"User {user['user']} is in Witboost identities {principals}.")

        return (errors, removed_users) if errors else (None, removed_users)
