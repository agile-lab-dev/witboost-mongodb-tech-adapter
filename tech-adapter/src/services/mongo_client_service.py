from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from src.models.service_error import ServiceError
from src.settings.mongodb_settings import MongoDBSettings


class MongoDBClientServiceError(ServiceError):
    pass


class MongoDBClientService:
    def __init__(
        self,
        settings: MongoDBSettings,
    ):
        self.mongodb_settings = settings
        connection_string = settings.connection_string

        self.client: MongoClient = MongoClient(connection_string)

    def create_database(self, database_name: str) -> Database:
        """Creates or updates a MongoDB database with the specified settings.

        If the database does not exist, it is created with the given settings.
        If the database already exists, no action is taken.

        Args:
            database_name (str): The name of the database to create or update.

        Returns:
            Database: The MongoDB database object.

        Raises:
            MongoDBClientServiceError: If the database creation or update fails.
        """
        logger.debug(f"Creating or updating database: {database_name}")
        try:
            db_names = [db["name"] for db in self.client.list_databases()]
            if database_name not in db_names:
                return self.client[database_name]
            else:
                logger.debug(f"Database {database_name} already exists. No action taken.")
                return self.client[database_name]
        except Exception as e:
            error_message = f"Failed to manage database {database_name}. Details: {str(e)}"
            logger.exception(error_message)
            raise MongoDBClientServiceError(error_message)

    def create_collection(self, database_name: str, collection_name: str, validator: dict) -> Collection:
        """Creates a collection in the specified database with an optional validator.

        Args:
            database_name (str): The name of the database.
            collection_name (str): The name of the collection to create.
            validator (dict, optional): A JSON schema validator for the collection.

        Raises:
            MongoDBClientServiceError: If the collection creation fails.
        """
        logger.debug(f"Creating collection {collection_name} in database {database_name} with validator: {validator}")
        try:
            db = self.client[database_name]
            if collection_name in db.list_collection_names():
                logger.debug(
                    f"Collection {collection_name} already exists in database {database_name}. Updating validator."
                )
                db.command({"collMod": collection_name, "validator": validator, "validationLevel": "moderate"})
                return db[collection_name]
            return db.create_collection(collection_name, validator=validator)
        except Exception as e:
            error_message = (
                f"Failed to create collection {collection_name} in database {database_name}. Details: {str(e)}"
            )
            logger.exception(error_message)
            raise MongoDBClientServiceError(error_message)

    def create_or_update_developer_role(
        self,
        database_name: str,
        user: str,
        role_name: str,
        roles: list,
    ):
        """Creates or updates a role in the MongoDB database.

        Args:
            database_name (str): The name of the database where the role will be created or updated.
            user (str | MappingError): The user to whom the role will be granted.
            role_name (str): The name of the role to create or update.
            roles (list): A list of roles to inherit from.
            privileges (list[dict]): A list of privileges to assign to the role.

        Raises:
            MongoDBClientServiceError: If the role creation or update fails.
        """
        logger.debug(f"Creating or updating role {role_name} in database {database_name} for user {user}")
        try:
            logger.debug(f"Checking if role {role_name} exists in database {database_name}.")
            db_admin = self.client[self.mongodb_settings.users_database]
            db = self.client[database_name]
            role_info = db.command("rolesInfo", role_name, showPrivileges=True)
            logger.debug(f"Granting role {role_name} to user {user} in database {database_name}.")
            if role_info.get("roles"):
                logger.debug(f"Role {role_name} already exists.")
                db_admin.command(
                    {
                        "grantRolesToUser": user,
                        "roles": [{"role": role_name, "db": database_name}],
                    }
                )
            else:
                logger.debug(f"Creating role {role_name} and granting it to user {user}.")
                db.command({"createRole": role_name, "privileges": [], "roles": roles})
                db_admin.command(
                    {
                        "grantRolesToUser": user,
                        "roles": [{"role": role_name, "db": database_name}],
                    }
                )

                logger.debug(f"Role {role_name} created and granted successfully.")
        except Exception as e:
            error_message = f"Failed to create or update role. Details: {str(e)}"
            logger.exception(error_message)
            raise MongoDBClientServiceError(error_message)

    def create_or_update_consumer_role(
        self,
        database_name: str,
        collection_name: str,
        actions: list[str],
    ):
        """Creates or updates a consumer role for a collection in the MongoDB database.

        Args:
            database_name (str): The name of the database.
            collection_name (str): The name of the collection.
            actions (list[str]): A list of actions to assign to the consumer role.

        Raises:
            MongoDBClientServiceError: If the role creation or update fails.
        """
        logger.debug(f"Creating or updating consumer role for {collection_name} in database {database_name}")
        try:
            consumer_role = f"{database_name}_{collection_name}_consumer"
            logger.debug(f"Checking if consumer role {consumer_role} exists.")
            db = self.client[database_name]
            role_info = db.command("rolesInfo", consumer_role, showPrivileges=True)
            if role_info.get("roles"):
                logger.debug(f"Consumer role {consumer_role} already exists. No action taken.")
                return
            logger.debug(f"Creating consumer role {consumer_role} for collection {collection_name}.")
            db.command(
                {
                    "createRole": consumer_role,
                    "privileges": [
                        {"resource": {"db": database_name, "collection": collection_name}, "actions": actions}
                    ],
                    "roles": [],
                }
            )
            logger.debug(f"Consumer role {consumer_role} created successfully for collection {collection_name}.")
        except Exception as e:
            error_message = (
                f"Failed to create or update consumer role for collection {collection_name} in database "
                f"{database_name}. Details: {str(e)}"
            )
            logger.exception(error_message)
            raise MongoDBClientServiceError(error_message)

    def drop_collection(self, database_name: str, collection_name: str):
        """Drops a collection from the specified database.

        Args:
            database_name (str): The name of the database.
            collection_name (str): The name of the collection to drop.

        Raises:
            MongoDBClientServiceError: If the collection drop fails.
        """
        logger.debug(f"Dropping collection {collection_name} from database {database_name}")
        try:
            logger.debug(f"Checking if database {database_name} exists.")
            db_names = [db["name"] for db in self.client.list_databases()]
            if database_name not in db_names:
                logger.error(f"Database {database_name} does not exist. Cannot drop collection {collection_name}.")
                return
            logger.debug(f"Dropping collection {collection_name} from database {database_name}.")
            db = self.client[database_name]
            db.drop_collection(collection_name)
            logger.debug(f"Collection {collection_name} dropped successfully from database {database_name}.")
        except Exception as e:
            error_message = (
                f"Failed to drop collection {collection_name} from database {database_name}. Details: {str(e)}"
            )
            logger.exception(error_message)
            raise MongoDBClientServiceError(error_message)

    def remove_role_from_consumer(
        self,
        database_name: str,
        collection_name: str,
    ):
        """Removes a role from a consumer in the MongoDB collection.

        Args:
            database_name (str): The name of the database.
            collection_name (str): The name of the collection.

        Raises:
            MongoDBClientServiceError: If the role removal fails.
        """
        logger.debug(f"Removing role from consumer for collection {collection_name} in database {database_name}")
        try:
            db = self.client[database_name]
            consumer_role = f"{database_name}_{collection_name}_consumer"
            logger.debug(f"Checking if role {consumer_role} exists.")
            role_info = db.command("rolesInfo", consumer_role, showPrivileges=True)
            if not role_info.get("roles"):
                logger.debug(f"Role {consumer_role} does not exist. No action taken.")
                return

            logger.debug(f"Removing role {consumer_role} from consumer in collection {collection_name}.")
            users = self.client[self.mongodb_settings.users_database].command(
                {"usersInfo": 1, "filter": {"roles": {"role": consumer_role, "db": database_name}}}
            )

            users_with_role = users.get("users", [])

            if not users_with_role:
                logger.debug(f"No users found with role {consumer_role} in database {database_name}.")
                return

            logger.debug(f"Revoking role {consumer_role} from users: {users_with_role}")
            for user in users_with_role:
                self.client[self.mongodb_settings.users_database].command(
                    {"revokeRolesFromUser": user["user"], "roles": [{"role": consumer_role, "db": database_name}]}
                )
                logger.debug(f"Revoked role {consumer_role} from user {user['user']}.")
            logger.debug(f"Role {consumer_role} removed successfully from consumer in collection {collection_name}.")
        except Exception as e:
            error_message = (
                f"Failed to remove role {consumer_role} from consumer in collection {collection_name}. "
                f"Details: {str(e)}"
            )
            logger.exception(error_message)
            raise MongoDBClientServiceError(error_message)

    def get_collections_info(self, database_name: str, collections: list[str] | None) -> list[tuple[str, dict | None]]:
        """Returns a list of collections informations in the specified database.

        Args:
            database_name (str): The name of the database.
            collections (list[str] | None): A list of collection names to retrieve information for or None if the
            collections list isn't specified in the parameters.

        Returns:
            list[tuple[str, dict | None]]: A list of collection names and their validators, if are present,
            in the database or None if the collection does't have the validator.

        Raises:
            MongoDBClientServiceError: If the operation fails.
        """
        try:
            db = self.client[database_name]
            collections_filter = {}
            if collections:
                collections_filter = {"name": {"$in": collections}}
                logger.debug(
                    "Retrieving collections with filter {} for database {}.", collections_filter, database_name
                )
            else:
                logger.debug("Retrieving all collections from database {}.", database_name)

            response = db.command({"listCollections": 1, "filter": collections_filter})["cursor"]["firstBatch"]
            logger.debug("Query response: {}", response)
            return [
                (collection_info["name"], collection_info.get("options", {}).get("validator"))
                for collection_info in response
            ]

        except Exception as e:
            error_message = (
                f"Failed to retrieve collection information from database {database_name}. Details: {str(e)}"
            )
            logger.exception(error_message)
            raise MongoDBClientServiceError(error_message)
