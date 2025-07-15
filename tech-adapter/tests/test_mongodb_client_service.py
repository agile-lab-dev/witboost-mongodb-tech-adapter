import unittest
from unittest.mock import MagicMock, patch

from pymongo.errors import OperationFailure

from src.services.mongo_client_service import MongoDBClientService, MongoDBClientServiceError


class TestMongoDBClientService(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.settings.mongodb_settings.MongoDBSettings")
        self.addCleanup(patcher.stop)
        mock_settings_class = patcher.start()
        self.settings = mock_settings_class.return_value
        self.settings.connection_string = "mongodb://localhost:27017"
        self.settings.users_database = "admin"
        self.settings.developer_roles = ["dbOwner"]
        self.settings.consumer_actions = ["read"]
        self.service = MongoDBClientService(self.settings)
        self.service.client = MagicMock()
        self.db_mock = MagicMock()
        self.admin_db = MagicMock()

    def test_create_database_when_not_exists(self):
        self.service.client.list_databases.return_value = [{"name": "otherdb"}]

        self.service.client.__getitem__.return_value = self.db_mock

        result = self.service.create_database("mydb")

        self.assertEqual(result, self.db_mock)
        self.service.client.__getitem__.assert_called_with("mydb")

    def test_create_database_when_exists(self):
        self.service.client.list_databases.return_value = [{"name": "mydb"}]

        self.service.client.__getitem__.return_value = self.db_mock

        result = self.service.create_database("mydb")

        self.assertEqual(result, self.db_mock)

    def test_create_collection_success(self):
        self.service.client.__getitem__.return_value = self.db_mock

        self.service.create_collection("mydb", "mycollection", {"validator": "mock"})

        self.db_mock.create_collection.assert_called_once_with("mycollection", validator={"validator": "mock"})

    def test_create_collection_failure(self):
        self.db_mock.create_collection.side_effect = OperationFailure("fail")
        self.service.client.__getitem__.return_value = self.db_mock

        with self.assertRaises(MongoDBClientServiceError):
            self.service.create_collection("mydb", "mycollection", {})

    def test_create_or_update_developer_role_existing(self):
        self.service.client.__getitem__.side_effect = lambda name: self.admin_db if name == "admin" else self.db_mock

        self.db_mock.command.side_effect = lambda *args, **kwargs: {"roles": [{"role": "test"}]}

        self.service.create_or_update_developer_role(
            database_name="mydb",
            user="user1",
            role_name="myrole",
            roles=[],
        )

        self.admin_db.command.assert_called_with(
            {
                "grantRolesToUser": "user1",
                "roles": [{"role": "myrole", "db": "mydb"}],
            }
        )

    def test_create_or_update_developer_role_new(self):
        self.service.client.__getitem__.side_effect = lambda name: self.admin_db if name == "admin" else self.db_mock

        # Simulate no existing roles
        def command_side_effect(cmd, *args, **kwargs):
            if cmd == "rolesInfo":
                return {"roles": []}
            return {}

        self.db_mock.command.side_effect = command_side_effect

        self.service.create_or_update_developer_role("mydb", "user1", "myrole", [])

        self.db_mock.command.assert_any_call({"createRole": "myrole", "privileges": [], "roles": []})
        self.admin_db.command.assert_called_with(
            {
                "grantRolesToUser": "user1",
                "roles": [{"role": "myrole", "db": "mydb"}],
            }
        )

    def test_create_consumer_role_success(self):
        self.service.client.__getitem__.side_effect = lambda name: self.admin_db if name == "admin" else self.db_mock

        self.db_mock.command.side_effect = lambda *args, **kwargs: {"roles": []}

        self.service.create_or_update_consumer_role(
            database_name="mydb",
            collection_name="mycollection",
            actions=["find"],
        )

        self.db_mock.command.assert_any_call(
            {
                "createRole": "mydb_mycollection_consumer",
                "privileges": [{"resource": {"db": "mydb", "collection": "mycollection"}, "actions": ["find"]}],
                "roles": [],
            }
        )

    def test_create_consumer_role_error(self):
        self.service.client.__getitem__.side_effect = lambda name: self.admin_db if name == "admin" else self.db_mock

        self.db_mock.command.side_effect = Exception("error creating role")

        with self.assertRaises(MongoDBClientServiceError):
            self.service.create_or_update_consumer_role(
                database_name="mydb",
                collection_name="mycollection",
                actions=["find"],
            )

    def test_create_consumer_role_already_exists(self):
        self.service.client.__getitem__.side_effect = lambda name: self.admin_db if name == "admin" else self.db_mock

    def test_drop_collection_success(self):
        self.service.client.list_databases.return_value = [{"name": "mydb"}]
        self.service.client.__getitem__.return_value = self.db_mock

        self.service.drop_collection("mydb", "col1")

        self.db_mock.drop_collection.assert_called_once_with("col1")

    def test_drop_collection_db_missing(self):
        self.service.client.list_databases.return_value = [{"name": "other"}]

        self.service.drop_collection("missing", "col")

        self.service.client.__getitem__.return_value.drop_collection.assert_not_called()

    def test_remove_role_successfully(self):
        db_name = "mydb"
        coll_name = "mycollection"
        consumer_role = f"{db_name}_{coll_name}_consumer"

        self.admin_db = MagicMock()
        self.db_mock = MagicMock()

        self.service.client = MagicMock()
        self.service.client.__getitem__.side_effect = (
            lambda name: self.admin_db if name == "database_admin" else self.db_mock
        )

        self.service.mongodb_settings = MagicMock()
        self.service.mongodb_settings.users_database = "database_admin"

        def admin_command_side_effect(*args, **kwargs):
            if len(args) == 1 and isinstance(args[0], dict):
                command = args[0]
                if "usersInfo" in command:
                    roles_filter = command.get("filter", {}).get("roles", {})
                    if roles_filter.get("role") == consumer_role and roles_filter.get("db") == db_name:
                        return {"users": [{"user": "consumer", "roles": [{"role": consumer_role, "db": db_name}]}]}

            if len(args) == 1 and isinstance(args[0], dict) and "revokeRolesFromUser" in args[0]:
                return {"ok": 1}

            raise ValueError(f"Unexpected admin_db.command call with args: {args}, kwargs: {kwargs}")

        self.admin_db.command.side_effect = admin_command_side_effect

        def db_command_side_effect(*args, **kwargs):
            if args and args[0] == "rolesInfo" and kwargs.get("showPrivileges") is True:
                return {"roles": [{"role": consumer_role}]}
            raise ValueError(f"Unexpected db.command call with args: {args}, kwargs: {kwargs}")

        self.db_mock.command.side_effect = db_command_side_effect

        self.service.remove_role_from_consumer(db_name, coll_name)

        self.db_mock.command.assert_any_call("rolesInfo", consumer_role, showPrivileges=True)

        self.admin_db.command.assert_any_call(
            {
                "usersInfo": 1,
                "filter": {"roles": {"role": consumer_role, "db": db_name}},
            }
        )

        self.admin_db.command.assert_any_call(
            {
                "revokeRolesFromUser": "consumer",
                "roles": [{"role": consumer_role, "db": db_name}],
            }
        )

    def test_role_does_not_exist(self):
        self.service.client["database"].command.return_value = {"roles": []}
        self.service.remove_role_from_consumer("database", "collection")

    def test_no_users_found(self):
        self.service.client["database"].command.return_value = {"roles": [{"role": "database_collection_consumer"}]}
        self.service.client["database_admin"].command.return_value = {"users": []}
        self.service.remove_role_from_consumer("database", "collection")

    def test_role_not_assigned_to_any_user(self):
        self.service.client["database"].command.return_value = {"roles": [{"role": "database_collection_consumer"}]}
        self.service.client["database_admin"].command.return_value = {
            "users": [{"user": "test", "roles": [{"role": "other_role", "db": "mydb"}]}]
        }
        self.service.remove_role_from_consumer("mydb", "collection")

    def test_exception_is_raised(self):
        self.service.client["database"].command.side_effect = Exception("Unexpected error")
        with self.assertRaises(MongoDBClientServiceError):
            self.service.remove_role_from_consumer("database", "collection")

    def test_update_unprovision_role_missing(self):
        self.service.client.__getitem__.return_value = self.db_mock

        self.db_mock.command.side_effect = lambda *args, **kwargs: {"roles": []}

        self.service.remove_role_from_consumer("mydb", "col")

        self.db_mock.command.assert_called_once_with("rolesInfo", "mydb_col_consumer", showPrivileges=True)

    def test_create_database_generic_exception(self):
        self.service.client.list_databases.side_effect = Exception("generic error")
        with self.assertRaises(MongoDBClientServiceError):
            self.service.create_database("faildb")

    def test_create_or_update_role_generic_error(self):
        self.service.client.__getitem__.return_value = self.db_mock
        self.db_mock.command.side_effect = Exception("error creating role")

        with self.assertRaises(MongoDBClientServiceError):
            self.service.create_or_update_developer_role("mydb", "user", "role", [])

    def test_drop_collection_generic_error(self):
        self.service.client.list_databases.return_value = [{"name": "mydb"}]
        self.service.client.__getitem__.return_value = self.db_mock
        self.db_mock.drop_collection.side_effect = Exception("error dropping collection")

        with self.assertRaises(MongoDBClientServiceError):
            self.service.drop_collection("mydb", "mycollection")

    def test_create_collection_generic_error(self):
        self.service.client.__getitem__.return_value = self.db_mock
        self.db_mock.create_collection.side_effect = Exception("error creating collection")

        with self.assertRaises(MongoDBClientServiceError) as context:
            self.service.create_collection("mydb", "mycollection", {})

        self.assertIn("error creating collection", str(context.exception))
