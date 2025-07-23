import unittest
from unittest.mock import MagicMock

from src.services.acl_service import AclService
from src.settings.mongodb_settings import MongoDBSettings


class TestAclService(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_client = MagicMock()
        self.mock_client.__getitem__.return_value = self.mock_db

        self.mongodb_settings = MagicMock(spec=MongoDBSettings)
        self.mongodb_settings.connection_string = "fakehost:27017"
        self.mongodb_settings.users_database = "test_users_db"

        self.acl_service = AclService(settings=self.mongodb_settings)

        self.acl_service.client = self.mock_client

    def test_apply_acls_to_principals_success(self):
        self.mock_db.command.side_effect = [{"users": []}, {"users": []}, None]

        errors, granted = self.acl_service.apply_acls_to_principals(
            database="testdb", role="database_collection_consumer", principals=["user1"]
        )

        self.assertIsNone(errors)
        self.assertEqual(granted, ["user1"])
        self.mock_db.command.assert_called_with(
            {"grantRolesToUser": "user1", "roles": [{"role": "database_collection_consumer", "db": "testdb"}]}
        )

    def test_apply_acls_user_already_has_role(self):
        self.mock_db.command.side_effect = [{"users": [{"user": "user1"}]}, {"users": []}]

        errors, granted = self.acl_service.apply_acls_to_principals(
            database="testdb", role="database_collection_consumer", principals=["user1"]
        )

        self.assertIsNone(errors)
        self.assertEqual(granted, [])

    def test_apply_acls_grant_failure(self):
        def command_side_effect(arg):
            if "usersInfo" in arg:
                return {"users": []}
            else:
                raise Exception("error")

        self.mock_db.command.side_effect = command_side_effect

        errors, granted = self.acl_service.apply_acls_to_principals(
            database="testdb", role="database_collection_consumer", principals=["user2"]
        )

        self.assertIsNotNone(errors)
        self.assertIn("error", errors[0])
        self.assertEqual(granted, [])

    def test_remove_all_acls_success(self):
        self.mock_db.command.side_effect = [{"users": [{"user": "user1"}, {"user": "user2"}]}, None]

        errors, removed = self.acl_service.remove_all_acls_for_principals(
            database="testdb", role="testdb-collection_consumer", principals=["user2"]
        )

        self.assertIsNone(errors)
        self.assertEqual(removed, ["user1"])

    def test_remove_all_acls_no_users_found(self):
        self.mock_db.command.return_value = {"users": []}

        errors, removed = self.acl_service.remove_all_acls_for_principals(
            database="testdb", role="database_collection_consumer", principals=[]
        )

        self.assertIsNone(errors)
        self.assertEqual(removed, [])

    def test_remove_all_acls_revoke_failure(self):
        def command_side_effect(arg):
            if "usersInfo" in arg:
                return {"users": [{"user": "user2"}, {"user": "user3"}]}
            else:
                raise Exception("revoke failed")

        self.mock_db.command.side_effect = command_side_effect

        errors, removed = self.acl_service.remove_all_acls_for_principals(
            database="testdb", role="testdb-collection_consumer", principals=["user2"]
        )

        self.assertIsNotNone(errors)
        self.assertIn("revoke failed", errors[0])
        self.assertEqual(removed, [])
