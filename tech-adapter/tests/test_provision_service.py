import unittest
from unittest.mock import MagicMock, Mock

from src.models.api_models import ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import DataProduct
from src.models.mongodb_models import (
    MongoDBComponentSpecific,
    MongoDBOutputPort,
    MongoDBOutputPortSubComponent,
    MongoDBSubComponentSpecific,
)
from src.services.mongo_client_service import MongoDBClientServiceError
from src.services.provision_service import ProvisionService


class TestProvisionService(unittest.TestCase):
    def setUp(self):
        self.mongo_client = MagicMock()
        self.mapping_service = MagicMock()
        self.settings = MagicMock()

        self.service = ProvisionService(
            mongodb_client_service=self.mongo_client,
            mongodb_mapping_service=self.mapping_service,
            mongodb_settings=self.settings,
        )

        self.data_product = Mock(spec=DataProduct)
        self.data_product.dataProductOwner = "owner"

        self.component = Mock(spec=MongoDBOutputPort)
        self.component.specific = Mock(MongoDBComponentSpecific)
        self.component.specific.database = "testdb"

    def test_provision_success(self):
        subcomponent = Mock(spec=MongoDBOutputPortSubComponent)
        subcomponent.id = "sub_id"
        subcomponent.specific = Mock(MongoDBSubComponentSpecific)
        subcomponent.specific.collection = "testcoll"
        subcomponent.specific.valueSchema = Mock()
        subcomponent.specific.valueSchema.definition = '{"bsonType": "object"}'
        self.component.get_typed_subcomponent_by_id.return_value = subcomponent

        self.mapping_service.map.return_value = {"owner": "mapped_owner"}
        self.mongo_client.create_database.return_value.name = "testdb"
        self.mongo_client.create_collection.return_value.name = "testcoll"

        mock_db = Mock()
        mock_db.command.return_value = {"users": [{"user": "user1"}, {"user": "mapped_owner"}, {"user": "admin"}]}
        self.mongo_client.client.return_value = mock_db

        self.settings.developer_roles = ["dpOwner"]
        self.settings.consumer_actions = ["find"]

        result = self.service.provision(self.data_product, self.component, "sub_id", remove_data=False)

        self.assertIsInstance(result, ProvisioningStatus)
        self.assertEqual(result.status, Status1.COMPLETED)

    def test_provision_service_error(self):
        component = Mock(spec=MongoDBOutputPort)
        component.specific = Mock()
        component.specific.database = "testdb"
        component.get_typed_subcomponent_by_id.return_value = Mock(
            id="sub_id", specific=Mock(collection="testcoll", valueSchema=Mock(definition='{"bsonType": "object"}'))
        )

        self.service.mongodb_mapping_service.map.return_value = {"owner": "mapped_owner"}

        self.service.mongodb_client_service.create_database.side_effect = MongoDBClientServiceError("Error creating DB")

        result = self.service.provision(self.data_product, component, "sub_id", remove_data=False)

        self.assertIsInstance(result, SystemErr)
        self.assertIn("Error creating DB", result.error)

    def test_unprovision_success(self):
        self.component.id = "mongodb-output-port"
        subcomponent = Mock(spec=MongoDBOutputPortSubComponent)
        subcomponent.specific = Mock(MongoDBSubComponentSpecific)
        subcomponent.specific.collection = "testcoll"
        self.component.get_typed_subcomponent_by_id.return_value = subcomponent

        self.mapping_service.map.return_value = {"owner": "mapped_owner"}
        self.mongo_client.drop_collection.return_value = None

        result = self.service.unprovision(self.data_product, self.component, "sub_id", remove_data=True)

        self.assertIsInstance(result, ProvisioningStatus)
        self.assertEqual(result.status, Status1.COMPLETED)

    def test_unprovision_service_error(self):
        self.component.id = "mongodb-output-port"

        self.service.mongodb_client_service.drop_collection.side_effect = MongoDBClientServiceError(
            "Error dropping collection"
        )

        result = self.service.unprovision(self.data_product, self.component, "sub_id", remove_data=True)

        self.assertIsInstance(result, SystemErr)
        self.assertIn("Error", result.error)
