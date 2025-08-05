import unittest
from unittest.mock import MagicMock, Mock

from src.models.api_models import ProvisioningStatus, Status1, SystemErr, ValidationError
from src.models.data_product_descriptor import DataProduct
from src.models.mongodb_models import (
    MongoDBComponentSpecific,
    MongoDBOutputPort,
    MongoDBOutputPortSubComponent,
    MongoDBSubComponentSpecific,
)
from src.services.mongo_client_service import MongoDBClientServiceError
from src.services.principal_mapping_service import MappingError
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
        subcomponent.useCaseTemplateId = (
            "urn:dmb:utm:mongodb-outputport-subcomponent-template:0.0.0"
        )
        subcomponent.specific = Mock(MongoDBSubComponentSpecific)
        subcomponent.specific.collection = "testcoll"
        subcomponent.specific.valueSchema = Mock()
        subcomponent.specific.valueSchema.definition = '{"bsonType": "object"}'
        self.component.get_typed_subcomponent_by_id.return_value = subcomponent
        self.component.useCaseTemplateId = "urn:dmb:utm:mongodb-outputport-template:0.0.0"

        self.mapping_service.map.return_value = {"owner": "mapped_owner"}
        self.mongo_client.create_database.return_value.name = "testdb"
        self.mongo_client.create_collection.return_value.name = "testcoll"

        mock_db = Mock()
        mock_db.command.return_value = {"users": [{"user": "user1"}, {"user": "mapped_owner"}, {"user": "admin"}]}
        self.mongo_client.client.return_value = mock_db

        self.settings.developer_roles = ["dpOwner"]
        self.settings.consumer_actions = ["find"]
        self.settings.useCaseTemplateId = "urn:dmb:utm:mongodb-outputport-template:0.0.0"
        self.settings.useCaseTemplateSubId = (
            "urn:dmb:utm:mongodb-outputport-subcomponent-template:0.0.0"
        )

        result = self.service.provision(self.data_product, self.component, "sub_id", remove_data=False)

        self.assertIsInstance(result, ProvisioningStatus)
        self.assertEqual(result.status, Status1.COMPLETED)

    def test_provision_service_error(self):
        component = Mock(spec=MongoDBOutputPort)
        component.specific = Mock()
        component.specific.database = "testdb"
        component.useCaseTemplateId = "urn:dmb:utm:mongodb-outputport-template:0.0.0"

        subcomponent_mock = Mock(
            id="sub_id",
            useCaseTemplateId=(
                "urn:dmb:utm:mongodb-outputport-subcomponent-template:0.0.0"
            ),
            specific=Mock(collection="testcoll", valueSchema=Mock(definition='{"bsonType": "object"}'))
        )
        component.get_typed_subcomponent_by_id.return_value = subcomponent_mock

        self.service.mongodb_mapping_service.map.return_value = {"owner": "mapped_owner"}
        self.service.mongodb_settings.useCaseTemplateId = "urn:dmb:utm:mongodb-outputport-template:0.0.0"
        self.service.mongodb_settings.useCaseTemplateSubId = (
            "urn:dmb:utm:mongodb-outputport-subcomponent-template:0.0.0"
        )

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

    def test_unprovision_preprovisioned_component(self):
        """Test unprovision with preprovisioned component (short subcomponent_id)"""
        result = self.service.unprovision(
            self.data_product, self.component, "short:id", 
            remove_data=True, is_parent_component=True
        )

        self.assertIsInstance(result, ProvisioningStatus)
        self.assertEqual(result.status, Status1.COMPLETED)

    def test_provision_validation_error_subcomponent_template_id(self):
        """Test provision with invalid subcomponent template ID"""
        subcomponent = Mock(spec=MongoDBOutputPortSubComponent)
        subcomponent.useCaseTemplateId = "wrong-template-id"
        self.component.get_typed_subcomponent_by_id.return_value = subcomponent
        
        self.settings.useCaseTemplateSubId = "correct-template-id"

        result = self.service.provision(self.data_product, self.component, "sub_id", remove_data=False)

        self.assertIsInstance(result, ValidationError)
        self.assertIn("Subcomponent use case template ID does not match", result.errors[0])

    def test_provision_validation_error_component_template_id_preprovisioned(self):
        """Test provision with preprovisioned component but wrong component template ID"""
        subcomponent = Mock(spec=MongoDBOutputPortSubComponent)
        subcomponent.useCaseTemplateId = "correct-sub-template-id"
        self.component.get_typed_subcomponent_by_id.return_value = subcomponent
        self.component.useCaseTemplateId = "wrong-component-template-id"
        
        self.settings.useCaseTemplateSubId = "correct-sub-template-id"
        self.settings.useCaseTemplateId = "correct-component-template-id"

        # Make subcomponent_id short to trigger preprovisioned logic
        result = self.service.provision(
            self.data_product, self.component, "short:id", 
            remove_data=False, is_parent_component=True
        )

        self.assertIsInstance(result, ValidationError)
        self.assertIn("Component use case template ID does not match", result.errors[0])

    def test_provision_preprovisioned_component_success(self):
        """Test provision with valid preprovisioned component"""
        subcomponent = Mock(spec=MongoDBOutputPortSubComponent)
        subcomponent.useCaseTemplateId = "correct-sub-template-id"
        self.component.get_typed_subcomponent_by_id.return_value = subcomponent
        self.component.useCaseTemplateId = "correct-component-template-id"

        self.settings.useCaseTemplateSubId = "correct-sub-template-id"
        self.settings.useCaseTemplateId = "correct-component-template-id"

        # Make subcomponent_id short to trigger preprovisioned logic
        result = self.service.provision(
            self.data_product, self.component, "short:id",
            remove_data=False, is_parent_component=True
        )

        self.assertIsInstance(result, ProvisioningStatus)
        self.assertEqual(result.status, Status1.COMPLETED)
        self.assertIn("Component already provisioned", result.info.publicInfo["message"])

    def test_provision_no_value_schema(self):
        """Test provision when subcomponent has no value schema"""
        subcomponent = Mock(spec=MongoDBOutputPortSubComponent)
        subcomponent.id = "sub_id"
        subcomponent.useCaseTemplateId = "urn:dmb:utm:mongodb-outputport-subcomponent-template:0.0.0"
        subcomponent.specific = Mock(MongoDBSubComponentSpecific)
        subcomponent.specific.collection = "testcoll"
        subcomponent.specific.valueSchema = None  # No value schema
        self.component.get_typed_subcomponent_by_id.return_value = subcomponent
        self.component.useCaseTemplateId = "urn:dmb:utm:mongodb-outputport-template:0.0.0"

        self.mapping_service.map.return_value = {"owner": "mapped_owner"}
        self.mongo_client.create_database.return_value.name = "testdb"
        self.mongo_client.create_collection.return_value.name = "testcoll"

        self.settings.developer_roles = ["dpOwner"]
        self.settings.consumer_actions = ["find"]
        self.settings.useCaseTemplateId = "urn:dmb:utm:mongodb-outputport-template:0.0.0"
        self.settings.useCaseTemplateSubId = "urn:dmb:utm:mongodb-outputport-subcomponent-template:0.0.0"

        result = self.service.provision(self.data_product, self.component, "sub_id", remove_data=False)

        self.assertIsInstance(result, ProvisioningStatus)
        self.assertEqual(result.status, Status1.COMPLETED)
        # Verify that create_collection was called with empty validator
        self.mongo_client.create_collection.assert_called_with(
            "testdb", "testcoll", validator={}
        )

    def test_provision_mapping_error(self):
        """Test provision when mapping service returns an error"""
        subcomponent = Mock(spec=MongoDBOutputPortSubComponent)
        subcomponent.useCaseTemplateId = "urn:dmb:utm:mongodb-outputport-subcomponent-template:0.0.0"
        self.component.get_typed_subcomponent_by_id.return_value = subcomponent
        
        self.settings.useCaseTemplateSubId = "urn:dmb:utm:mongodb-outputport-subcomponent-template:0.0.0"
        
        # Mock mapping service to return MappingError
        mapping_error = MappingError("User mapping failed")
        self.mapping_service.map.return_value = {"owner": mapping_error}

        result = self.service.provision(self.data_product, self.component, "sub_id", remove_data=False)

        self.assertIsInstance(result, SystemErr)
        self.assertIn("User mapping failed", result.error)
