import unittest
from unittest.mock import Mock

from src.models.api_models import ProvisioningStatus, Status1, ValidationError
from src.models.data_product_descriptor import DataProduct
from src.models.mongodb_models import (
    MongoDBComponentSpecific,
    MongoDBOutputPort,
    MongoDBOutputPortSubComponent,
    MongoDBSubComponentSpecific,
)
from src.services.acl_service import AclService
from src.services.mongo_client_service import MongoDBClientService
from src.services.principal_mapping_service import PrincipalMappingService
from src.services.update_acl_service import UpdateAclService


class UpdateAclServiceTest(unittest.TestCase):
    def setUp(self):
        self.mock_mapping_service = Mock(PrincipalMappingService)
        self.mock_acl_service = Mock(AclService)
        self.mock_mongo_client_service = Mock(MongoDBClientService)
        self.mock_settings = Mock(MongoDBClientService)

        self.service = UpdateAclService(
            mongodb_mapping_service=self.mock_mapping_service,
            acl_service=self.mock_acl_service,
            mongodb_client_service=self.mock_mongo_client_service,
            mongodb_settings=self.mock_settings,
        )

        self.data_product = Mock(spec=DataProduct)
        self.output_port = Mock(spec=MongoDBOutputPort)
        self.subcomponent = Mock(spec=MongoDBOutputPortSubComponent)

        self.output_port.get_typed_subcomponent_by_id.return_value = self.subcomponent
        self.output_port.specific = Mock(MongoDBComponentSpecific)
        self.output_port.specific.database = "mydb"
        self.subcomponent.specific = Mock(MongoDBSubComponentSpecific)
        self.subcomponent.specific.collection = "mycollection"

    def test_successful_acl_update(self):
        self.mock_mapping_service.map.return_value = {"user:alice": "principal_user"}

        self.mock_acl_service.remove_all_acls_for_principals.return_value = (None, ["principal_user"])
        self.mock_acl_service.apply_acls_to_principals.return_value = (None, ["principal_user"])

        result = self.service.update_acls(
            data_product=self.data_product,
            component=self.output_port,
            subcomponent_id="sub1",
            witboost_identities=["user:alice"],
        )

        self.assertIsInstance(result, ProvisioningStatus)
        self.assertEqual(result.status, Status1.COMPLETED)

    def test_subcomponent_not_found_returns_validation_error(self):
        self.output_port.get_typed_subcomponent_by_id.return_value = None

        result = self.service.update_acls(
            data_product=self.data_product,
            component=self.output_port,
            subcomponent_id="invalid_sub",
            witboost_identities=["user:alice"],
        )

        self.assertIsInstance(result, ValidationError)
        self.assertIn("Subcomponent with ID", result.errors[0])

    def test_acl_removal_error_returns_failed_status(self):
        self.mock_mapping_service.map.return_value = {"user:alice": "principal_user"}
        self.mock_acl_service.remove_all_acls_for_principals.return_value = (["error1"], [])
        self.mock_acl_service.apply_acls_to_principals.return_value = (None, [])

        result = self.service.update_acls(
            data_product=self.data_product,
            component=self.output_port,
            subcomponent_id="sub1",
            witboost_identities=["user:alice"],
        )

        self.assertIsInstance(result, ProvisioningStatus)
        self.assertEqual(result.status, Status1.FAILED)
        self.assertIn("error1", result.info.publicInfo["errors"])
        self.assertEqual(result.info.publicInfo["errors"], ["error1"])
