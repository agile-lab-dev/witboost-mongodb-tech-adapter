import unittest
from unittest.mock import MagicMock, Mock

from src.models.api_models import (
    ReverseProvisioningRequest,
    ReverseProvisioningStatus,
    Status1,
    SystemErr,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct
from src.models.mongodb_models import (
    MongoDBComponentSpecific,
    MongoDBOutputPort,
)
from src.models.service_error import ServiceError
from src.services.reverse_provision_service import ReverseProvisionService


class TestReverseProvisionService(unittest.TestCase):
    def setUp(self):
        self.service = ReverseProvisionService(
            mongodb_client_service=MagicMock(),
        )

        self.data_product = Mock(spec=DataProduct)
        self.data_product.dataProductOwner = "owner"

        self.component = Mock(spec=MongoDBOutputPort)
        self.component.specific = Mock(MongoDBComponentSpecific)
        self.component.specific.database = "testdb"

    def test_reverse_provision_success(self):
        self.service.mongodb_client_service.get_collections_info.return_value = [("testcoll", {"bsonType": "object"})]

        result = self.service.reverse_provision(
            ReverseProvisioningRequest(
                useCaseTemplateId="test", params={"database": "testdb", "collections": ["testcoll"]}, environment="test"
            )
        )

        self.assertIsInstance(result, ReverseProvisioningStatus)
        self.assertEqual(result.status, Status1.COMPLETED)

    def test_reverse_provision_validation_error(self):
        self.service.mongodb_client_service.get_collections_info.return_value = [("testcoll", {"bsonType": "object"})]

        request = ReverseProvisioningRequest(useCaseTemplateId="test", params={}, environment="test")
        result = self.service.reverse_provision(request)

        self.assertIsInstance(result, ValidationError)
        self.assertIn("No database specified", result.errors)

    def test_reverse_provision_system_error(self):
        self.service.mongodb_client_service.get_collections_info.side_effect = ServiceError(error_msg="Database error")

        result = self.service.reverse_provision(
            ReverseProvisioningRequest(
                useCaseTemplateId="test",
                params={"database": "testdb", "collections": ["testcoll"]},
                environment="test",
            )
        )

        self.assertIsInstance(result, SystemErr)
        self.assertIn("Database error", result.error)

    def test_reverse_provision_no_database(self):
        result = self.service.reverse_provision(
            ReverseProvisioningRequest(
                useCaseTemplateId="test",
                params={"collections": ["testcoll"]},
                environment="test",
            )
        )

        self.assertIsInstance(result, ValidationError)
        self.assertIn("No database specified", result.errors)
