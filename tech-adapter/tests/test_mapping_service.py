import unittest

from src.services.principal_mapping_service import PrincipalMappingService


class TestPrincipalMappingService(unittest.TestCase):
    def setUp(self):
        self.service = PrincipalMappingService()

    def test_map_valid_user_with_underscore(self):
        result = self.service.map({"user:john_doe"})
        self.assertEqual(result, {"user:john_doe": "john@doe"})

    def test_map_user_without_underscore(self):
        result = self.service.map({"user:admin"})
        self.assertEqual(result, {"user:admin": "admin"})
