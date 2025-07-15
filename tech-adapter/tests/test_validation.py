import unittest
from pathlib import Path

import pydantic_core
import pytest
import yaml

from src.models.data_product_descriptor import (
    DataProduct,
)
from src.models.mongodb_models import MongoDBOutputPort
from src.utility.parsing_pydantic_models import parse_yaml_with_model


class TestValidation(unittest.TestCase):
    def test_valid_mongodb_output_port(self):
        descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()
        request = yaml.safe_load(descriptor_str)
        data_product = parse_yaml_with_model(request.get("dataProduct"), DataProduct)
        subcomponent_to_provision = request.get("componentIdToProvision")
        component_to_provision = subcomponent_to_provision.rsplit(":", 1)[0]

        component = data_product.get_typed_component_by_id(component_to_provision, MongoDBOutputPort)
        assert component is not None

        assert isinstance(component, MongoDBOutputPort)

    def test_invalid_mongodb_output_port(self):
        descriptor_str = Path("tests/descriptors/descriptor_storage_valid.yaml").read_text()
        request = yaml.safe_load(descriptor_str)
        data_product = parse_yaml_with_model(request.get("dataProduct"), DataProduct)
        invalid_subcomponent_to_provision = request.get("componentIdToProvision")
        invalid_component_to_provision = invalid_subcomponent_to_provision.rsplit(":", 1)[0]

        assert (
            ":" in invalid_component_to_provision
        ), "Invalid subcomponent ID format: not expecting ':' in component_id"

        invalid_component = data_product.get_typed_component_by_id(invalid_component_to_provision, MongoDBOutputPort)
        assert invalid_component is None

        assert not isinstance(invalid_component, MongoDBOutputPort)

        with pytest.raises(pydantic_core.ValidationError, match="7 validation errors for MongoDBOutputPort"):
            data_product.get_typed_component_by_id(invalid_subcomponent_to_provision, MongoDBOutputPort)
