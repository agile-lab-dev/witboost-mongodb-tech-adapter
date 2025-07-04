import unittest

from src.models.data_product_descriptor import ComponentKind, DataContract, DataSharingAgreement
from src.models.mongodb_models import (
    MongoDBComponentSpecific,
    MongoDBOutputPort,
    MongoDBOutputPortSubComponent,
    MongoDBSchema,
    MongoDBSubComponentSpecific,
)


class TestMongoDBModels(unittest.TestCase):
    def setUp(self):
        # Create a sample MongoDBOutputPort object for testing
        self.sample_mongodb_outputport = MongoDBOutputPort(
            id="op1",
            name="Output Port 1",
            description="An output port",
            kind=ComponentKind.OUTPUTPORT,
            version="1.0",
            infrastructureTemplateId="infra1",
            outputPortType="Type1",
            dependsOn=["op2"],  # Depends on another output port within the same Data Product
            dataContract=DataContract(schema=[]),  # Provide valid schema instances
            dataSharingAgreement=DataSharingAgreement(),
            tags=[],
            semanticLinking=[],
            consumable=False,
            shoppable=False,
            specific=MongoDBComponentSpecific(database="sample_database"),
            components=[
                MongoDBOutputPortSubComponent(
                    id="sub_op1",
                    name="Output Port 1",
                    description="An output port",
                    kind=ComponentKind.OUTPUTPORT,
                    version="1.0",
                    infrastructureTemplateId="infra1",
                    outputPortType="Type1",
                    dependsOn=["op2"],  # Depends on another output port within the same Data Product
                    dataContract=DataContract(schema=[]),  # Provide valid schema instances
                    dataSharingAgreement=DataSharingAgreement(),
                    tags=[],
                    semanticLinking=[],
                    consumable=False,
                    shoppable=False,
                    specific=MongoDBSubComponentSpecific(
                        collection="sample_collection",
                        valueSchema=MongoDBSchema(type="JSON", definition='{"key": "value"}'),
                    ),
                ),
                MongoDBOutputPortSubComponent(
                    id="sub_op2",
                    name="Output Port 2",
                    description="An output port",
                    kind=ComponentKind.OUTPUTPORT,
                    version="1.0",
                    infrastructureTemplateId="infra2",
                    outputPortType="Type2",
                    dependsOn=["op2"],  # Depends on another output port within the same Data Product
                    dataContract=DataContract(schema=[]),  # Provide valid schema instances
                    dataSharingAgreement=DataSharingAgreement(),
                    tags=[],
                    semanticLinking=[],
                    consumable=False,
                    shoppable=False,
                    specific=MongoDBSubComponentSpecific(
                        collection="sample_collection",
                        valueSchema=MongoDBSchema(type="JSON", definition='{"key": "value"}'),
                    ),
                ),
            ],
        )

    def test_get_components_by_kind_outputport(self):
        output_ports_subcomponents = self.sample_mongodb_outputport.get_subcomponents_by_kind(ComponentKind.OUTPUTPORT)
        self.assertEqual(2, len(output_ports_subcomponents))
        self.assertIsInstance(output_ports_subcomponents[0], MongoDBOutputPortSubComponent)

    def test_get_component_by_id_existing(self):
        component_id = "sub_op1"
        component = self.sample_mongodb_outputport.get_subcomponent_by_id(component_id)
        self.assertIsNotNone(component)
        self.assertEqual(component.id, component_id)

    def test_get_component_by_id_non_existing(self):
        component_id = "nonexistent"
        component = self.sample_mongodb_outputport.get_subcomponent_by_id(component_id)
        self.assertIsNone(component)
