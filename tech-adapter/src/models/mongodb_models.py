import json
from typing import Literal, Type

from loguru import logger
from pydantic import AfterValidator, BaseModel, BeforeValidator, TypeAdapter
from typing_extensions import Annotated, List

from src.models.data_product_descriptor import OutputPort, component_map


def check_json(value: str) -> str:
    try:
        json.loads(value)
        return value
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e


class MongoDBSchema(BaseModel):
    type: Literal["JSON"]
    definition: Annotated[str, AfterValidator(check_json)]


class MongoDBComponentSpecific(BaseModel):
    database: str


class MongoDBSubComponentSpecific(BaseModel):
    collection: str
    valueSchema: MongoDBSchema | None = None


class MongoDBOutputPortSubComponent(OutputPort):
    consumable: bool
    shoppable: bool
    specific: MongoDBSubComponentSpecific


def parse_subcomponent(data: dict | MongoDBOutputPortSubComponent) -> MongoDBOutputPortSubComponent:
    if isinstance(data, MongoDBOutputPortSubComponent):
        if data.kind not in component_map:
            raise ValueError(f"Unknown component kind: {data.kind}")
        return data
    else:
        kind = data.get("kind")
        if kind not in component_map:
            raise ValueError(f"Unknown component kind: {kind}")
        if issubclass(MongoDBOutputPortSubComponent, component_map[kind]):
            adapter = TypeAdapter(MongoDBOutputPortSubComponent)
            component = adapter.validate_python(data)
        else:
            raise ValueError(f"Component kind {kind} does not have a subcomponent.")
        logger.debug("Parsed component: " + str(component))
        return component


class MongoDBOutputPort(OutputPort):
    components: List[Annotated[MongoDBOutputPortSubComponent, BeforeValidator(parse_subcomponent)]]
    consumable: bool
    shoppable: bool
    specific: MongoDBComponentSpecific

    def get_subcomponent_by_id(self, subcomponent_id: str) -> MongoDBOutputPortSubComponent | None:
        """
        Retrieve a subcomponent within the parent component by its unique identifier.

        This method searches for a component with the specified ID within the components's
        list of subcomponents and returns the matching subcomponent, if found.

        Args:
            subcomponent_id (str): The unique identifier of the subcomponent to retrieve.

        Returns:
            MongoDBOutputPortSubComponent | None: The MongoDBOutputPortSubComponent object with the specified ID if found, or None if
            no matching subcomponent is found.

        Example:
           To retrieve a specific subcomponent with ID '12345' from a components 'my_component':
           >>> specific_subcomponent = my_component.get_subcomponent_by_id('12345')
           >>> if specific_subcomponent:
           ...     print(f"Found component: {specific_subcomponent.name}")
           ... else:
           ...     print("Component not found.")
        """  # noqa: E501
        for subcomponent in self.components:
            if subcomponent.id == subcomponent_id:
                return subcomponent
        return None

    def get_typed_subcomponent_by_id(self, subcomponent_id: str, subcomponent_type: Type[BaseModel]):
        subcomponent = self.get_subcomponent_by_id(subcomponent_id)
        if subcomponent is not None:
            return subcomponent_type.model_validate(subcomponent.model_dump(by_alias=True, mode="python"))
        else:
            return None

    def get_subcomponents_by_kind(self, kind: str) -> List[MongoDBOutputPortSubComponent]:
        """
        Filters the subcomponents associated with the component and returns
        a list containing only the subcomponents that have the specified kind.

        Args:
            kind (str): The kind of subcomponents to retrieve.

        Returns:
            List[MongoDBOutputPortSubComponent]: A list of MongoDBOutputPortSubComponent objects that match the specified kind.
            If no matching components are found, an empty list is returned.

        Example:
            To retrieve all subcomponents of kind 'outputport' from a components 'my_component':
            >>> outputport_components = my_component.get_subcomponents_by_kind('outputport')
        """  # noqa: E501

        new_subcomponents_list = [subcomponent for subcomponent in self.components if subcomponent.kind == kind]

        return new_subcomponents_list
