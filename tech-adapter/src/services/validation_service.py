from typing import Annotated

import pydantic
from fastapi import Depends
from loguru import logger

from src.dependencies import UnpackedProvisioningRequestDep
from src.models.api_models import ProvisioningRequestMongoDB, ValidationError
from src.models.mongodb_models import MongoDBOutputPort


def validate_mongodb_output_port(
    request: UnpackedProvisioningRequestDep,
) -> ProvisioningRequestMongoDB | ValidationError:
    if isinstance(request, ValidationError):
        return request

    data_product, subcomponent_id, remove_data = request

    component_id = subcomponent_id.rsplit(":", 1)[0]

    try:
        component_to_provision = data_product.get_typed_component_by_id(component_id, MongoDBOutputPort)
    except pydantic.ValidationError as ve:
        error_msg = f"Failed to parse the component {component_id} as a MongoDB OutputPort:"
        logger.exception(error_msg)
        combined = [error_msg]
        combined.extend(
            map(
                str,
                ve.errors(include_url=False, include_context=False, include_input=False),
            )
        )
        return ValidationError(errors=combined)

    if component_to_provision is None:
        error_msg = f"Component with ID {component_id} not found in descriptor"
        logger.error(error_msg)
        return ValidationError(errors=[error_msg])

    return ProvisioningRequestMongoDB(
        dataProduct=data_product,
        component=component_to_provision,
        subcomponentId=subcomponent_id,
        removeData=remove_data,
    )


ValidateMongoDBOutputPortDep = Annotated[
    ProvisioningRequestMongoDB | ValidationError,
    Depends(validate_mongodb_output_port),
]
