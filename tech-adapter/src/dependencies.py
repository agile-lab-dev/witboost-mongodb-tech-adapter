from typing import Annotated, Tuple

import yaml
from fastapi import Depends

from src.models.api_models import (
    DescriptorKind,
    ProvisioningRequest,
    UpdateAclRequest,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct
from src.models.mongodb_models import MongoDBOutputPort
from src.services.acl_service import AclService
from src.services.mongo_client_service import MongoDBClientService
from src.services.principal_mapping_service import PrincipalMappingService
from src.services.provision_service import ProvisionService
from src.services.update_acl_service import UpdateAclService
from src.settings.mongodb_settings import MongoDBSettings
from src.utility.parsing_pydantic_models import parse_yaml_with_model


async def unpack_provisioning_request(
    provisioning_request: ProvisioningRequest,
) -> Tuple[DataProduct, str, bool] | ValidationError:
    """
    Unpacks a Provisioning Request.

    This function takes a `ProvisioningRequest` object and extracts relevant information
    to perform provisioning or unprovisioning for a data product component.

    Args:
        provisioning_request (ProvisioningRequest): The request to be unpacked.

    Returns:
        Union[Tuple[DataProduct, str, bool], ValidationError]:
            - If successful, returns a tuple containing:
                - `DataProduct`: The data product for provisioning.
                - `str`: The component ID to provision.
                - `bool`: The value of the removeData field.
            - If unsuccessful, returns a `ValidationError` object with error details.

    Note:
        - This function expects the `provisioning_request` to have a descriptor kind of `DescriptorKind.COMPONENT_DESCRIPTOR`.
        - It will attempt to parse the descriptor and return the relevant information. If parsing fails or the descriptor kind is unexpected, a `ValidationError` will be returned.

    """  # noqa: E501

    if not provisioning_request.descriptorKind == DescriptorKind.COMPONENT_DESCRIPTOR:
        error = (
            "Expecting a COMPONENT_DESCRIPTOR but got a "
            f"{provisioning_request.descriptorKind} instead; please check with the "
            f"platform team."
        )
        return ValidationError(errors=[error])
    try:
        descriptor_dict = yaml.safe_load(provisioning_request.descriptor)
        data_product = parse_yaml_with_model(descriptor_dict.get("dataProduct"), DataProduct)
        component_to_provision = descriptor_dict.get("componentIdToProvision")
        remove_data = provisioning_request.removeData if provisioning_request.removeData is not None else False

        if isinstance(data_product, DataProduct):
            return data_product, component_to_provision, remove_data
        elif isinstance(data_product, ValidationError):
            return data_product

        else:
            return ValidationError(
                errors=[
                    "An unexpected error occurred while parsing the provisioning request."  # noqa: E501
                ]
            )

    except Exception as ex:
        return ValidationError(errors=["Unable to parse the descriptor.", str(ex)])


UnpackedProvisioningRequestDep = Annotated[
    Tuple[DataProduct, str, bool] | ValidationError,
    Depends(unpack_provisioning_request),
]


async def unpack_update_acl_request(
    update_acl_request: UpdateAclRequest,
) -> Tuple[DataProduct, MongoDBOutputPort, str, list[str]] | ValidationError:
    """
    Unpacks an Update ACL Request.

    This function takes an `UpdateAclRequest` object and extracts relevant information
    to update access control lists (ACL) for a data product.

    Args:
        update_acl_request (UpdateAclRequest): The update ACL request to be unpacked.

    Returns:
        Union[Tuple[DataProduct, MongoDBOutputPort, str, List[str]], ValidationError]:
            - If successful, returns a tuple containing:
                - `DataProduct`: The data product to update ACL for.
                - `MongoDBOutputPort`: The component to provision.
                - `str`: The subcomponent ID to provision.
                - `List[str]`: A list of references.
            - If unsuccessful, returns a `ValidationError` object with error details.

    Note:
        This function expects the `update_acl_request` to contain a valid YAML string
        in the 'provisionInfo.request' field. It will attempt to parse the YAML and
        return the relevant information. If parsing fails, a `ValidationError` will
        be returned.

    """  # noqa: E501

    try:
        request = yaml.safe_load(update_acl_request.provisionInfo.request)
        data_product = parse_yaml_with_model(request.get("dataProduct"), DataProduct)
        subcomponent_to_provision: str = request.get("componentIdToProvision")

        component_id = subcomponent_to_provision.rsplit(":", 1)[0]
        if not isinstance(data_product, DataProduct):
            return ValidationError(errors=["Invalid data product in the request."])
        component_to_provision: MongoDBOutputPort = data_product.get_typed_component_by_id(
            component_id, MongoDBOutputPort
        )

        if isinstance(data_product, DataProduct):
            return (
                data_product,
                component_to_provision,
                subcomponent_to_provision,
                update_acl_request.refs,
            )
        elif isinstance(data_product, ValidationError):
            return data_product
        else:
            return ValidationError(errors=["An unexpected error occurred while parsing the update acl request."])
    except Exception as ex:
        return ValidationError(errors=["Unable to parse the descriptor.", str(ex)])


UnpackedUpdateAclRequestDep = Annotated[
    Tuple[DataProduct, MongoDBOutputPort, str, list[str]] | ValidationError,
    Depends(unpack_update_acl_request),
]


def get_mongodb_settings() -> MongoDBSettings:
    return MongoDBSettings()


def get_mongodb_client_service(
    mongodb_settings: Annotated[MongoDBSettings, Depends(get_mongodb_settings)],
) -> MongoDBClientService:
    return MongoDBClientService(mongodb_settings)


def get_mapping_service() -> PrincipalMappingService:
    return PrincipalMappingService()


def get_provision_service(
    mongodb_client_service: Annotated[MongoDBClientService, Depends(get_mongodb_client_service)],
    mongodb_mapping_service: Annotated[PrincipalMappingService, Depends(get_mapping_service)],
    mongodb_settings: Annotated[MongoDBSettings, Depends(get_mongodb_settings)],
) -> ProvisionService:
    return ProvisionService(mongodb_client_service, mongodb_mapping_service, mongodb_settings)


ProvisionServiceDep = Annotated[ProvisionService, Depends(get_provision_service)]


def get_acl_service(
    mongodb_settings: Annotated[MongoDBSettings, Depends(get_mongodb_settings)],
) -> AclService:
    return AclService(mongodb_settings)


def get_update_acl_service(
    principal_mapping_service: Annotated[PrincipalMappingService, Depends(get_mapping_service)],
    acl_service: Annotated[AclService, Depends(get_acl_service)],
    mongodb_client_service: Annotated[MongoDBClientService, Depends(get_mongodb_client_service)],
    mongodb_settings: Annotated[MongoDBSettings, Depends(get_mongodb_settings)],
) -> UpdateAclService:
    return UpdateAclService(principal_mapping_service, acl_service, mongodb_client_service, mongodb_settings)


UpdateAclServiceDep = Annotated[
    UpdateAclService,
    Depends(get_update_acl_service),
]
