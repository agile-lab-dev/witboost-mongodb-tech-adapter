from typing import Set

from loguru import logger

from src.models.api_models import (
    Info,
    ProvisioningStatus,
    Status1,
    SystemErr,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct
from src.models.mongodb_models import MongoDBOutputPort, MongoDBOutputPortSubComponent
from src.models.service_error import ServiceError
from src.services.acl_service import AclService
from src.services.mongo_client_service import MongoDBClientService
from src.services.principal_mapping_service import (
    MappingError,
    PrincipalMappingService,
)
from src.settings.mongodb_settings import MongoDBSettings


class UpdateAclService:
    def __init__(
        self,
        mongodb_mapping_service: PrincipalMappingService,
        acl_service: AclService,
        mongodb_client_service: MongoDBClientService,
        mongodb_settings: MongoDBSettings,
    ):
        self.mongodb_mapping_service = mongodb_mapping_service
        self.acl_service = acl_service
        self.mongodb_client_service = mongodb_client_service
        self.mongodb_settings = mongodb_settings

    def update_acls(
        self,
        data_product: DataProduct,
        component: MongoDBOutputPort,
        subcomponent_id: str,
        witboost_identities: Set[str],
    ) -> ProvisioningStatus | ValidationError | SystemErr:
        try:
            logger.info("Starting ACL update for subcomponent %s", subcomponent_id)
            subcomponent_to_provision = component.get_typed_subcomponent_by_id(
                subcomponent_id, MongoDBOutputPortSubComponent
            )

            if subcomponent_to_provision is None:
                error_msg = f"Subcomponent with ID {subcomponent_id} not found in descriptor"
                logger.error(error_msg)
                return ValidationError(errors=[error_msg])

            database_to_update = component.specific.database
            collection_to_update = subcomponent_to_provision.specific.collection
            role_to_update = f"{database_to_update}_{collection_to_update}_consumer"

            logger.info(f"Mapping identity for {witboost_identities}")
            mapped_identity = self.mongodb_mapping_service.map(witboost_identities)
            identities = mapped_identity.items()

            identities_not_mapped = set()
            identities_to_map = set()
            for witboost_identity, identity in identities:
                if isinstance(identity, MappingError):
                    error = f"Failed to map identity {witboost_identity}: {identity.error}"
                    logger.error(error)
                    identities_not_mapped.add(error)
                else:
                    logger.info(f"Mapped {witboost_identity} to {identity}")
                    identities_to_map.add(identity)

            logger.info(f"Removing existing acls for {identities_to_map} ")
            errors_removal, removed_users = self.acl_service.remove_all_acls_for_principals(
                database=database_to_update,
                role=role_to_update,
                principals=identities_to_map,
            )

            logger.info(f"Applying acls to {identities_to_map}")
            errors_application, granted_users = self.acl_service.apply_acls_to_principals(
                database=database_to_update,
                role=role_to_update,
                principals=identities_to_map,
            )

            errors = []
            if errors_removal or errors_application or identities_not_mapped:
                logger.info("Errors occurred during ACL update")
                if errors_removal:
                    errors.extend(errors_removal)
                if errors_application:
                    errors.extend(errors_application)
                if identities_not_mapped:
                    errors.extend({str(identity) for identity in identities_not_mapped})
                logger.error(f"Errors occurred while updating ACLs: {errors}")
                return ProvisioningStatus(
                    status=Status1.FAILED,
                    result="",
                    info=Info(publicInfo=self._get_public_info(errors, granted_users, removed_users), privateInfo={}),
                )
            else:
                logger.info("No errors occurred while updating ACLs, proceeding with successful completion")
                logger.info("ACL update completed successfully")

            return ProvisioningStatus(
                status=Status1.COMPLETED,
                result="",
                info=Info(
                    publicInfo=self._get_public_info(
                        errors=errors,
                        granted_users=granted_users,
                        removed_users=removed_users,
                    ),
                    privateInfo={},
                ),
            )
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def _get_public_info(
        self,
        errors: list[str],
        granted_users: list[str],
        removed_users: list[PrincipalMappingService],
    ) -> dict:
        public_info = dict()
        if errors:
            public_info["errors"] = errors
        else:
            public_info["updated_acls"] = [user for user in granted_users]
            public_info["removed_acls"] = [str(user) for user in removed_users]
        return public_info
