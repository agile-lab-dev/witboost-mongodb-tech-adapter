import json

from loguru import logger

from src.models.api_models import Info, ProvisioningStatus, Status1, SystemErr, ValidationError
from src.models.data_product_descriptor import DataProduct
from src.models.mongodb_models import MongoDBOutputPort, MongoDBOutputPortSubComponent
from src.models.service_error import ServiceError
from src.services.mongo_client_service import MongoDBClientService
from src.services.principal_mapping_service import MappingError, PrincipalMappingService
from src.settings.mongodb_settings import MongoDBSettings

LEN_SUBCOMPONENT_ID = 8


class ProvisionService:
    def __init__(
        self,
        mongodb_client_service: MongoDBClientService,
        mongodb_mapping_service: PrincipalMappingService,
        mongodb_settings: MongoDBSettings,
    ):
        self.mongodb_client_service = mongodb_client_service
        self.mongodb_mapping_service = mongodb_mapping_service
        self.mongodb_settings = mongodb_settings

    def provision(
        self,
        data_product: DataProduct,
        component: MongoDBOutputPort,
        subcomponent_id: str,
        remove_data: bool | None = None,
        is_parent_component: bool | None = None,
    ) -> ProvisioningStatus | SystemErr | ValidationError:
        try:
            if len(subcomponent_id.split(":")) < LEN_SUBCOMPONENT_ID and is_parent_component:
                if component.useCaseTemplateId != self.mongodb_settings.useCaseTemplateId:
                    return ValidationError(
                        errors=[
                            f"Component use case template ID does not match: "
                            f"component='{component.useCaseTemplateId}', "
                            f"expected='{self.mongodb_settings.useCaseTemplateId}'"
                        ]
                    )
                return ProvisioningStatus(
                    status=Status1.COMPLETED,
                    result="",
                    info=Info(
                        publicInfo={"message": "Component already provisioned, no action taken"},
                        privateInfo=dict(),
                    ),
                )

            logger.info(f"Starting provisioning for subcomponent {subcomponent_id}")

            subcomponent = component.get_typed_subcomponent_by_id(subcomponent_id, MongoDBOutputPortSubComponent)

            if subcomponent.useCaseTemplateId != self.mongodb_settings.useCaseTemplateSubId:
                return ValidationError(
                    errors=[
                        f"Subcomponent use case template ID does not match: "
                        f"component='{subcomponent.useCaseTemplateId}', "
                        f"expected='{self.mongodb_settings.useCaseTemplateSubId}'"
                    ]
                )

            database_name = component.specific.database
            logger.info(f"Managing database {database_name}")

            mapping = self.mongodb_mapping_service.map({data_product.dataProductOwner})
            mapped_user = mapping[data_product.dataProductOwner]

            if isinstance(mapped_user, MappingError):
                logger.error(f"Failed to map user {data_product.dataProductOwner}: {mapped_user.error}")
                return SystemErr(error=mapped_user.error)

            user: str = mapped_user

            db = self.mongodb_client_service.create_database(database_name)
            logger.info(f"Database {db.name} managed successfully")

            logger.info(f"Creating role for database {database_name}")

            # Set the developer role
            dev_role = f"{database_name}_developer"
            self.mongodb_client_service.create_or_update_developer_role(
                database_name=database_name,
                user=user,
                role_name=dev_role,
                roles=[{"role": role, "db": database_name} for role in self.mongodb_settings.developer_roles],
            )

            logger.info(f"Creating collection {subcomponent.specific.collection}")
            if not subcomponent.specific.valueSchema:
                logger.warning(f"No value schema provided for subcomponent {subcomponent_id}, using empty schema")
            else:
                logger.info(f"Using value schema for subcomponent {subcomponent_id}")
                validator = json.loads(subcomponent.specific.valueSchema.definition)
            collection = self.mongodb_client_service.create_collection(
                component.specific.database,
                subcomponent.specific.collection,
                validator=validator if subcomponent.specific.valueSchema else {},
            )
            logger.info(f"Collection {collection.name} created successfully")

            logger.info(f"Creation of consumer role for collection {subcomponent.specific.collection}")
            self.mongodb_client_service.create_or_update_consumer_role(
                database_name=component.specific.database,
                collection_name=subcomponent.specific.collection,
                actions=self.mongodb_settings.consumer_actions,
            )
            logger.info(f"Consumer role created successfully for collection {subcomponent.specific.collection}")

            logger.info(f"Successfully provisioned subcomponent {subcomponent}")
            return ProvisioningStatus(
                status=Status1.COMPLETED,
                result="",
                info=Info(
                    publicInfo=self._get_public_info(component, subcomponent),
                    privateInfo=dict(),
                ),
            )
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def _get_public_info(self, component: MongoDBOutputPort, subcomponent: MongoDBOutputPortSubComponent) -> dict:
        public_info = dict()
        if isinstance(subcomponent, MongoDBOutputPortSubComponent):
            public_info["subcomponent_id"] = {
                "type": "string",
                "label": "Subcomponent ID",
                "value": subcomponent.id,
            }
        public_info["database"] = {
            "type": "string",
            "label": "Database Name",
            "value": component.specific.database,
        }
        public_info["collection"] = {
            "type": "string",
            "label": "Collection Name",
            "value": subcomponent.specific.collection,
        }
        return public_info

    def unprovision(
        self,
        data_product: DataProduct,
        component: MongoDBOutputPort,
        subcomponent_id: str,
        remove_data: bool | None = None,
        is_parent_component: bool | None = None,
    ) -> ProvisioningStatus | SystemErr:
        try:
            if len(subcomponent_id.split(":")) < LEN_SUBCOMPONENT_ID and is_parent_component:
                return ProvisioningStatus(status=Status1.COMPLETED, result="")

            logger.info(f"Starting unprovisioning for subcomponent {subcomponent_id}")
            subcomponent = component.get_typed_subcomponent_by_id(subcomponent_id, MongoDBOutputPortSubComponent)

            if remove_data:
                logger.info(f"Removing data for subcomponent {subcomponent_id}")
                self.mongodb_client_service.drop_collection(
                    database_name=component.specific.database,
                    collection_name=subcomponent.specific.collection,
                )
                logger.info("Collection removed successfully")

            logger.info(f"Removing role from consumer for collection {subcomponent.specific.collection}")
            self.mongodb_client_service.remove_role_from_consumer(
                database_name=component.specific.database,
                collection_name=subcomponent.specific.collection,
            )
            logger.info(f"Role removed successfully for collection {subcomponent.specific.collection}")

            logger.info(f"Successfully unprovisioned subcomponent {subcomponent_id}")
            return ProvisioningStatus(status=Status1.COMPLETED, result="")
        except ServiceError as se:
            return SystemErr(error=se.error_msg)
