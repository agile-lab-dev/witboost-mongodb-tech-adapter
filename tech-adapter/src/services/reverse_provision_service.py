import json

from loguru import logger
from pyparsing import Any

from src.models.api_models import (
    ReverseProvisioningRequest,
    ReverseProvisioningStatus,
    Status1,
    SystemErr,
    ValidationError,
)
from src.models.service_error import ServiceError
from src.services.mongo_client_service import MongoDBClientService


class ReverseProvisionService:
    def __init__(
        self,
        mongodb_client_service: MongoDBClientService,
    ):
        self.mongodb_client_service = mongodb_client_service

    def reverse_provision(
        self,
        request: ReverseProvisioningRequest,
    ) -> ReverseProvisioningStatus | ValidationError | SystemErr:
        try:
            logger.info("Starting reverse provisioning")
            if request.params is None:
                logger.debug("No parameters provided for reverse provisioning")

            if isinstance(request.params, dict):
                collections = request.params.get("collections", None)
                if not collections:
                    logger.debug("No collections specified for reverse provisioning")

                database = request.params.get("database", None)
                if not database:
                    logger.debug("No database specified for reverse provisioning")
                    return ValidationError(errors=["No database specified"])
            else:
                logger.debug("Invalid parameters format for reverse provisioning")
                return ValidationError(errors=["Invalid parameters format"])

            collection_info = self.mongodb_client_service.get_collections_info(database, collections)

            logger.info(f"Reverse provisioning completed for database {database}")

            return ReverseProvisioningStatus(
                status=Status1.COMPLETED,
                updates=self._get_updates(collection_info, database, request.environment),
            )
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def _get_updates(
        self, collection_info: list[tuple[str, dict | None]], database: str, environment: str
    ) -> dict[str, Any]:
        updates = {
            "parameters": {
                "subcomponentDefinition": {
                    "components": [
                        {
                            "description": collection,
                            "collection": collection,
                            **({"jsonschema": json.dumps(validator)} if validator else {}),
                        }
                        for collection, validator in collection_info
                    ]
                }
            },
            "environmentParameters": {environment: {"database": database}},
        }
        return updates
