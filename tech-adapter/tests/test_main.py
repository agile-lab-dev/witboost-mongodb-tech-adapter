from pathlib import Path
from unittest.mock import Mock

from fastapi.encoders import jsonable_encoder
from starlette.testclient import TestClient

from src.dependencies import get_provision_service, get_update_acl_service
from src.main import app
from src.models.api_models import (
    DescriptorKind,
    ProvisionInfo,
    ProvisioningRequest,
    ProvisioningStatus,
    UpdateAclRequest,
)
from src.services.provision_service import ProvisionService
from src.services.update_acl_service import UpdateAclService

client = TestClient(app)


# TODO refine tests
expected_valid_unprovision_response = ProvisioningStatus(
    status="COMPLETED",
    result="",
    info=None,
)

expected_valid_provision_response = ProvisioningStatus(
    status="COMPLETED",
    result="",
    info={
        "publicInfo": {
            "subcomponent_id": {
                "type": "string",
                "label": "Subcomponent ID",
                "value": "urn:dmb:cmp:healthcare:vaccinations:0:mongodb-output-port:mongodb-output-port-sub-component",
            },
            "database": {"type": "string", "label": "Database Name", "value": "database"},
            "collection": {"type": "string", "label": "Collection Name", "value": "collection2"},
        },
        "privateInfo": {},
    },
)

expected_valid_update_acl_response = ProvisioningStatus(
    status="COMPLETED", result="", info={"publicInfo": {"updated_acls": [], "removed_acls": []}, "privateInfo": {}}
)


def override_dependency() -> ProvisionService:
    mock = Mock()
    mock.provision.return_value = expected_valid_provision_response
    mock.unprovision.return_value = expected_valid_unprovision_response
    return mock


def override_acl_dependency() -> UpdateAclService:
    mock = Mock(spec=UpdateAclService)
    mock.update_acls.return_value = expected_valid_update_acl_response
    return mock


app.dependency_overrides[get_provision_service] = override_dependency
app.dependency_overrides[get_update_acl_service] = override_acl_dependency


def test_provisioning_invalid_descriptor():
    provisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor="descriptor"
    )

    resp = client.post("/v1/provision", json=dict(provisioning_request))

    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_provisioning_valid_descriptor():
    descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()

    provisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor=descriptor_str, removeData=False
    )

    resp = client.post("/v1/provision", json=dict(provisioning_request))

    assert resp.status_code == 200


def test_unprovisioning_invalid_descriptor():
    unprovisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor="descriptor"
    )

    resp = client.post("/v1/unprovision", json=dict(unprovisioning_request))

    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_unprovisioning_valid_descriptor():
    descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()

    unprovisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor=descriptor_str, removeData=True
    )

    resp = client.post("/v1/unprovision", json=dict(unprovisioning_request))

    assert resp.status_code == 200
    assert resp.json() == {"status": "COMPLETED", "result": "", "info": None}


def test_validate_invalid_descriptor():
    validate_request = ProvisioningRequest(descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor="descriptor")

    resp = client.post("/v1/validate", json=dict(validate_request))

    assert resp.status_code == 200
    assert "Unable to parse the descriptor." in resp.json().get("error").get("errors")


def test_validate_valid_descriptor():
    descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()

    validate_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor=descriptor_str
    )

    resp = client.post("/v1/validate", json=dict(validate_request))

    assert resp.status_code == 200
    assert {"valid": True, "error": None} == resp.json()


def test_updateacl_invalid_descriptor():
    updateacl_request = UpdateAclRequest(
        provisionInfo=ProvisionInfo(request="descriptor", result=""),
        refs=["user:alice", "user:bob"],
    )

    resp = client.post("/v1/updateacl", json=jsonable_encoder(updateacl_request))

    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_updateacl_valid_descriptor():
    descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()

    updateacl_request = UpdateAclRequest(
        provisionInfo=ProvisionInfo(request=descriptor_str, result=""),
        refs=["user:alice", "user:bob"],
    )

    resp = client.post("/v1/updateacl", json=jsonable_encoder(updateacl_request))

    assert resp.status_code == 200
    assert resp.json() == {
        "status": "COMPLETED",
        "result": "",
        "info": {
            "publicInfo": {"updated_acls": [], "removed_acls": []},
            "privateInfo": {},
        },
    }
