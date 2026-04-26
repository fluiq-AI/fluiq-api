import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from db_queues.postgresql.auth import (
    create_api_key,
    delete_api_key,
    get_organization,
)
from routes.auth.helper import get_current_session
from shared.model import ApiKeyCreated, OrganizationModel

api_keys_router = APIRouter()


class CreateApiKeyPayload(BaseModel):
    name: str = Field(min_length=1, max_length=64)


@api_keys_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=ApiKeyCreated,
)
async def create_key(
    payload: CreateApiKeyPayload,
    session: dict = Depends(get_current_session),
) -> ApiKeyCreated:
    org_id = uuid.UUID(session["org_id"])
    api_key = await create_api_key(org_id=org_id, name=payload.name.strip())
    if api_key is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="API key limit reached for this plan",
        )
    return api_key


@api_keys_router.delete(
    "/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_key(
    key_id: uuid.UUID,
    session: dict = Depends(get_current_session),
) -> None:
    org_id = uuid.UUID(session["org_id"])
    removed = await delete_api_key(org_id=org_id, key_id=key_id)
    if not removed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found",
        )


@api_keys_router.get(
    "",
    status_code=status.HTTP_200_OK,
    response_model=OrganizationModel,
)
async def list_keys(
    session: dict = Depends(get_current_session),
) -> OrganizationModel:
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )
    return organization
