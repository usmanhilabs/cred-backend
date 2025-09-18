from pydantic import BaseModel, Field
from datetime import datetime

class FormDataSchema(BaseModel):
    formId: str
    typeForm: str
    data: dict

    class Config:
        arbitrary_types_allowed = True


class ApplicationCreate(BaseModel):
    id: str | None = None
    provider_id: str = Field(..., alias="providerId")
    form_id: str = Field(..., alias="formId")
    name: str
    last_name: str | None = Field(..., alias="providerLastName")
    email : str
    phone : str
    status: str
    progress: int
    assignee: str
    source: str
    market: str
    specialty: str
    address: str
    npi: str

class ApplicationResponse(ApplicationCreate):
    # Override fields to allow nulls in responses
    provider_id: str | None = Field(None, alias="providerId")
    form_id: str | None = Field(None, alias="formId")
    email: str | None = None
    phone: str | None = None
    address: str | None = None
    npi: str | None = None
    last_name: str | None = Field(None, alias="providerLastName")
    create_dt: datetime
    last_updt_dt: datetime

    class Config:
        orm_mode = True
        populate_by_name = True

class EmailCreate(BaseModel):
    application_id: str
    recipient_email: str
    subject: str
    body: str
    status: str
    sent_at: datetime