from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator


class Extraction(BaseModel):
    profession: Optional[str] = Field(default=None)
    qualification: Optional[str] = Field(default=None)
    education: Optional[str] = Field(default=None)
    experience: Optional[str] = Field(default=None)
    ps_general_code: Optional[str] = Field(default=None, description="Code from PS section 'Общие сведения' (e.g. 19.071)")

    @field_validator("profession", "qualification", "education", "experience", "ps_general_code")
    @classmethod
    def _strip_or_none(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        s = v.strip()
        return s or None

