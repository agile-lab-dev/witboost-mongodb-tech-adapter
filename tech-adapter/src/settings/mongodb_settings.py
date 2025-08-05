from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class MongoDBSettings(BaseSettings):
    connection_string: str
    users_database: str
    developer_roles: List[str]
    consumer_actions: List[str]
    useCaseTemplateId: str
    useCaseTemplateSubId: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)
