import sqlalchemy
from dagster import ConfigurableResource


class SqlAlchemyClientResource(ConfigurableResource):
    database_ip: str
    database_port: str
    database_user: str
    database_password: str
    database_name: str

    def create_engine(self):
        return sqlalchemy.create_engine(
            f"postgresql://{self.database_user}:{self.database_password}@{self.database_ip}:{self.database_port}/{self.database_name}")
