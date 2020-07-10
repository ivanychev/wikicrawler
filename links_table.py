from sqlalchemy import MetaData, Table, Column, Integer, String

metadata = MetaData()

URL_MAX_LENGTH = 2000

links = Table("links", metadata,
    Column("url_from", String(URL_MAX_LENGTH), primary_key=True),
    Column("url_to", String(URL_MAX_LENGTH), primary_key=True),
    Column("count", Integer),
)