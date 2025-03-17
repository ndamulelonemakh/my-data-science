from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Boolean
from loguru import logger
import pyodbc
from urllib.parse import quote_plus

server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

if not all([server, database, username, password]):
    raise ValueError("Database credentials are missing from environment variables.")

connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=tcp:{server},1433;DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(connectionString) # Optional

# connection_string = 'mssql+pyodbc:///?odbc_connect={}'.format(quote_plus(connectionString))
connection_string = 'mssql+pyodbc:///?odbc_connect={}'.format(connectionString)
engine = create_engine(connection_string)
Base = declarative_base()


class TodoItem(Base):
    __tablename__ = "todo_items"
    id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    description = Column(String(250))
    completed = Column(Boolean, default=False)


def create_todo_item(title, description):
    logger.info(f"Creating a new todo item: {title}")
    new_item = TodoItem(title=title, description=description)
    session.add(new_item)
    session.commit()
    return new_item


def get_all_todo_items():
    logger.info("Getting all todo items")
    return session.query(TodoItem).all()


def get_todo_item_by_id(item_id):
    return session.query(TodoItem).filter(TodoItem.id == item_id).first()


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # Create a new todo item
        create_todo_item("Learn SQLAlchemy", "Understand how to use SQLAlchemy with Azure SQL Server")

        # Read all todo items
        todos = get_all_todo_items()
        for todo in todos:
            logger.info(todo.title, todo.description, todo.completed)

    finally:
        session.close()
