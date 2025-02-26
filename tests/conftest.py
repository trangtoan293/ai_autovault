"""
Test configuration
"""
import os
import pytest
import tempfile
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.services.metadata_store import Base

# Create a temporary SQLite database for testing
@pytest.fixture(scope="session")
def test_db():
    """Create a test database"""
    # Create a temporary file for SQLite database
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    
    # Create SQLite database URL
    db_url = f"sqlite:///{path}"
    
    # Create engine and tables
    engine = create_engine(db_url)
    Base.metadata.create_all(bind=engine)
    
    # Create session
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Return database objects
    yield {"engine": engine, "session": TestingSessionLocal(), "url": db_url, "path": path}
    
    # Cleanup
    os.unlink(path)


@pytest.fixture
def test_client(test_db):
    """Create a test client for FastAPI"""
    from fastapi.testclient import TestClient
    from app.main import app
    from app.api.dependencies import get_db
    
    # Override the get_db dependency to use test database
    def override_get_db():
        try:
            db = test_db["session"]
            yield db
        finally:
            db.close()
    
    app.dependency_overrides[get_db] = override_get_db
    
    # Create test client
    with TestClient(app) as client:
        yield client


@pytest.fixture
def sample_csv_file():
    """Create a sample CSV file for testing"""
    # Create a temporary file
    fd, path = tempfile.mkstemp(suffix='.csv')
    with os.fdopen(fd, 'w') as f:
        f.write("id,name,value,created_at\n")
        f.write("1,Test 1,100,2023-01-01\n")
        f.write("2,Test 2,200,2023-01-02\n")
        f.write("3,Test 3,300,2023-01-03\n")
    
    # Return path
    yield path
    
    # Cleanup
    os.unlink(path)


@pytest.fixture
def sample_excel_file():
    """Create a sample Excel file for testing"""
    import pandas as pd
    
    # Create a temporary file
    fd, path = tempfile.mkstemp(suffix='.xlsx')
    os.close(fd)
    
    # Create DataFrame
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Test 1", "Test 2", "Test 3"],
        "value": [100, 200, 300],
        "created_at": ["2023-01-01", "2023-01-02", "2023-01-03"]
    })
    
    # Write to Excel
    df.to_excel(path, index=False)
    
    # Return path
    yield path
    
    # Cleanup
    os.unlink(path)


@pytest.fixture
def sample_metadata():
    """Sample metadata for testing"""
    return {
        "table_name": "test_table",
        "column_name": "test_column",
        "data_type": "varchar",
        "description": "Test description",
        "source_system": "test_source",
        "business_definition": "Test business definition",
        "is_primary_key": False,
        "is_foreign_key": False,
        "nullable": True,
        "sample_values": ["test1", "test2", "test3"]
    }


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    
    # Return path
    yield temp_dir
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_dbt_project(temp_dir):
    """Create a mock DBT project for testing"""
    # Create project structure
    os.makedirs(os.path.join(temp_dir, "models"), exist_ok=True)
    os.makedirs(os.path.join(temp_dir, "models", "hub"), exist_ok=True)
    os.makedirs(os.path.join(temp_dir, "models", "link"), exist_ok=True)
    os.makedirs(os.path.join(temp_dir, "models", "satellite"), exist_ok=True)
    
    # Create project.yml
    with open(os.path.join(temp_dir, "dbt_project.yml"), "w") as f:
        f.write("""
name: 'test_project'
version: '1.0.0'
config-version: 2
profile: 'test_profile'
        """)
    
    # Return project directory
    return temp_dir


@pytest.fixture
def mock_git_repo(temp_dir):
    """Create a mock Git repository for testing"""
    import subprocess
    
    # Initialize Git repository
    subprocess.run(["git", "init"], cwd=temp_dir, capture_output=True)
    
    # Create a file
    with open(os.path.join(temp_dir, "README.md"), "w") as f:
        f.write("# Test Repository")
    
    # Configure Git user
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=temp_dir, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=temp_dir, capture_output=True)
    
    # Commit file
    subprocess.run(["git", "add", "."], cwd=temp_dir, capture_output=True)
    subprocess.run(["git", "commit", "-m", "Initial commit"], cwd=temp_dir, capture_output=True)
    
    # Return repository directory
    return temp_dir


@pytest.fixture
def mock_openai_response():
    """Mock response from OpenAI API"""
    class MockCompletion:
        def __init__(self, content):
            self.message = {"content": content}
    
    class MockResponse:
        def __init__(self, content):
            self.choices = [MockCompletion(content)]
    
    return MockResponse("""
    {
        "business_keys": ["id"],
        "model_name": "hub_test_table",
        "sensitive_columns": ["name"],
        "data_quality_tests": [
            "not_null(id)",
            "unique(id)",
            "relationships(id, ref('other_table'), 'id')"
        ]
    }
    """)
