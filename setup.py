from setuptools import setup, find_packages

setup(
    name="nl-db-query-system",
    version="1.0.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "openai>=1.0.0",
        "pymongo>=4.5.0",
        "clickhouse-driver>=0.2.5",
        "aiohttp>=3.8.4",
        "python-dotenv>=1.0.0",
        "click>=8.1.3",
        "fastapi>=0.100.0",
        "uvicorn>=0.23.2",
        "pytest>=7.3.1",
        "pytest-asyncio>=0.21.0",
        "loguru>=0.7.0",
        "pandas>=2.0.0",
        "redis>=4.6.0",
        "pydantic>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "nldbq=app.main:cli",
        ],
    },
    author="NL-DB-Query-System Team",
    author_email="team@nldbquerysystem.com",
    description="Natural Language to Database Query System",
    keywords="nlp, database, query, chatgpt, openai, mongodb, clickhouse",
    url="https://github.com/nldbquerysystem/nl-db-query-system",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
)