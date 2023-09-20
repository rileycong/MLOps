from setuptools import find_packages, setup

setup(
    name="dagster_MLOps",
    packages=find_packages(exclude=["dagster_MLOps_tests"]),
    install_requires=[
        "dagster==1.4.12",
        "dagster-cloud",
        "pandas",
        "requests",
        "scikit-learn",
        "dagster-duckdb==0.20.12",
        "dagster-webserver==1.4.12",
        "pytest"
    ]
)
