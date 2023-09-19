from setuptools import find_packages, setup

setup(
    name="dagster_MLOps",
    packages=find_packages(exclude=["dagster_MLOps_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "requests",
        "scikit-learn",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
