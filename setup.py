from setuptools import setup, find_packages


with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setup(
    name="pytest_dbconnect",
    version="0.0.1",
    description="Python package that does some magic tests with pytest and Azure Databricks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Ivan Georgiev",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License"
    ],
    keywords="pytest databricks unittest bdd",
    packages=find_packages(),
    install_requires=["databricks-connect", "pytest", "pytest-cov", "pandas"],
    python_requires="~=3.7"
)
