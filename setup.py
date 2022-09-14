from setuptools import setup

setup (
    name="mtDB",
    version="0.1",
    packages=["mtDB.db"],
    install_requires=["numpy", "pandas", "pathlib"]
)
