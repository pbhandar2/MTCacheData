from setuptools import setup

setup (
    name="mtDB",
    version="0.1",
    packages=["mtDB.db", "mtDB.cydonia"],
    install_requires=["numpy", "pandas", "pathlib"]
)
