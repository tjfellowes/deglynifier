from setuptools import setup

setup(
    name="deglynifier",
    version="0.3.0",
    py_modules=["deglynifier"],
    install_requires=[],
    entry_points={
        "console_scripts": [
            "deglynifier=deglynifier:cli",
        ],
    },
)