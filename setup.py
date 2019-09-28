from setuptools import setup, find_packages

setup(
    name="testwsapp",
    version='1.0',
    packages=find_packages("testwsapp"),
    package_dir={"": "testwsapp"},
    include_package_data=True,
    python_requires=">=3.6",
    install_requires=[
        "aiohttp==3.5.4",
        "asyncpg==0.18.3",
        "asyncio==3.4.3",
        "asyncio_redis==0.15.1",
        "simplejson==3.16.0"
    ],
)
