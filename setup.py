from setuptools import setup

install_requires = []

setup(
    name="qloop",
    version="0.0.1",
    author="Pau Freixes",
    url="https://github.com/pfreixes/qloop",
    author_email="pfreixes@gmail.com",
    description="POC Loop with fair queue schedule",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5.0",
    ],
    packages=['qloop'],
    install_requires=install_requires
)
