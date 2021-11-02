from setuptools import setup, find_packages

setup(
    name="delta_migrations",
    version="0.0.1",
    author="Jared Magrath",
    author_email="magrathj@tcd.ie",
    description="Provides delta schema migrations",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[],
    entry_points='''
        [console_scripts]
        delta_migrations=cli:cli
    ''',
)
