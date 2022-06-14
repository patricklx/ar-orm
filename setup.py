from setuptools import setup
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='arorm',
    version='0.3.2',
    description='A python orm with identity pattern mainly for arango',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/patricklx/ar-orm',
    author='Patrick Pircher',
    author_email='',
    license='MIT',
    packages=['arorm'],
    install_requires=[
        "event_emitter",
        "inflection",
        "arango",
        "arango-orm"
    ],
    classifiers=[],
)
