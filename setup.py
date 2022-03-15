from setuptools import setup

setup(
    name='arorm',
    version='0.3.0',
    description='A python orm with identity pattern mainly for arango',
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
