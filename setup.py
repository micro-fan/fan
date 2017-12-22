from setuptools import setup, find_packages
from pip.req import parse_requirements

with open('fan/__init__.py', 'r') as f:
    for line in f:
        if line.startswith('__version__'):
            version = line.strip().split('=')[1].strip(' \'"')
            break

setup(
    name='fan',
    packages=find_packages(),
    version=version,
    description='microservices kit',
    author='cybergrind',
    author_email='cybergrind@gmail.com',
    keywords=['rpc', 'microservices'],
    url='https://github.com/tipsi/fan',
    classifiers=[],
    entry_points={
        'console_scripts': [
            'fan_register=fan.contrib.sync_helper.fan_register:main',
            'fan=fan.scripts.fan:main',
        ]
    },
    install_requires=[
        'aiozk',
        'opentracing',
        'basictracer',
        'requests',
        'kazoo',
        'tipsi_tools',
        'py_zipkin'
    ],
)
