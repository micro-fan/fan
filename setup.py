from setuptools import setup, find_packages
from pip.req import parse_requirements


setup(
    name='fan',
    packages=find_packages(),
    version='0.4.0',
    description='microservices kit',
    author='cybergrind',
    author_email='cybergrind@gmail.com',
    keywords=['rpc', 'microservices'],
    classifiers=[],
    entry_points={
        'console_scripts': [
            'fan_register=fan.contrib.sync_helper.fan_register:main',
            'fan=fan.scripts.fan:main',
        ]
    },
    install_requires=[
        'opentracing',
        'basictracer',
        'requests',
        'kazoo',
        'tipsi_tools',
        'py_zipkin'
    ],
)
