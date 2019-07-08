from setuptools import setup, find_packages

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
    url='https://github.com/micro-fan/fan',
    classifiers=[],
    entry_points={
        'console_scripts': [
            'fan_register=fan.contrib.sync_helper.fan_register:main',
            'fan=fan.scripts.fan:main',
        ]
    },
    install_requires=[
        'aiozk>=0.15',
        'opentracing',
        'basictracer',
        'requests',
        'kazoo',
        'tipsi_tools[logging]>=1.49.0',
        'py-zipkin>=0.18.0,<1.0.0',
        'aiohttp',
        'sanic',
    ],
)
