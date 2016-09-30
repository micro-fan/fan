from setuptools import setup, find_packages

setup(
    name='fan',
    packages=find_packages(),
    version='0.2.0',
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
)
