from setuptools import setup, find_packages
setup(
    name='substrait-tools-rtia8',
    version='0.0.1',
    author='richtia',
    description='A Substrait command line tool',
    long_description='Tool for generation substrait plans',
    # url='https://github.com/substrait-io/consumer-testing',
    # keywords='substrait, consumer',
    python_requires='>=3.9, <4',
    packages=find_packages(include=['produce_substrait*', 'produce_substrait.*']),
    package_data={
        'produce_substrait': ['jars/*.jar'],
    },
    entry_points={
        'console_scripts': [
            'test_substrait = produce_substrait:main',
        ]
    }
)
