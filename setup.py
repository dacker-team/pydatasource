from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

setup(
    name='pydatasource',
    version='0.0.23',
    description='Easily manage a dataflow',
    long_description=readme,
    author='Dacker',
    author_email='hello@dacker.co',
    url='https://github.com/dacker-team/pydatasource',
    keywords='easily manage dataflow',
    packages=find_packages(exclude=('tests', 'docs')),
    python_requires='>=3',
    install_requires=[
        "dbstream>=0.0.14",
        "PyYAML>=5.1.2"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
