import os
from setuptools import setup, find_packages

def get_version():
    version = {}
    version_file_path = os.path.join(os.path.dirname(__file__), 'sda', '__version__.py')
    with open(version_file_path) as fp:
        exec(fp.read(), version)
    return version['__version__']

def get_readme():
    readme_file_path = os.path.join(os.path.dirname(__file__), 'README.md')
    with open(readme_file_path, 'r', encoding='utf-8') as f:
        return f.read()

setup(
    name='sda',
    version=get_version(),
    author='Jan Christian Redlich',
    author_email='jan.redlich@bonnconsulting.group',
    description='Semantic Data Abstraction',
    long_description=get_readme(),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    python_requires='>=3.8',
    include_package_data=True,
    install_requires=[
        'matplotlib==3.7.5',
        'matplotlib-inline==0.1.6',
        'mpmath==1.3.0',
        'networkx==3.1',
        'numpy==1.24.4',
        'pandas==2.0.3',
        'pyspark==3.5.1',
        'py4j==0.10.9.7'
    ],
    dependency_links=[
        'git+ssh://git@github.com/JanChristianRedlich/ReGraph.git#egg=ReGraph-2.0.5'
    ],
    classifiers=[
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)