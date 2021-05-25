#!/usr/bin/env python3

"""The setup script."""

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().strip().split('\n')

with open('README.md') as f:
    long_description = f.read()
setup(
    maintainer='Xdev',
    maintainer_email='xdev@ucar.edu',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Scientific/Engineering',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
    ],
    description='Integral: A Python package to facilitate synthesis and analysis of Earth system data.',
    install_requires=requirements,
    license='Apache Software License 2.0',
    long_description_content_type='text/markdown',
    long_description=long_description,
    include_package_data=True,
    keywords='reproducible,science,xarray,earth system model',
    name='esds-funnel',
    packages=find_packages(),
    entry_points={},
    url='https://github.com/NCAR/esds-funnel',
    project_urls={
        'Documentation': 'https://github.com/NCAR/esds-funnel',
        'Source': 'https://github.com/NCAR/esds-funnel',
        'Tracker': 'https://github.com/NCAR/esds-funnel/issues',
    },
    use_scm_version={
        'version_scheme': 'post-release',
        'local_scheme': 'dirty-tag',
    },
    setup_requires=['setuptools_scm', 'setuptools>=30.3.0'],
    zip_safe=False,
)
