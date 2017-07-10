from setuptools import setup


version = '0.0.1'
download_url = 'https://github.com/kkroening/daglet/archive/v{}.zip'.format(version)


setup(
    name='daglet',
    packages=['daglet'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    version=version,
    description='DAG tools for Python',
    author='Karl Kroening',
    author_email='karlk@kralnet.us',
    url='https://github.com/kkroening/daglet',
    download_url=download_url,
    keywords=[],
    long_description='DAG tools for Python',
    install_requires=['future'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
