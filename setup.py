from setuptools import setup

install_requires = [
    'bottle==0.12.4',
    'CherryPy==3.6.0',
    # 'rednose',
    # 'nose-cov',
    # 'tissue',
]

test_requires = [
    # 'mock==1.0.1',
    # 'WebTest==2.0.14',
]

setup(
    name='HDFS Data Explorer',
    version='0.1.0',
    author='Oli Hall',
    author_email='oli.hall1309@googlemail.com',
    description="Explorer for data stored in HDFS",
    license='MIT',
    url='-',
    packages=['dataexplorer'],
    setup_requires=['nose>=1.0'],
    install_requires=install_requires,
    tests_require=test_requires,
)
