from setuptools import setup, find_packages

setup(
    name="p2p-latimes",
    version="2.0.36",
    packages=find_packages(),
    install_requires=(
        "python-dateutil",
        "requests",
        "iso8601",
        "pytz",
    ),
    test_suite='p2p.tests',
    author="Tribune News Applications, Los Angeles Times Data Desk",
    author_email="datadesk@latimes.com",
    description="Tribune P2P API wrapper",
    long_description="Python wrapper for API at P2P, the Tribune Publishing CMS",
    url="http://github.com/datadesk/p2p-python",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
