from setuptools import setup, find_packages

extensions = []

reqs = ['astropy', 'requests', 'xmltodict']

with open ("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="pykoa",
    version="0.2",
    author="Mihseh Kong",
    description="KOA archive access client", 
    long_description = long_description,
    url="https://github.com/KeckObservarotyArchive/PyKOA",
    classifiers=[
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: MIT License',
        'Topic :: Scientific/Engineering :: Astronomy'],
    packages=['pykoa', 'pykoa/koa'],
    data_files=[],
    install_requires=reqs,
    python_requires='>= 3.6',
    include_package_data=False
)
