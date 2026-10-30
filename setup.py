from setuptools import setup

reqs = ['requests', 'xmltodict', 'bs4', 'lxml', 'astropy']

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="pykoa",
    version="1.7.2",
    author="Mihseh Kong",
    author_email="mihseh@ipac.caltech.edu",
    description="KOA archive access client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/KeckObservatoryArchive/PyKOA",
    classifiers=[
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'License :: Other/Proprietary License',
        'Topic :: Scientific/Engineering :: Astronomy',
    ],
    packages=['pykoa', 'pykoa/koa'],
    data_files=[],
    install_requires=reqs,
    python_requires='>=3.11',
    include_package_data=False,
)
