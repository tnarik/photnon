import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="photnon",
    version="0.0.1",
    author="Tnarik Innael",
    author_email="tnarik@lecafeautomatique.co.uk",
    description="A small test package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/test/test",
    scripts=["bin/photnon"],
    entry_points={
    	'console_scripts': [
        	'samplea=sample:main',
    	],
	},
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)