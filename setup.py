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
    url="https://github.com/tnarik/photnon",
    scripts=["bin/photnon"],
    entry_points={
    	'console_scripts': [
        	'samplea=sample:main',
    	],
	},
	package_dir={'': 'src'},
    packages=setuptools.find_namespace_packages(where='src'),
    install_requires=[
          'pandas',
          'numpy',
          'prompt_toolkit',
          'colorama',
          'piexif',
          'tables',
          'python-magic',
          'hachoir',
          'tqdm'
      ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_data={'': ['templates/*']},
)