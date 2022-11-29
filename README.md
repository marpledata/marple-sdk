# Marple SDK

A simple SDK to upload data to [Marple](https://www.marpledata.com/)

Open [README in subfolder](marple/README.md) for docs



**Development**

To install from this git repo to your local python,
```bash
cd <where pyproject.toml is>
pip install .
```

To build the package

`py -m build`

To upload the package to test pypi

`py -m twine upload --repository testpypi dist/*`

To install the test pypi package

`py -m pip install --index-url https://test.pypi.org/simple/ --no-deps marpledata`

To deploy on actual pypi, but be careful!

`py -m twine upload dist/*`

