# Marple SDK

A simple SDK to upload data to [Marple](https://www.marpledata.com/)

Open [README in subfolder](marple/README.md) for docs

## Development (MATLAB)

You can use [MATLAB online](https://matlab.mathworks.com/) for testing.
It has a free tier for 20h of MATLAB / month.

## Development (Python)

**Local testing**

- To do instructions without build

**Local build**

- `poetry build`
- `poetry shell `
- (inside shell) `pip install dist/*.whl`
- (inside shell) `python`
- (inside repl) `from marple import Marple, Insight, DB`

**Publishing**

`py -m twine upload --repository testpypi dist/*`

To install the test pypi package

`py -m pip install --index-url https://test.pypi.org/simple/ --no-deps marpledata`

To deploy on actual pypi, but be careful!

`py -m twine upload dist/*`
