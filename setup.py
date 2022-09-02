import setuptools
import codecs
import os.path


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="autotrader",
    version=get_version("autotrader/__init__.py"),
    author="Kieran Mackle",
    author_email="kemackle98@gmail.com",
    license="gpl-3.0",
    description="A Python-based platform for developing, optimising "
    + "and deploying automated trading systems.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://kieran-mackle.github.io/AutoTrader/",
    project_urls={
        "Bug Tracker": "https://github.com/kieran-mackle/AutoTrader/issues",
        "Source Code": "https://github.com/kieran-mackle/AutoTrader",
        "Documentation": "https://autotrader.readthedocs.io/en/latest/",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    keywords=["algotrading", "finance"],
    package_dir={"": "."},
    packages=setuptools.find_packages(where="."),
    python_requires=">=3.7",
    install_requires=[
        "numpy >= 1.20.3",
        "pandas >= 1.3.4",
        "pyfiglet >= 0.8.post1",
        "PyYAML",
        "bokeh >= 2.3.1",
        "scipy >= 1.7.1",
        "finta >= 1.3",
        "tqdm>=4.64.0",
        "importlib-resources",
        "Click",
        "requests",
    ],
    extras_require={
        "dydx": ["dydx-v3-python"],
        "ccxt": ["ccxt"],
        "oanda": [
            "v20 >= 3.0.25.0",
        "breeze_connect"
        ],
        "ib": [
            "ib_insync >= 0.9.70",
        ],
        "yfinance": [
            "yfinance >= 0.1.67",
        ],
        "dev": ["black"],
    },
    setup_requires=[
        "setuptools_git",
        "setuptools_scm",
    ],
    package_data={"": ["data/*.js", "data/keys.yaml"]},
    entry_points={
        "console_scripts": [
            "autotrader = autotrader.bin.cli:cli",
        ],
    },
)
