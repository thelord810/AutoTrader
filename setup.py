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


# Define extra dependencies
dydx_dep = ["dydx-v3-python >= 1.9.0"]
ccxt_dep = ["ccxt >= 2.0.53"]
oanda_dep = [
    "v20 >= 3.0.25.0",
]
ib_dep = [
    "ib_insync >= 0.9.70",
]
yfinance_dep = [
    "yfinance >= 0.1.67",
]
dev_dep = [
    "pytest >= 7.1.1",
    "black >= 22.10.0",
    "sphinx-copybutton >= 0.5.0",
    "sphinx-inline-tabs >= 2022.1.2b11",
    "myst-parser >= 0.18.1",
    "furo >= 2022.9.29",
    "sphinx-autobuild >= 2021.3.14",
    "commitizen >= 2.35.0",
]
all_dep = ccxt_dep + oanda_dep + ib_dep + yfinance_dep + dev_dep

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
    keywords=["algotrading", "finance", "crypto", "forex", "python"],
    package_dir={"": "."},
    packages=setuptools.find_packages(where="."),
    python_requires=">=3.7",
    install_requires=[
        "numpy >= 1.20.3",
        "pandas >= 1.3.4",
        "art >= 5.7",
        "PyYAML",
        "bokeh == 2.3.2",
        "scipy >= 1.7.1",
        "finta >= 1.3",
        "tqdm>=4.64.0",
        "importlib-resources",
        "click >= 8.1.3",
        "requests >= 2.28.1",
        "python-telegram-bot >= 13.14",
        "psutil >= 5.9.2",
        "prometheus-client >= 0.15.0",
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
