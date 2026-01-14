# setup.py
from setuptools import setup, find_packages

setup(
    name="indy-reader-revenue-budget-forecasts",
    version="0.1.0",
    description="Budget Forecast Tools",
    author="Ana Blanco Moreno",
    
    # Automatically find all packages (folders with __init__.py)
    packages=find_packages(),
    
    # Python version
    python_requires=">=3.8",
    
    install_requires=[
        "pandas",
        "numpy",
        "matplotlib",
        # Add others as you discover you need them
    ],
)
