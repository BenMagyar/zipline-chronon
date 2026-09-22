from setuptools import find_packages, setup

setup(
    name="listing_ctr_model",
    version="1.0",
    packages=find_packages(),
    install_requires=[
        "xgboost==2.1.3",
        "numpy==1.26.4",
        "pandas==2.2.3",
        "joblib==1.6.0",
        "pyiceberg==0.12.0",
        "pyarrow==25.0.1",
    ],
)
