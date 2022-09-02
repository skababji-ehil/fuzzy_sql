from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name = "fuzzy-sql",
    version ="0.0.11",
    description = "A generator of random SQL SELECT queries mainly to compare responses from a real dataset against that from a synthetic dataset.",
    long_description=long_description,
    long_description_content_type = "text/markdown",
    author="Samer Kababji",
    author_email = "skababji@ehealthinformation.ca",
    classifiers=[
        "Development Status :: 3 - Alpha","Programming Language :: Python :: 3"
        ],
    keywords="sql, synthetic, clinical trials, generative, testing, fuzzy, fuzzing", 
    package_dir = {"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.7",
    install_requires=["scikit-learn==1.1.2","pandas==1.4.3","pdfkit==1.0.0","seaborn==0.11.2"],
    extras_require={ 
        "dev": ["jupyter==1.0.0"],
    },

)



