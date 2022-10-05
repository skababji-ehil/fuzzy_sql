from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

# with open('requirements.txt') as f:
#     requirements = f.read().splitlines()



setup(
    name="fuzzy-sql",
    version="0.1.9",
    description="A generator of random SQL SELECT queries mainly to compare responses from a real dataset against that from a synthetic dataset.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Samer Kababji",
    author_email="skababji@ehealthinformation.ca",
    classifiers=[
        "Development Status :: 4 - Beta", "Programming Language :: Python :: 3","Operating System :: OS Independent","License :: OSI Approved :: MIT License"
    ],
    keywords="sql, synthetic, clinical trials, generative, testing, fuzzy, fuzzing",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "pandas",
        "Jinja2",
        "seaborn",
        "scikit-learn",
    ],
    extras_require={
        "dev": ["jupyter"],
    },

)
