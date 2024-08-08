Examples
========


Usage is best explained using real examples from various datasets. First,  go to the **examples** folder in the `repository <https://github.com/skababji-ehil/fuzzy_sql>`_ and download and unzip the data file by running  **0.0-download_data.ipynb**. This will download the three datasets in separate subfolders under the **examples** folder. Both the real and synthetic sample datasets are provided in `csv` formats. The metadata files are  provided in `json` format to define the corresponding variable types and data relations for longitudinal datasets. Below is a list of the three sample datasets:  

    *  sdgd-C1: Tabular dataset. Real, Synthetic and Metadata for sdgd-C1 is downloaded under :code:`data/sdgd/`
    *  cal: Longitudinal dataset with single child. Real, Synthetic and Metadata for sdgd-C1 is downloaded under :code:`data/cal/`
    *  cms: Longitudinal dataset with multiple-child. Real, Synthetic and Metadata for sdgd-C1 is downloaded under :code:`data/cms/`

If **0.0-download_data.ipynb** fails to download and unzip the data file, you may manually download the file from the link:

https://drive.google.com/file/d/1ag35pYzSdZSE71kY5_BDoN02zOzyzBQY/view?usp=share_link

If you encounter difficulties accessing the link, please contact us.

The first step is to prepare and import the csv files into the databases. All notebooks starting with the format x.1 are used for that purpose. These three notebooks (one for each dataset) are typically run only for the first time to create and setup the databases. A separate database is created for each dataset. The remaining notebooks are for generating SQL random queries after setting up the databases. 

Use Jupyter by typing in your terminal :code:`jupyter notebook` and run the provided notebooks in the proper sequence i.e. first, set up a database and then generate random queries. The notebooks include setting up the necessary environment besides various valuable comments. Moreover, details about adjusting query parameters are provided in one of the files. 
