Usage
=====

Usage is best explained using examples from various datasets. Please follow the following steps to install and run the examples:

#. Download and unzip the file from the link below:  

    `<https://ehealthinformation-my.sharepoint.com/:u:/g/personal/skababji_ehealthinformation_ca/Ec6Paj0ypqNHm6_4cHn2qP4Br-ek5L6WGUGNar_tEf3oHQ?e=Nzrcxa>`_

#. Navigate to the folder that contains the python files `main_sdgd.py, main_cal.py, main_cms.py and main_cms_tuned.py`. Each file is a standalone example and generates random queries corresponding to the following datasets:

    *  sdgd -C1 : Tabular Dataset 
    *  cal: Longitudinal dataset with single child
    *  cms: Longitudinal dataset with multiple-child
  
#. In the directory above, create your virtual environment as explained in :doc:`/installation` . This is repeated here for convenience assuming a Linux system:

    .. code-block:: console

        $ python3 -m venv .
        $ source bin/activate
        $ pip install --upgrade pip
        $ pip install git+ssh://git@github.com/skababji-ehil/fuzzy_sql.git@v1.0.0-beta#egg=fuzzy_sql

#. Run each of the four scripts using your activated environment above. The scripts are self-explanatory and include various useful comments. 