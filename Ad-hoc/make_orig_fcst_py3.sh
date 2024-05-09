#!/bin/bash
# Need to setup path to Anaconda because root does not seem to inherit the environment
export PATH=/usr/local/anaconda/bin:$PATH

# Check if this virtual environment already exists
source activate orig_fcst_py3
exist_flag=$? ; if [[ $exist_flag -eq 0 ]]; then echo 'The orig_fcst_py3 virtual environment was already installed' ; exit 1 ; fi

conda clean --packages --tarballs -y

# Create the basic environment by installing all the conda packages
conda create --name orig_fcst_py3 python=3.6 -y

# Activate the environment
conda activate orig_fcst_py3

# same packages as superbass, except no hyperopt
# because it is not supported in py3
conda install numpy scipy pandas scikit-learn matplotlib seaborn dask \
keras tensorflow sasl pymysql pyhive pyarrow \
coverage faker luigi joblib redis numexpr bottleneck lightgbm \
progress thrift thrift_sasl pymc3 nose pytables -y

# notebook > 6.1.6 has a fatal error that can not be used. sO 
conda install notebook=6.1.4

conda install xgboost pyicu --channel conda-forge -y
conda install dataset --channel coecms -y
conda install hdfs --channel pdrops -y

# todo: snowflake_sqlalchemy dependencies give many conflicts with py3.6
# tried building this env as py3.5 but the other packages conflicted with py3.5 then
# try to pip install
#conda install snowflake_sqlalchemy -c snowflakedb
pip install snowflake-sqlalchemy --upgrade-strategy only-if-needed


# todo: replace this with real py3 version of dstools
# merging the pymysql related changes with the rest of nathan's py3 changes
# Special stuff for datasciencetools because it needs a patched version for now
#cd /mnt/dsnfs/xiaoxiao.xing/datasciencetools
#python setup.py install

pip install datasciencetools==2.0.3.dev --index-url https://pypi.prod.hulu.com/simple

# Special stuff for ds-luigi because it needs a patched version for now
#cd /mnt/dsnfs/xiaoxiao.xing/ds-luigi
#python setup.py install
pip install ds-luigi==0.98.0.dev --index-url https://pypi.prod.hulu.com/simple

pip install oauth2client
pip install gspread
pip install gspread-dataframe
pip install openpyxl
pip install pyhive==0.6.2