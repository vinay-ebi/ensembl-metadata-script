# ensembl-metadata-script

# create virtualenv 
pyenv virtualenv 3.8.3  metadata
pyrnv activate metadata

# install dependencies
pip install -r requirements.txt

# run script 
python main.py  -e 107 -r 38 -m mysql://ensro@mysql-ens-meta-prod-1:3366/ -d ensembl_metadata_qrp -c mysql://ensro@mysql-ens-sta-5:3366/ -t geneset -n homo_sapiens_gca009914755v4_core_107_1 -f /hps/<rrdump_path>