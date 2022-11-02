# ensembl-metadata-script

# create virtualenv 
pyenv virtualenv 3.8.3  metadata
pyrnv activate metadata

# install dependencies
pip install -r requirements.txt

# run script 
python main.py  --release_version 108 --rapid_version 40 --metadata_url mysql://ensro@mysql-ens-meta-prod-1:3366/ --metadata_dbname ensembl_metadata_qrp --coredb_url mysql://ensro@mysql-ens-sta-5:3366/ --data_type geneset --data_type variation --data_type rnaseq --data_type genome --database_names homo_sapiens_gca009914755v4_core_107_1 --ftp_path /hps/<rrdump_path>