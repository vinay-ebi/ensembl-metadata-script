#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import argparse
import logging
import os
import sys
from os.path import expanduser
from metadata import generate_metadata, get_annotations_source, get_annotations_source_async
from metadata import MetadataParams

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

logging.basicConfig(
    # filename='main.log',
    # filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script to update directory path for Rapid Release')
    parser.add_argument('-v', '--verbose', help='Verbose output', action='store_true')
    parser.add_argument('-e', '--release_version',action="extend", nargs="+", type=int, required=True, help='Release numbers [101, 102]')
    parser.add_argument('-r', '--rapid_version', action="extend", nargs="+", type=int, required=True, help='Rapid Release number [36, 37]')
    parser.add_argument('-f', '--ftp_path', type=str, required=True, help='FTP RR directory path')
    parser.add_argument('-m', '--metadata_url', type=str, required=True,
                        help='Metadata Host Url format mysql://user:pass@host:port')
    parser.add_argument('-d', '--metadata_dbname', type=str, required=True,
                        help='Metadata database name', default='ensembl_metadata_qrp')
    parser.add_argument('-c', '--coredb_url', type=str,
                        help='Core db Host Url format mysql://user:pass@host:port')
    
    parser.add_argument('-s', '--species_names', action="extend", nargs="+", type=str,
                        help='species names ', )
    parser.add_argument('-n', '--database_names', action="extend", nargs="+", type=str,
                        help='species names ', )
    
    parser.add_argument('-t', '--data_type', action="extend", nargs="+", type=str,
                        choices=['geneset', 'genome', 'rnaseq', 'variation'], 
                        help='datatype subdir for ftp dumps'
                        )
    

    arguments = parser.parse_args(sys.argv[1:])
    logger.setLevel(logging.INFO)
    if not os.path.exists(os.path.join(arguments.ftp_path, 'species' )):
        logger.error(f"No species dir found in provided ftp_path: {arguments.ftp_path}")
        sys.exit(1)
        
    logger.info("Preparing Directory Path For RR Ftp Dumps With Subdir Annotation Source")
    for species_info in generate_metadata(arguments):
        core_metadata = get_annotations_source(species_info['dbname'], arguments.coredb_url)
        #if 'species.annotation_source' not in core_metadata.keys() or core_metadata['species.annotation_source'] == '' :
        if core_metadata.get('species.annotation_source', '')  ==  '':
            core_metadata['species.annotation_source'] = 'ensembl' 
                  
        print("getting core")
        print(core_metadata)
        species_name = "_".join(species_info['display_name'].split(' ')[0:2]) #Homo sapiens (Human) - GCA_018503265.1 
        accession_name = species_info['assembly_accession'] #GCA_009914755.4
        genebuild_inital = core_metadata.get('genebuild.initial_release_date').replace('-','_') if core_metadata.get('genebuild.initial_release_date', None) else ''
        genebuild_update = core_metadata.get('genebuild.last_geneset_update').replace('-','_') if core_metadata.get('genebuild.initial_release_date', None) else ''
        
        for data_type in arguments.data_type:
            base_path = os.path.join(arguments.ftp_path, f"species/{species_name}/{accession_name}/{data_type}")
            print(f"base_path: {base_path}")
            species_info[data_type] = [ i for i in [ os.path.join(base_path, genebuild_update) , 
                                                                           os.path.join(base_path, genebuild_inital)] if os.path.exists(i) ]
        
        print(species_info)
  