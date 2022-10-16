import os
from typing import List, Optional
from yarl import URL
from uuid import UUID
from pydantic import BaseModel, ValidationError, validator
from sqlalchemy.orm.session import Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import  sessionmaker
from sqlalchemy import select
from ensembl.database.dbconnection import DBConnection
from metadata_model import Genome, GenomeDatabase, Organism, DataRelease, DataReleaseDatabase, Division, Assembly
from ensembl.core.models import Meta
import logging 

logging.basicConfig(
    filename='metadata.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



class MetadataParams(BaseModel):
    species_names: Optional[List[str]]
    database_names: Optional[List[str]] 
    release_version: List[int]
    rapid_version: List[int]
    coredb_url: Optional[str]
    metadata_url: Optional[str]
    metadata_dbname: str
    ftp_path: str
    

async def async_metadbsession(dburl: str):
    
    engine = create_async_engine(dburl, future=True, echo=True)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    
    return async_session

class AnnotationSourceDAL():
    """Data Access Layer to access metadata db and get the annotation resource from core db
    """    
    def __init__(self, db_session: Session):
        self.db_session = db_session


    async def get_metakeys(self, meta_keys=['species.annotation_source',
                                            'genebuild.last_geneset_update',
                                            'genebuild.initial_release_date']) :
        core_query = select(Meta.meta_key, Meta.meta_value).filter(Meta.meta_key.in_(meta_keys))
        q = await self.db_session.execute(core_query)
        return q.scalars().all()

async def get_annotations_source_async(core_dburl, dbname):
    core_url_conn_string = os.path.join(core_dburl, dbname)
    aysnc_mysession =  async_metadbsession(core_url_conn_string)
    async with aysnc_mysession() as session :
        async with session.begin():
            annotation_source_dal = AnnotationSourceDAL(session)
            core_metakeys = await annotation_source_dal.get_metakeys()
            return core_metakeys
    
  
    
    
    # engine = create_async_engine(
    #     url,
    #     echo=True,
    # )
    
    # async with engine.connect() as conn:
        
    #     result = await conn.execute(resource_quey)

    #     print(result.fetchall())

    # # for AsyncEngine created in function scope, close and
    # # clean-up pooled connections
    # await engine.dispose()
    

def get_db_session(url ) :
    """Provide the DB session scope with context manager

    Args:
        url (URL): Mysql URI string mysql://user:pass@host:port/database
    """   
    return  DBConnection(url)



def get_annotations_source(dbname: str, coredb_url: str, meta_keys=['species.annotation_source',
                                            'genebuild.last_geneset_update',
                                            'genebuild.initial_release_date']): 
    """Fetch Meta key annotation_source information species core db 

    Args:
        dbname (str): Ensembl Metadata database name
        coredb_url (str): Mysql url for species core database

    Returns:
        sting: annotation source information
    """     
    core_url_conn_string = os.path.join(coredb_url, dbname)   
    db_connection =  get_db_session(core_url_conn_string)
    with db_connection.session_scope() as session:
        core_query = select(Meta.meta_key, Meta.meta_value).filter(Meta.meta_key.in_(meta_keys))
        result =  dict(session.execute(core_query).all())
        return result
            
                    

def generate_metadata(metadata_params: MetadataParams):    
    """ Get Species Metadata information from ensembl metadata database

    Args:
        species_list (Optional[List[str]]): List of species names
        database_list (Optional[List[str]]): List of database names 
        release_version (int): Ensembl release version 
        rapid_version (int): Rapid release version
        core_db_url (URL): Core database mysql url 
        metadata_db_url (URL): metadata database mysql url 
        metadata_db_name (str): metadata database name 
    """  
    metadata_url_conn_string = os.path.join(metadata_params.metadata_url, metadata_params.metadata_dbname)   
    db_connection =  get_db_session(metadata_url_conn_string)
    with db_connection.session_scope() as session:
        meta_query = select(Assembly.assembly_accession.label("assembly_accession"), Assembly.assembly_name,
               Organism.name, Organism.scientific_name, Organism.display_name, Organism.species_taxonomy_id, Organism.strain,
               Genome.genebuild, GenomeDatabase.dbname,GenomeDatabase.type, DataRelease.release_date).select_from(GenomeDatabase) \
        .join(Genome).join(Assembly).join(Organism).join(DataRelease).join(Division) \
        .filter(DataRelease.ensembl_version.in_( metadata_params.release_version) ) \
        .filter(DataRelease.ensembl_genomes_version.in_(metadata_params.rapid_version))
        
              
        if metadata_params.species_names:
            meta_query = meta_query.filter(
                Organism.name.in_(metadata_params.species_names)
            )
        
        if metadata_params.database_names:
            meta_query = meta_query.filter(
                GenomeDatabase.dbname.in_(metadata_params.database_names)
            )
        
        for result in session.execute(meta_query): 
            species_info = dict(result)
            #species_info['annotation_source'] = get_annotations_source(species_info['dbname'], metadata_params.coredb_url)

            yield species_info
        
 
def prepare_files_paths(species_info: dict, ftp_path: str) :
    """_summary_

    Args:
        species_info (dict): _description_
        ftp_path (str): _description_

    Returns:
        _type_: _description_
    """    
    
    
    return 'dbpaths'
    

        

    
    
    