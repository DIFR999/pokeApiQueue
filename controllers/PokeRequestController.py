import json 
import logging

from fastapi import HTTPException
from models.PokeRequest import PokemonRequest
from utils.database import execute_query_json
from utils.AQueue import AQueue
from utils.ABlob import ABlob


# Configurar el logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Crea una nueva solicitud de reporte Pokémon en la base de datos y envía la solicitud
#  a una cola de Azure para su procesamiento
async def insert_pokemon_request( pokemon_request: PokemonRequest) -> dict:
    try:
        query = " exec pokequeue.create_poke_request ?, ? "
        params = ( pokemon_request.pokemon_type, pokemon_request.sample_size,)
        result = await execute_query_json( query , params, True )
        result_dict = json.loads(result)
        
        await AQueue().insert_message_on_queue( result )
        
        return result_dict

    except Exception as e:
        logger.error( f"Error inserting report request {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )
    
async def update_pokemon_request( pokemon_request: PokemonRequest) -> dict:
    try:
        query = " exec pokequeue.update_poke_request ?, ?, ? "
        if not pokemon_request.url:
            pokemon_request.url = "";

        params = ( pokemon_request.id, pokemon_request.status, pokemon_request.url  )
        result = await execute_query_json( query , params, True )
        result_dict = json.loads(result)
        return result_dict
    except Exception as e:
        logger.error( f"Error updating report request {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )



async def get_all_request() -> dict:
    query = """
        select 
            r.id as ReportId
            , s.description as Status
            , r.type as PokemonType
            , r.url 
            , r.created 
            , r.updated
        from pokequeue.requests r 
        inner join pokequeue.statuses s 
        on r.id_status = s.id 
    """
    result = await execute_query_json( query  )
    result_dict = json.loads(result)
    blob = ABlob()
    for record in result_dict:
        id = record['ReportId']
        record['url'] = f"{record['url']}?{blob.generate_sas(id)}"
    return result_dict    



async def select_pokemon_request( id: int ):
    try:
        query = "select * from pokequeue.requests where id = ?"
        params = (id,)
        result = await execute_query_json( query , params )
        result_dict = json.loads(result)
        return result_dict
    except Exception as e:
        logger.error( f"Error selecting report request {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )



## POR AQUI ME QUEDO, DEBO HACER PROCEDIMIENTO ALMACENADO, EL SDK DEL BLOB STORAGE
# Función que eliminarla el reporte del pokemon de la bases de datos y mi blob storage
async def delete_pokemon_report(id: int):
    try:
        # Validar que exista el reporte en DB
        existing = await select_pokemon_request(id)

        if not existing:
            raise HTTPException(status_code=404, detail="Reporte no encontrado")



         # Intentar borrar el blob del contenedor Azure Blob Storage
        try:
            blob = ABlob()
            blob_deleted = blob.delete_blob(id)
            if blob_deleted:
                logger.info(f"Blob poke_report_{id}.csv eliminado correctamente de Azure Blob Storage.")
            else:
                logger.warning(f"El blob poke_report_{id}.csv no se encontró en Azure Blob Storage.")
        except Exception as blob_error:
            logger.error(f"Error al eliminar el blob del contenedor: {blob_error}")
            # No interrumpimos el flujo, continuamos con el borrado de BD

        

        # Si el blob se borró, eliminar el registro en la base de datos usando el procedimiento almacenado
        query = " exec  pokequeue.delete_poke_request ? "
        params = (id,)
        await execute_query_json(query, params, True)

        return {"ok":True,"message": "Reporte eliminado correctamente", "id": id}

    except Exception as e:
        logger.error(f"Error eliminando reporte {id}: {e}")
        raise HTTPException(status_code=500, detail="Error eliminando reporte")
