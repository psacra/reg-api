# main.py
# registration gateway
# This script allows to 

#General imports
import os
from fastapi import FastAPI, Body, Path, Request
from fastapi import Depends, HTTPException, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import asyncio
import json
from urllib.request import Request as URLlibRequest, urlopen as URLlibUrlopen
from urllib.error import URLError, HTTPError
import datetime as dt
import re
import shutil

#Setup FastAPI
app = FastAPI(title="Registration Gateway",description="This application acts as a gateway for OGC Records - Transactions API, capturing requests and ensuring the assets contained in the Items to be registered are valid an stored into a proper storage location.",version="0.0.1")

#Get current script execution path
CURPATH = os.path.dirname(os.path.realpath(__file__))
CFGPATH = os.path.realpath(os.path.join(CURPATH,"../cfg"))

#Use SQLite database to store AAI and other configuration information. Open this in read only mode
import sqlite3
con = sqlite3.connect('file:'+os.path.join(CFGPATH,"auth.db")+'?mode=ro',uri=True,check_same_thread=False)

#Use basic HTTP security for the endpoints (get username/password from the configuration DB)
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import hashlib
security = HTTPBasic()
def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
  #Open connection to the db
  cur = con.cursor()
  #Query for the ID. If retreived, the user is logged in
  cur.execute("SELECT id FROM auth WHERE username = :username AND password_sha256 = :password_sha256;",{"username": credentials.username,"password_sha256": hashlib.sha256(credentials.password.encode("utf8")).hexdigest()})
  query_result = cur.fetchone()
  if query_result and len(query_result) > 0 and query_result[0] is not None:
    return query_result[0]
  else:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Incorrect username or password",
      headers={"WWW-Authenticate": "Basic"},
    )

#Check the authorization using the configuration DB
def check_user_collection_authorization(user_id: int, collection_name: str):
  #Open connection to the db
  cur = con.cursor()
  #Query for the collection id map to the user
  cur.execute("SELECT stagein_path, assets_path, stacs_path, datastore_url, cat_post_url, extra_auths FROM user_collection_write_map WHERE collection_name = :collection_name AND user_id= :user_id;",{"user_id": user_id,"collection_name": collection_name})
  query_result = cur.fetchall()
  cur.close()
  if query_result and len(query_result) > 0:
    return query_result[0]
  else:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="User is not authorized to this collection",
      headers={"WWW-Authenticate": "Basic"},
    )


#Rewrite the validation error to make it look like a GeoJSON error
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
  return JSONResponse(status_code=422,content={'id':'unknown','failure_reason':str(exc)})

@app.post(
  "/collections/{collectionId}/items",
  tags=["Implemented transaction operations:"],
  summary="Register an Item or an ItemCollection",
  description="""This call performs a registration of a [STAC Item](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md) or [STAC ItemCollection](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/fragments/itemcollection) (a set of STAC Items) into the Catalogue.

To access it, you need to be registered as Data Provider and authorized to publish in the Catalogue collection (_{collectionId}_ in the API endpoint path).

The POST request body needs to contain the STAC Item or the STAC ItemCollection to be published.

The STAC Item or STAC ItemCollection to be published needs to respect the following constraints:
- It needs to be a valid STAC Item, according to the [STAC Item Specifications](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md), or a valid STAC ItemCollection, according to the [STAC ItemCollection Specifications](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/fragments/itemcollection), containing valid STAC Items inside
- If a _collection_ metadata is present, it needs to contain the same value as _{collectionId}_. If it is not present, it will be added with the same value as _{collectionId}_
- The _id_ metadata can contain only the [a-zA-Z0-9._-] characters and shall have maximum 100 characters.
- Each asset of the STAC Item containing a URI as "href" is ignored and will be posted into the Catalogue as-is
- Each asset of the STAC Item containing a local path as "href" is ingested. It shall thus have:
  - An asset href pointing to the relative path of the asset binary file in the S3 stagein bucket associated to the Collection
  - An asset href filename containing only the [a-zA-Z0-9._-] characters (the rest of the filename path is ignored). The path cannot point to a directory, only a file. The filename shall have maximum 100 characters.
  - An asset href filename unique among all the STAC Item assets (two assets in the same STAC item cannot have the same filename, even if they have different relative paths)

The API response will contain, in case of success, the ingested STAC Item or a STAC ItemCollection (containing the ingested STAC Item) as present in the Catalogue, thus with
- Additional required metadata, including a link to the STAC Collection
- Rewritten asset links pointing to the datastore.

The API response will contain, in case of failure, a JSON entry with the _id_ of the STAC Item to be ingested and a _failure_reason_ message for the STAC Items which failed ingestion.

Examples of API request and response are provided below. Please note that this API operation will require authorization.
  """,
  status_code=201,
  responses={
    201: {"description": "Successful addition. The added item is reported in the body (with rewritten assets HREF).","content":{"application/json":{"examples":{"Item":{"value":
      {
      "type": "Feature",
      "stac_version": "1.0.0",
      "stac_extensions": [
        "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
        "https://stac-extensions.github.io/storage/v1.0.0/schema.json"
      ],
      "id": "S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600",
      "assets": {
        "PRODUCT": {
          "href": "http://myserver.com/d/2015/05/19/S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600/S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600.tgz",
          "title": "Product",
          "type": "application/octet-stream",
        }
      },
      "collection": "PRR_TEST",
      "links": []
    }},"ItemCollection":{"value":
{
  "type": "FeatureCollection",
  "features": [
    {
      "assets": {
        "PRODUCT": {
          "href": "http://myserver.com/d/2015/05/19/S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600/S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600.tgz",
          "title": "Product",
          "type": "application/octet-stream"
        }
      },
      "id": "S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600",
      "stac_extensions": [
        "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
        "https://stac-extensions.github.io/storage/v1.0.0/schema.json"
      ],
      "stac_version": "1.0.0",
      "type": "Feature",
      "collection": "PRR_TEST",
      "links": []
    }
  ]
}
    }
    }}}},
    409: {"description": "Conflict. Failed to add one or more items because items already exist","content":{"application/json":{"examples":{"Item":{"value":
      {"id": "the id of the item which already exists",
       "failure_reason": "Item already exists"}
    },"ItemCollection":{"value":
{
  "type": "FeatureCollection",
  "features": [
      {"id": "the id of the item which already exists",
       "failure_reason": "Item already exists"}
  ]
}
    }}}}}, 
    422: {"description": "Validation error. Failed to add one or more items. Error in the response","content":{"application/json":{"examples":{"Item":{"value":
      {"id": "the id of the item which failed to be ingested if determined",
       "failure_reason": "a test describing the error"}
    },"ItemCollection":{"value":
{
  "type": "FeatureCollection",
  "features": [
      {"id": "the id of the item which failed to be ingested if determined",
       "failure_reason": "a test describing the error"}
  ]
}
    }}}}}
  }
)
async def collection_items_post_request(
  user_id: int = Depends(get_current_username),
  collectionId: str = Path(example="PRR_TEST"),                                          
  body: dict = Body(openapi_examples={"single":{"summary":"Item","value":{
    "type": "Feature",
    "stac_version": "1.0.0",
    "stac_extensions": [
      "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
      "https://stac-extensions.github.io/storage/v1.0.0/schema.json"
    ],
    "id": "S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600",
    "properties": {
      "datetime": "2015-05-19T12:00:00.000000Z",
    },
    "assets": {
      "PRODUCT": {
        "href": "the/path/is/not/important/S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600.tgz",
        "title": "Product",
        "type": "application/octet-stream",
      }
    }
  }},
  "collection":{"summary":"ItemCollection","value":{
    "type": "FeatureCollection",
    "features": [
        {
            "assets": {
                "PRODUCT": {
                    "href": "the/path/is/not/important/S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600.tgz",
                    "title": "Product",
                    "type": "application/octet-stream"
                }
            },
            "id": "S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600",
            "stac_extensions": [
                "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
                "https://stac-extensions.github.io/storage/v1.0.0/schema.json"
            ],
            "stac_version": "1.0.0",
            "type": "Feature",
            "properties": {
              "datetime": "2015-05-19T12:00:00.000000Z",
            },
        }
    ],
  }}})
                                        ):
  #Check if user is authorized to the collection
  (assets_source, assets_dest, stac_dest, datastore_url,catalogue_post_url,extra_auths) = check_user_collection_authorization(user_id,collectionId)

  #Check what type of GeoJSON this is and call the ingestion accordingly
  if 'type' in body and body['type']=='Feature':
    #Single item to ingest
    ingested_item = await add_item_to_collection(assets_source,assets_dest,stac_dest,datastore_url,catalogue_post_url,collectionId,body)
    #Construct response
    response_body = ingested_item
    if 'failure_reason' in response_body:
      if response_body['failure_reason'] == 'Item already exists':
        response_status=409
      else:
        response_status=422
    else:
      response_status=201
  elif 'type' in body and body['type']=='FeatureCollection' and 'features' in body and isinstance(body['features'],list) and len(body['features'])>0:
    #Multiple items to ingest
    ingested_items = await asyncio.gather(*[add_item_to_collection(assets_source,assets_dest,stac_dest,datastore_url,catalogue_post_url,collectionId,k) for k in body['features']])
    #Construct response
    response_body = { 'type':'FeatureCollection','features':ingested_items }
    response_status=201
    for ingested in ingested_items:
      if 'failure_reason' in ingested:
        if ingested['failure_reason'] == 'Item already exists':
          response_status=409
        else:
          response_status=422
          break
  else:
    #Invalid request
    raise HTTPException(status_code=422, detail="You need to post an item of the type Feature or FeatureCollection")

  return JSONResponse(status_code=response_status,content=response_body)

def valid_id_match(strg, search=re.compile(r'[^a-zA-Z0-9._-]').search, lenmax=100):
  if len(strg)>lenmax: return False
  return not bool(search(strg))

async def add_item_to_collection(assets_source: str, assets_dest: str, stac_dest: str, datastore_url: str, catalogue_post_url: str, collectionId: str, i: dict):
  #Check for validity of the STAC
  if 'id' not in i:
    return {"id":"unknown","failure_reason":"ID is required in the STAC item to be ingested"}
  if not valid_id_match(i['id'],lenmax=100):
    return {"id":i['id'],"failure_reason":"ID field is invalid. Only [a-zA-Z0-9._-] are allowed"}
  if 'collection' not in i:
    i['collection']=collectionId
  elif i['collection']!=collectionId:
    return {"id":i['id'],"failure_reason":"Collection ID contained in the STAC JSON is different from the one in the POST"}
  if 'links' not in i:
    #Add default empty links
    i['links']=[]
  if 'geometry' not in i:
    #Add default empty geometry
    i['geometry']=None
  if 'properties' not in i:
    return {"id":i['id'],"failure_reason":"Missing required property field from the STAC JSON"}
  if 'datetime' not in i['properties'] or i['properties']['datetime'] is None:
    if 'start_datetime' in i['properties']:
      item_datetime_str=i['properties']['start_datetime']
    else:
      return {"id":i['id'],"failure_reason":"datetime or start_datetime properties are mandatory"}
  else:
    item_datetime_str=i['properties']['datetime']
  try:
    item_datetime_obj=dt.datetime.fromisoformat(item_datetime_str)
  except Exception as e:
    return {"id":i['id'],"failure_reason":"Failed to parse product date time. {item_datetime_str} is an invalid ISO time"}

  #Extract all the local assets (and check that they are available)
  #Destination for the asset is calculated using the collection and the asset start time if present. This is to not overload the file system with too many assets and have one unique way of representing assets in the datastore.
  assets_base_date=item_datetime_obj.strftime(f'%Y{os.sep}%m{os.sep}%d')
  assets_base_path=os.path.join(assets_base_date,i["id"])
  assets_to_move_src=[]
  assets_to_move_dst=[]
  if 'assets' in i:
    assets_dict=i['assets']
    for asset_key in assets_dict:
      asset=assets_dict[asset_key]
      #Skip URLs and assets who do not have HREF from the assets to be moved
      if 'href' in asset and '://' not in asset['href']:
        #Check if the assets exist in the storage
        staging_asset_path=os.path.join(assets_source,asset['href'])
        if not os.path.exists(staging_asset_path):
          return {"id":i['id'],"failure_reason":f"{asset_key} asset {asset['href']} not found in the staging area location"}
        #Determine if the asset is a file or not (directories are not allowed for now, as there is no directory listing capability in a frontend download service)
        if os.path.islink(staging_asset_path):
          return {"id":i['id'],"failure_reason":f"{asset_key} asset {asset['href']} is a link. Sharing of links is not supported"}
        if not os.path.isfile(staging_asset_path):
          return {"id":i['id'],"failure_reason":f"{asset_key} asset {asset['href']} is not a file. Sharing of directories is not supported"}
        #Check asset file name is valid filename keys for a asset
        staging_asset_filename=os.path.basename(staging_asset_path)
        if not valid_id_match(staging_asset_filename,lenmax=100):
          return {"id":i['id'],"failure_reason":f"Asset {asset_key} file name is invalid. Only [a-zA-Z0-9._-] are allowed"}
        #Determine destination for the asset in the data store, this is used for both the access URL and the datastore destination path below
        ds_asset_path=os.path.join(assets_base_path,staging_asset_filename)
        #Rewrite the asset URL to make it point to the stagein
        i['assets'][asset_key]['href']=datastore_url+ds_asset_path
        #Add the item in the lists of assets to be moved
        if staging_asset_path in assets_to_move_src:
          #Skip this asset move, we already moved it before (to the same path)
          continue
        assets_to_move_src.append(staging_asset_path)
        staging_asset_path_dest=os.path.join(assets_dest,ds_asset_path)
        if staging_asset_path_dest in assets_to_move_dst:          
          return {"id":i['id'],"failure_reason":f"Asset {asset_key} file name is not unique. Another asset in the product has the same file name"}
        assets_to_move_dst.append(staging_asset_path_dest)
  
  #Post the STAC Item to the catalogue
  #Construct catalogue request
  stac_item=json.dumps(i).encode("utf-8")
  req = URLlibRequest(catalogue_post_url, stac_item, headers={'Content-Type':'application/geo+json'}, method='POST')
  try:
    response = URLlibUrlopen(req)
  except HTTPError as e:
    response_text=e.read().decode('utf-8')
    response_status=str(e)
    return {"id":i['id'],"failure_reason":f"Catalogue refused STAC: {response_status}: {response_text}"}
  except URLError as e:
    response_status='URLError'
    return {"id":i['id'],"failure_reason":f"Catalogue refused STAC: {response_status}: {response_text}"}
  except Exception as e:
    response_status='Exception'
    response_text=str(e)
    return {"id":i['id'],"failure_reason":f"Catalogue refused STAC: {response_status}: {response_text}"}

  #Now save STAC item
  backup_stac_item=os.path.join(os.path.join(stac_dest,assets_base_date),i['id'])
  try:
    os.makedirs(os.path.dirname(backup_stac_item), exist_ok=True)
    with open(backup_stac_item,'wb') as f:
      f.write(stac_item)
  except Exception as e:
    response_status='Exception'
    response_text=str(e)
    return {"id":i['id'],"failure_reason":f"Failed to store STAC in datastore: {response_status}: {response_text}"}

  #And move output
  for idx,asset_src in enumerate(assets_to_move_src):
    asset_dst=assets_to_move_dst[idx]
    try:
      #Move file
      os.makedirs(os.path.dirname(asset_dst), exist_ok=True)
      os.rename(asset_src,asset_dst)
      #Cleanup XATTR metadata (if any)
      if os.path.exists(asset_src+'.xattr'): os.remove(asset_src+'.xattr')
    except Exception as e:
      response_status='Exception'
      response_text=str(e)
      return {"id":i['id'],"failure_reason":f"Failed to store asset {os.path.basename(asset_dst)} in datastore: {response_status}: {response_text}"}

  #All ok, return the updated product
  return i

@app.delete(
  "/collections/{collectionId}/items/{recordId}",
  tags=["Implemented tranaction operations:"],
  summary="Delete an Item or an ItemCollection from a collection",
  description="""This call allows to **delete** a STAC Item or ItemCollection from the catalogue and datastore.

Please note that this action cannot be reversed, and it will delete all the assets from the catalogue and datastore including all backups. It should be used with lots of caution and for testing only.

Please note that this API will require special authorization. **Only administrators should be able to use it.**""",
  status_code=201,
  responses={
      201: {"description": "Successful deletion.","content":{"application/json":{"examples":{"Item":{"value":
      {"id": "the id of the item deleted",
       "message": "deleted"}
      }}}}},
      422: {"description": "Validation error. Failed to add one or more items. Error in the response","content":{"application/json":{"examples":{"Item":{"value":
      {"id": "the id of the item to be deleted",
       "failure_reason": "a test describing the error"}
      }}}}}
    }
)
async def collection_items_del_request(
  user_id: int = Depends(get_current_username),
  collectionId: str = Path(example="PRR_TEST"),
  recordId: str = Path(example="S3A_OPER_AUX_GNSSRD_POD__20171212T193142_V20160223T235943_20160224T225600")):

  #Check if user is authorized to delete from the collection
  (assets_source, assets_dest, stac_dest, datastore_url,catalogue_post_url,extra_auths) = check_user_collection_authorization(user_id,collectionId)
  if extra_auths % 2 == 0:
    raise HTTPException(status_code=422, detail="User is not authorized to delete items in this collection")

  #Final failure reason
  failure_reason=''

  #Get the product from the catalogue (and also check it is actually there)
  req = URLlibRequest(os.path.join(catalogue_post_url,recordId), headers={'Content-Type':'application/geo+json'}, method='GET')
  try:
    response = URLlibUrlopen(req)
  except HTTPError as e:
    response_text=e.read().decode('utf-8')
    response_status=str(e)
    failure_reason=f"Catalogue refused GET: {response_status}: {response_text}"
  except URLError as e:
    response_status='URLError'
    response_text=str(e)
    failure_reason=f"Catalogue refused GET: {response_status}: {response_text}"
  except Exception as e:
    response_status='Exception'
    response_text=str(e)
    failure_reason=f"Catalogue refused GET: {response_status}: {response_text}"
  if failure_reason!='':
    raise HTTPException(status_code=422, detail=failure_reason)
  try:
    i=json.loads(response.read())
  except Exception as e:
    response_status='Exception'
    response_text=str(e)
    failure_reason=f"Catalogue refused GET: {response_status}: {response_text}"
    raise HTTPException(status_code=422, detail=failure_reason)

  #Extract datetime and determine paths of products to delete
  if 'properties' not in i:
    raise HTTPException(status_code=422, detail={"id":i['id'],"failure_reason":"Missing required property field from the STAC JSON"})
  if 'datetime' not in i['properties'] or i['properties']['datetime'] is None:
    if 'start_datetime' in i['properties']:
      item_datetime_str=i['properties']['start_datetime']
    else:
      raise HTTPException(status_code=422, detail={"id":i['id'],"failure_reason":"datetime or start_datetime properties are mandatory"})
  else:
    item_datetime_str=i['properties']['datetime']
  try:
    item_datetime_obj=dt.datetime.fromisoformat(item_datetime_str)
  except Exception as e:
    raise HTTPException(status_code=422, detail={"id":i['id'],"failure_reason":"Failed to parse product date time. {item_datetime_str} is an invalid ISO time"})
  assets_base_date=item_datetime_obj.strftime(f'%Y{os.sep}%m{os.sep}%d')
  assets_base_path=os.path.join(assets_base_date,i["id"])
  assets_path_to_delete=os.path.join(assets_dest,assets_base_path)
  stacs_path_to_delete=os.path.join(os.path.join(stac_dest,assets_base_date),i['id'])

  #Delete element form the catalogue collection
  req = URLlibRequest(os.path.join(catalogue_post_url,recordId), headers={'Content-Type':'application/geo+json'}, method='DELETE')
  try:
    response = URLlibUrlopen(req)
  except HTTPError as e:
    response_text=e.read().decode('utf-8')
    response_status=str(e)
    failure_reason=f"Catalogue refused DELETE: {response_status}: {response_text}"
  except URLError as e:
    response_status='URLError'
    response_text=str(e)
    failure_reason=f"Catalogue refused DELETE: {response_status}: {response_text}"
  except Exception as e:
    response_status='Exception'
    response_text=str(e)
    failure_reason=f"Catalogue refused DELETE: {response_status}: {response_text}"
  if failure_reason!='':
    raise HTTPException(status_code=422, detail=failure_reason)

  #Delete STAC Item backup
  if os.path.exists(stacs_path_to_delete):
    os.remove(stacs_path_to_delete)
  else:
    failure_reason='Cannot delete STAC Item backup. It does not exist.'
  #Delete STAC assets
  if os.path.exists(assets_path_to_delete):
    shutil.rmtree(assets_path_to_delete)
  else:
    failure_reason=failure_reason+'Cannot delete STAC Item Assets. They do not exist.'

  #Return result
  if failure_reason=='':
    return JSONResponse(status_code=201,content={"id":recordId,"message":"deleted"})
  else:
    raise HTTPException(status_code=422, detail=failure_reason)

