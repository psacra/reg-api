cwlVersion: v1.1
class: CommandLineTool
#baseCommand: reg-api-client
arguments:
 - --rapi-endpoint
 - https://eoresults.esa.int/reg-api
 - --rapi-s3-endpoint
 - https://eoresults.esa.int
inputs:
  product:
    label: Input product
    doc: STAC Item, STAC Collection, STAC Catalog or binary asset to be published.
    type: string
    inputBinding:
      position: 0
  collection:
    label: Collection ID
    doc: ID of the collection where the product will be registered
    type: string
    inputBinding:
      prefix: -c
  username:
    label: Username
    doc: Username of the user authorized to publish the product in the collection
    type: string
    inputBinding:
      prefix: -u
  password:
    label: Password
    doc: Password of the user authroized to publish the product in the collection
    type: string
    inputBinding:
      prefix: -p
outputs: []

requirements:
  DockerRequirement:
    dockerPull: eoepca/reg-api-client:latest
  NetworkAccess:
    networkAccess: true
