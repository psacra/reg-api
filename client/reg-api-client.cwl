cwlVersion: v1.2
class: CommandLineTool
baseCommand: reg-api-client
arguments:
 - --rapi-endpoint
 - eoresults.esa.int
 - --rapi-s3-endpoint
 - eoresults.esa.int
inputs:
  product:
    label: Input product
    doc: STAC Item, STAC Collection, STAC Catalog or binary asset to be published.
    type: string
    default: "https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/10/T/FK/2021/7/S2B_10TFK_20210713_0_L2A/TCI.tif"
    inputBinding:
      position: 1
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
      prefix -p
outputs: []

requirements:
  DockerRequirement:
    dockerPull: localhost/reg-api-client:latest
  NetworkAccess:
    networkAccess: true
