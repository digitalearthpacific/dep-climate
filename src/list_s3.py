from odc.stac import configure_s3_access
import boto3

print("Starting...")

configure_s3_access(cloud_defaults=True, requester_pays=True)
s3 = boto3.client("s3")

response = s3.list_buckets()

# Output the bucket names
#print('Existing buckets:')
#for bucket in response['Buckets']:
#    print(f'  {bucket["Name"]}')

response = s3.list_objects_v2(
    Bucket='dep-public-staging',
    Prefix ='dep_ls_climate',
    MaxKeys=9999999,
)

#print(response)
prefix = "https://dep-public-staging.s3.us-west-2.amazonaws.com/"
asset = "tavg"
for content in response['Contents']:
    url = f'{content["Key"]}'
    if (url.endswith(f"_{asset}.tif")):
        url = prefix + url
        print(url)