# This logic only works locally on the LEAP-Pangeo hub (or similar Jupyterhubs)
import os
import subprocess
import s3fs

user = os.environ['JUPYTERHUB_USER']

#TODO: factor this out into an importable function and import here and in config_local.py
try:
    # Run the git command to get the top-level directory path
    repo_path = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
    # Use os.path.basename to get the repository name from the path
    repo_name = os.path.basename(repo_path)
except subprocess.CalledProcessError as e:
    raise

BUCKET_PREFIX = f"gs://leap-scratch/{user}/{repo_name}"
print(f"{BUCKET_PREFIX=}")

# access_key_id = os.environ['OSN_LEAP_PIPELINE_KEY']
# secret_access_key = os.environ['OSN_LEAP_PIPELINE_KEY_SECRET']
access_key_id = os.environ['access_key_id']
secret_access_key = os.environ['secret_access_key']

c.Bake.prune = True
c.Bake.bakery_class = "pangeo_forge_runner.bakery.local.LocalDirectBakery"

c.InputCacheStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.InputCacheStorage.root_path = f"{BUCKET_PREFIX}/cache/"

c.TargetStorage.fsspec_class = "s3fs.S3FileSystem"
s3_args = {
       "key": access_key_id,
       "secret": secret_access_key,
       "client_kwargs":{"endpoint_url":"https://nyu1.osn.mghpcc.org"}
   }

c.TargetStorage.fsspec_args = s3_args
c.TargetStorage.root_path = f"leap-pangeo-pipeline/CASM/"
