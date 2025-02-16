import odc.geo.xr  # noqa: F401

from logging import INFO, Formatter, Logger, StreamHandler, getLogger
import boto3
import odc.geo.xr  # noqa: F401
import typer
from dea_tools.dask import create_local_dask_cluster

# from dask.distributed import Client
from dep_tools.aws import write_stac_s3
import dep_tools.grids as pgrid
from dep_tools.namers import S3ItemPath
from dep_tools.stac_utils import StacCreator, set_stac_properties
from dep_tools.writers import (
    AwsDsCogWriter,
)
from geopandas import GeoDataFrame

from odc.stac import configure_s3_access
from typing_extensions import Annotated, Optional
import numpy as np
import xarray as xr
import warnings

warnings.filterwarnings("ignore")


# NIU uv run src/run.py --tile-id 77,19 --year 2024 --version 0.0.1
# NRU uv run src/run.py --tile-id 50,41 --year 2024 --version 0.0.1
# FJI_Coral_Coast uv run src/run.py --tile-id 84,63 --year 2024 --version 0.0.1


# Main
def main(
        country_code: Annotated[str, typer.Option()],
        output_bucket: str = "dep-public-staging",
        dataset_id: str = "climate",
        base_product: str = "ls",
        version: str = "1.0.0",
        memory_limit: str = "64GB",
        workers: int = 4,
        threads_per_worker: int = 32,
) -> None:
    tc_folder = "/Users/sachin/Documents/TerraClimate/"

    log = get_logger(country_code)
    log.info("Starting processing...")

    xr.set_options(keep_attrs=False)

    # dask and aws
    client = create_local_dask_cluster(return_client=True)
    """
    client = DaskClient(
        n_workers=workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
    )
    """
    log.info(client)

    configure_s3_access(cloud_defaults=True, requester_pays=True)

    # get aoi
    grid = pgrid.gadm()
    aoi = grid[grid["GID_0"] == country_code]

    # buffer
    aoi = aoi['geometry'].buffer(0.02)
    log.info(f"{country_code}")

    # generate product
    data = xr.open_mfdataset(tc_folder + "*.nc", parallel=False, engine="h5netcdf", decode_coords="all")
    data = data.rio.write_crs("EPSG:4326")
    data = data.rio.clip(aoi.geometry, aoi.crs, drop=True)

    # Add Average Temperature Variable
    data['tavg'] = (data['tmax'] + data['tmin']) / 2

    # coastline clip
    data = get_clipped(data, grid[grid["GID_0"] == country_code])

    # write locally
    data = data.drop_attrs(deep=True)
    data = data.rio.write_crs("EPSG:4326")
    #data = data.compute()
    # print(data)

    # publish
    index = 0
    for t in data.time.to_numpy():
        ds_source = data.isel(time=index).squeeze()
        datetime = np.datetime_as_string(t, unit="D")
        publish(
            ds_source,
            ds_source,
            base_product,
            dataset_id,
            log,
            output_bucket,
            country_code,
            version,
            datetime,
        )
        index = index + 1

    # finish
    log.info(f"{country_code} Processed.")
    client.close()


def publish(
        ds,
        ds_source,
        base_product,
        dataset_id,
        log,
        output_bucket,
        country_code,
        version,
        datetime,
):
    aws_client = boto3.client("s3")
    # itempath
    itempath = S3ItemPath(
        bucket=output_bucket,
        sensor=base_product,
        dataset_id=dataset_id,
        version=version,
        time=datetime,
        prefix="dep",
    )
    stac_document = itempath.stac_path(country_code)
    # write externally
    output_data = set_stac_properties(ds_source, ds)
    writer = AwsDsCogWriter(
        itempath=itempath,
        overwrite=True,
        convert_to_int16=True,
        extra_attrs=dict(dep_version=version),
        write_multithreaded=True,
        client=aws_client,
    )
    paths = writer.write(output_data, country_code) + [stac_document]
    stac_creator = StacCreator(
        itempath=itempath, remote=True, make_hrefs_https=True, with_raster=True
    )
    stac_item = stac_creator.process(output_data, country_code)
    write_stac_s3(stac_item, stac_document, output_bucket)
    if paths is not None:
        log.info(f"Completed writing to {paths[-1]}")
    else:
        log.warning("No paths returned from writer")


def get_clipped(ds, buffer) -> GeoDataFrame:
    buffer = buffer.to_crs(4326)
    try:
        ds = ds.rio.clip(buffer.geometry.values, buffer.crs, drop=True, invert=False)
    except:
        pass
    return ds


# Logger
def get_logger(region_code: str) -> Logger:
    """Set Logger"""
    console = StreamHandler()
    time_format = "%Y-%m-%d %H:%M:%S"
    console.setFormatter(
        Formatter(
            fmt=f"%(asctime)s %(levelname)s ({region_code}):  %(message)s",
            datefmt=time_format,
        )
    )
    log = getLogger("FRACTIONAL_COVER")
    log.addHandler(console)
    log.setLevel(INFO)
    return log


# Run
if __name__ == "__main__":
    typer.run(main)
