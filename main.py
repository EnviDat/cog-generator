"""Process S3-based geotiffs, using rio-cogeo (gdal)."""

import logging
import os
import urllib.parse
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import NoReturn, Union

import click
import rasterio
from envidat.s3.bucket import Bucket
from envidat.utils import get_logger
from rasterio.io import DatasetReader
from rio_cogeo.cogeo import cog_translate, cog_validate
from rio_cogeo.profiles import cog_profiles

log = logging.getLogger(__name__)


def _translate(
    src_path: Union[str, Path],
    dst_path: Union[str, Path],
    profile: str,
    profile_options: dict = {},
    **options,
) -> bool:
    """Convert TIFF to COG using GDAL translate."""
    output_profile = cog_profiles.get(profile)
    # Extra info on options:
    # https://gdal.org/drivers/raster/cog.html
    # https://developers.google.com/earth-engine/Earth_Engine_asset_from_cloud_geotiff
    output_profile.update(profile_options)

    # Dataset Open options (see gdalwarp `-oo` option)
    config = dict(
        GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_TIFF_INTERNAL_MASK=True,
        GDAL_TIFF_OVR_BLOCKSIZE="128",
    )

    cog_translate(
        src_path,
        dst_path,
        output_profile,
        config=config,
        use_cog_driver=True,
        **options,
    )
    return True


def process_cog_with_params(
    data: Union[str, bytes, Path, DatasetReader],
    dst_path: Union[str, Path] = None,
    profile_options: dict = {
        "blockxsize": 256,
        "blockysize": 256,
        "BLOCKSIZE": 256,  # COG
        "LEVEL": 9,  # COG
        "ZLEVEL": 9,  # GTIFF
        "BIGTIFF": "IF_SAFER",
        "NUM_THREADS": "ALL_CPUS",
    },
    compress: bool = False,
    is_dem: bool = False,
    smooth_dem: bool = False,
    **options,
) -> str:
    """Set params and send tiff to translate function."""
    if isinstance(data, (str, Path)):
        src_path = Path(data).resolve()
        if not src_path.is_file():
            raise OSError("Input file does not exist on disk")

    elif isinstance(data, bytes):
        temp_dir = os.getenv("TEMP_DIR", default="/tmp")
        log.debug(f"Loading data into tempfile in dir: {temp_dir}")
        temp_file = NamedTemporaryFile(dir=temp_dir, delete=False, suffix=".tiff")
        temp_file.write(data)
        src_path = Path(temp_file.name)

    if isinstance(data, DatasetReader):
        log.info("Data already in rasterio format")
        geotiff = data
        temp_dir = os.getenv("TEMP_DIR", default="/tmp")
        src_path = Path(f"{temp_dir}/{uuid.uuid4()}.tif")

    else:
        log.info("Reading tiff with rasterio")
        geotiff = rasterio.open(src_path)

    log.debug("Default output profile = deflate")
    profile = "deflate"

    if is_dem:
        """
        Don't lossy compress DEMs, use DEFLATE (or artifacts / steps)
        https://kokoalberti.com/articles/geotiff-compression-optimization-guide/
        GDAL PREDICTOR
            To improve size savings, use a predictor: PREDICTOR=2
            If your data is floating point values, use PREDICTOR=3
        GDAL RESAMPLING
            Use BILINEAR for DEM, as NEAREST (neighbour)
                produces grid/herringbone artifacts
            If artifacts remain, switch to CUBIC for further smoothing
        """
        if all(x == "float32" for x in geotiff.dtypes):
            profile_options.update({"PREDICTOR": 3})
        else:
            profile_options.update({"PREDICTOR": 2})
        profile_options.update(
            {"RESAMPLING": "CUBIC"} if smooth_dem else {"RESAMPLING": "BILINEAR"}
        )

    elif compress:
        # # WebP only supports 3-4 band images
        # if geotiff.count >= 3:
        #     log.debug("Setting output profile to webp")
        #     profile = "webp"
        #     profile_options.update({"quality": 85})
        #     # TODO test webp, currently freezes
        #     # Possibly due to: UserWarning: Nodata/Alpha band will
        #     # be translated to an internal mask band.
        # else:
        #     log.debug("Setting output profile to jpeg")
        #     profile = "jpeg"
        log.debug("Setting output profile to jpeg")
        profile = "jpeg"
        profile_options.update({"QUALITY": 85})

    if dst_path is None:
        dst_path = src_path.with_name(
            src_path.stem + f"_COG_{profile}" + src_path.suffix
        )
        log.debug(f"Set output path to {dst_path}")

    log.info(
        "Creating COG with params "
        f"src_path: {str(src_path)} | dst_path: {dst_path} | profile: {profile} "
        f"| profile_options: {profile_options} | options: {options}"
    )
    _translate(
        geotiff,
        dst_path,
        profile,
        profile_options=profile_options,
        **options,
    )
    log.info(f"Validating generated COG file at {dst_path}")
    cog_validate(dst_path)

    return dst_path


def process_cog_list(
    tiff_keys: Union[list, str],
    replicate_from_bucket: str = None,
    preload: bool = False,
    overwrite: bool = False,
    compress: bool = False,
    is_dem: bool = False,
    smooth_dem: bool = False,
    web_optimized: bool = False,
) -> NoReturn:
    """
    Process S3 based TIFFs to create a data copy in COG format.

    The S3 bucket to access data from is pulled from the
        BUCKET_NAME environment variable.
    The TEMP_DIR environment variable can be specified for writing temporary files.
        This particularly import in containers, where the default /tmp is in memory.
        Change to a valid disk based location, for example an emptyDir in /data.

    Args:
        tiff_key (list,str): List of strings containing S3 keys to tiff files.
            Must be only a relative key from bucket root, without the SWITCH URL.
            E.g. wsl/uav-datasets...
        replicate_from_bucket (str): S3 bucket to replicate data from, prior
            to processing. Useful if the data is in another source bucket,
            but you don't want the output to be produced there.
        preload (bool): Load the COG file into memory before processing.
            Only use for smaller files < 4-8GB.
        overwrite (bool): Overwrite destination S3 file, if exists.
        compress (bool): Use lossy compression for the internal tiling. JPEG or WEBP.
        is_dem (bool): If the input data is a DEM, DSM, etc.
        smooth_dem (bool): Set if the output DEM COG has artifacts and requires further
            smoothing, using cubic resampling.
        web_optimized (bool): Re-project the data to web mercator for
            web map consumption. EPSG 3857.
    """
    bucket_name = os.getenv("BUCKET_NAME", default="cog")

    if isinstance(tiff_keys, (str)):
        log.debug("String input provided, converting to list")
        tiff_keys = [tiff_keys]

    s3_cog = Bucket(bucket_name, is_new=True, is_public=True)
    if replicate_from_bucket:
        log.debug("replicate_from_bucket set, instantiating bucket")
        s3_from = Bucket(bucket_name=replicate_from_bucket)

    profile = "jpeg" if compress else "deflate"

    for tiff_key in tiff_keys:

        # Set destination key in bucket for COG
        src_key = Path(tiff_key)
        dst_key = str(
            src_key.with_name(src_key.stem + f"_COG_{profile}" + src_key.suffix)
        )

        if not overwrite:
            if s3_cog.check_file_exists(dst_key):
                log.info(
                    f"Key {dst_key} already exists in bucket {bucket_name}. "
                    "Skipping COG creation..."
                )
                continue

        if replicate_from_bucket:
            log.info(
                f"Copying TIFF data from bucket named {replicate_from_bucket} "
                f"to bucket named {bucket_name}"
            )
            s3_from.transfer(tiff_key, bucket_name, tiff_key)

        if preload:

            # # Set this env variable in a K8S setup to an emptyDir volume
            temp_dir = os.getenv("TEMP_DIR", default="/tmp")
            with NamedTemporaryFile(dir=temp_dir, suffix=".tif") as temp_file:

                s3_cog.download_file(tiff_key, temp_file.name)

                cog_path = process_cog_with_params(
                    temp_file.name,
                    compress=compress,
                    is_dem=is_dem,
                    smooth_dem=smooth_dem,
                    web_optimized=web_optimized,
                )

        else:

            suffix_safe = urllib.parse.quote(tiff_key, safe="")

            log.debug("Opening rasterio tiff directly from S3")
            with rasterio.open(
                f"https://{bucket_name}.s3-zh.os.switch.ch/{suffix_safe}"
            ) as src_geotiff:

                cog_path = process_cog_with_params(
                    src_geotiff,
                    compress=compress,
                    is_dem=is_dem,
                    smooth_dem=smooth_dem,
                    web_optimized=web_optimized,
                )

        try:
            s3_cog.upload_file(dst_key, cog_path)
        finally:
            # Cleanup
            Path(cog_path).unlink(missing_ok=True)


@click.command()
@click.option("--tiff", "tiff_keys", help="URL to S3 file for processing.")
@click.option("--bucket", "bucket_name", help="The S3 bucket to read and write from.")
@click.option(
    "--replicate-from",
    "replicate_from_bucket",
    required=False,
    help="S3 bucket to replicate data from, prior to processing.",
)
@click.option(
    "--preload", required=False, help="Load the COG file into memory before processing."
)
@click.option(
    "--overwrite",
    "overwrite",
    required=False,
    help="Overwrite destination S3 file, if exists.",
)
@click.option(
    "--compress", required=False, help="Use lossy compression for the internal tiling."
)
@click.option(
    "--dem", "is_dem", required=False, help="If the input data is a DEM, DSM, etc."
)
@click.option(
    "--smooth-dem",
    "--smooth_dem",
    required=False,
    help="Set if the output DEM COG requires further smoothing.",
)
@click.option(
    "--web-optimise",
    "web_optimized",
    required=False,
    help="Re-project the data to EPSG:3857.",
)
def command_line_run(
    tiff_keys: Union[list, str],
    bucket_name: str,
    replicate_from_bucket: str = None,
    preload: bool = False,
    overwrite: bool = False,
    compress: bool = False,
    is_dem: bool = False,
    smooth_dem: bool = False,
    web_optimized: bool = False,
) -> NoReturn:
    """
    Process S3 based TIFF to create a data copy in COG format.

    Current working directory must contain .env.secret file, with AWS credentials:

    \b
    LOG_LEVEL=INFO
    TEMP_DIR=/tmp
    AWS_ENDPOINT=xxx
    AWS_REGION=xxx
    AWS_ACCESS_KEY=xxx
    AWS_SECRET_KEY=xxx
    """
    from dotenv import load_dotenv

    load_dotenv(".env.secret")
    get_logger()

    if bucket_name is None:
        bucket_name = os.getenv("BUCKET_NAME")

    process_cog_list(
        tiff_keys,
        replicate_from_bucket=replicate_from_bucket,
        preload=preload,
        overwrite=overwrite,
        compress=compress,
        is_dem=is_dem,
        smooth_dem=smooth_dem,
        web_optimized=web_optimized,
    )


if __name__ == "__main__":
    command_line_run()
