"""Process S3-based geotiffs, using rio-cogeo (gdal)."""

import logging
import os
import urllib.parse
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import NoReturn, Union

import rasterio
from envidat.s3.bucket import Bucket
from envidat.utils import get_logger, load_dotenv_if_in_debug_mode
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
    **options,
) -> tuple:
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

    log.debug("Default output profile=deflate")
    profile = "deflate"

    if is_dem:
        """
        Don't lossy compress DEMs, use DEFLATE (or artifacts / steps)
        To improve size savings, use a predictor: GDAL PREDICTOR=2
        If your data is floating point values, use PREDICTOR=3
        https://kokoalberti.com/articles/geotiff-compression-optimization-guide/
        """
        profile_options.update({"PREDICTOR": 2})

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
        log.debug("Setting output path to write to")
        dst_path = src_path.with_name(
            src_path.stem + f"_COG_{profile}" + src_path.suffix
        )

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

    return (dst_path, profile)


def process_cog_list(
    tiff_key_list: list,
    s3_copy_from: str = None,
    preload: bool = False,
    compress: bool = False,
    is_dem: bool = False,
    web_optimized: bool = False,
) -> NoReturn:
    """
    Copy file to new bucket (if needed) and process.

    Args:
        tiff_key_list (list): List of strings containing S3 keys to tiff files.
        s3_copy_from
        preload (bool): Set to True to load the COG file into memory before processing.
    """
    s3_drone_data = Bucket("drone-data", is_new=True, is_public=True)
    if s3_copy_from:
        s3_from = Bucket(bucket_name=s3_copy_from)

    for tiff_key in tiff_key_list:

        if s3_copy_from:
            s3_from.transfer(tiff_key, "drone-data", tiff_key)

        if preload:

            # # Set this env variable in a K8S setup to an emptyDir volume
            temp_dir = os.getenv("TEMP_DIR", default="/tmp")
            with NamedTemporaryFile(dir=temp_dir, suffix=".tif") as temp_file:

                s3_drone_data.download_file(tiff_key, temp_file.name)

                cog_path, cog_format = process_cog_with_params(
                    temp_file.name,
                    compress=compress,
                    is_dem=is_dem,
                    web_optimized=web_optimized,
                )

                # Set destination key in bucket for COG
                src_key = Path(tiff_key)
                dst_key = str(
                    src_key.with_name(
                        src_key.stem + f"_COG_{cog_format}" + src_key.suffix
                    )
                )

        else:

            suffix_safe = urllib.parse.quote(tiff_key, safe="")
            with rasterio.open(
                f"https://drone-data.s3-zh.os.switch.ch/{suffix_safe}"
            ) as src_geotiff:

                cog_path, cog_format = process_cog_with_params(
                    src_geotiff,
                    compress=compress,
                    is_dem=is_dem,
                    web_optimized=web_optimized,
                )

                # Set destination key in bucket for COG
                src_key = Path(tiff_key)
                dst_key = str(
                    src_key.with_name(
                        src_key.stem + f"_COG_{cog_format}" + src_key.suffix
                    )
                )

        try:
            s3_drone_data.upload_file(dst_key, cog_path)
        finally:
            # Cleanup
            Path(cog_path).unlink(missing_ok=True)


def main():
    """Run main script logic."""
    load_dotenv_if_in_debug_mode(env_file=".env.secret")
    get_logger()

    log.info("Starting main COG generator script.")

    prefix = "wsl/uav-datasets-for-three-alpine-glaciers/"
    optical_tiffs = [
        "findelen_20160419/findelen_20160419_photoscan_oi_CH1903+_LV95_0.1m.tif",
        "gries_20150926/gries_20150926_photoscan_oi_CH1903+_LV95_0.1m.tif",
        "stanna_20150928/stanna_20150928_photoscan_oi_CH1903+_LV95_0.1m.tif",
    ]
    dem_tiffs = [
        "findelen_20160419/findelen_20160419_photoscan_dsm_CH1903+_LV95_0.1m.tif",
        "gries_20150926/gries_20150926_photoscan_dsm_CH1903+_LV95_0.1m.tif",
        "stanna_20150928/stanna_20150928_photoscan_dsm_CH1903+_LV95_0.1m.tif",
    ]
    optical_tiffs = [f"{prefix}{tiff_key}" for tiff_key in optical_tiffs]
    dem_tiffs = [f"{prefix}{tiff_key}" for tiff_key in dem_tiffs]

    process_cog_list(optical_tiffs, s3_copy_from="envicloud", compress=True)
    process_cog_list(dem_tiffs, s3_copy_from="envicloud", is_dem=True)

    log.info("Finished main COG generator script.")


if __name__ == "__main__":
    main()
