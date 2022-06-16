"""Process S3-based geotiffs, using rio-cogeo (gdal)."""

import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Union

import rasterio
from envidat.s3.bucket import Bucket
from envidat.utils import get_logger, load_dotenv_if_in_debug_mode
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
    """Convert image to COG."""
    output_profile = cog_profiles.get(profile)
    output_profile.update(profile_options)

    # https://gdal.org/drivers/raster/cog.html
    # https://developers.google.com/earth-engine/Earth_Engine_asset_from_cloud_geotiff
    config = dict(
        GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_TIFF_INTERNAL_MASK=True,
        GDAL_TIFF_OVR_BLOCKSIZE="128",
        BLOCKXSIZE="256",
        BLOCKYSIZE="256",
        ZLEVEL=9,
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


def process_cog(
    data: Union[str, bytes, Path],
    profile_options: dict = {},
    dst_path: Union[str, Path] = None,
    compress: bool = False,
    **options,
) -> bool:
    """Convert an in-memory GeoTIFF to COG."""
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

    log.info("Reading tiff with rasterio")
    geotiff = rasterio.open(src_path)
    if compress:
        # WebP only supports 3-4 band images
        if geotiff.count >= 3:
            log.debug("Setting output profile to webp")
            profile = "webp"
        else:
            log.debug("Setting output profile to jpeg")
            profile = "jpeg"
    else:
        log.debug("Setting output profile to deflate")
        profile = "deflate"

    if dst_path is None:
        dst_path = src_path.with_name(
            src_path.stem + f"_COG_{profile}" + src_path.suffix
        )

    log.info(
        "Creating COG with params "
        f"src_path: {src_path} | dst_path: {dst_path} "
        f"profile: {profile} | options: {options}"
    )
    _translate(
        geotiff,
        dst_path,
        profile,
        profile_options=profile_options,
        **options,
    )
    log.info("Validating generated COG file")
    cog_validate(dst_path)

    return dst_path


def main():
    """Run main script logic."""
    load_dotenv_if_in_debug_mode(env_file=".env.secret")
    get_logger()

    log.info("Starting main COG generator script.")

    prefix = "wsl/uav-datasets-for-three-alpine-glaciers/"
    tiffs = [
        # "findelen_20160419/findelen_20160419_photoscan_dsm_CH1903+_LV95_0.1m.tif",
        "findelen_20160419/findelen_20160419_photoscan_oi_CH1903+_LV95_0.1m.tif",
        # "gries_20150926/gries_20150926_photoscan_dsm_CH1903+_LV95_0.1m.tif",
        "gries_20150926/gries_20150926_photoscan_oi_CH1903+_LV95_0.1m.tif",
        # "stanna_20150928/stanna_20150928_photoscan_dsm_CH1903+_LV95_0.1m.tif",
        "stanna_20150928/stanna_20150928_photoscan_oi_CH1903+_LV95_0.1m.tif",
    ]
    tiffs = [f"{prefix}{tiff_key}" for tiff_key in tiffs]

    s3_drone_data = Bucket("drone-data", is_new=True, is_public=True)
    s3_envicloud = Bucket(bucket_name="envicloud")

    for tiff_key in tiffs:
        s3_envicloud.transfer(tiff_key, "drone-data", tiff_key)

        # Set destination key in bucket for COG
        src_key = Path(tiff_key)
        dst_key = str(src_key.with_name(src_key.stem + "_COG" + src_key.suffix))

        # # Set this env variable in a K8S setup to an emptyDir volume
        temp_dir = os.getenv("TEMP_DIR", default="/tmp")
        with NamedTemporaryFile(dir=temp_dir, suffix=".tif") as temp_file:

            s3_drone_data.download_file(tiff_key, temp_file.name)
            cog_path = process_cog(
                temp_file.name,
                web_optimized=True,
                # profile_options={"jpeg"},
            )
            try:
                s3_drone_data.upload_file(dst_key, cog_path)
            finally:
                # Cleanup
                Path(cog_path).unlink(missing_ok=True)

    s3_drone_data.set_cors_config(allow_all=True)

    log.info("Finished main opendataswiss script.")


if __name__ == "__main__":
    main()
