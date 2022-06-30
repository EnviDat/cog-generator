"""Usage example with drone data for calling function directly."""

import logging

from envidat.utils import get_logger, load_dotenv_if_in_debug_mode

from main import process_cog_list

log = logging.getLogger(__name__)


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
