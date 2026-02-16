#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.adpc_gem._version import __version__
from hdx.scraper.adpc_gem.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-adpc-gem"
_SAVED_DATA_DIR = "saved_data"
_UPDATED_BY_SCRIPT = "HDX Scraper: ADPC GEM"


def main(
    save: bool = False,
    use_saved: bool = False,
    countries: str = "",
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save: Save downloaded data. Defaults to False.
        use_saved: Use saved data. Defaults to False.
        countries: Comma-separated ISO3 codes to process. Empty for all.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("hdx")

    # Country filter for testing
    country_filter = None
    if countries:
        country_filter = {c.strip().upper() for c in countries.split(",")}
        logger.info(f"Filtering to countries: {country_filter}")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        temp_dir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )
            pipeline = Pipeline(configuration, retriever, temp_dir)

            for country_data in pipeline.get_country_data():
                # Skip if not in filter
                if country_filter and country_data["iso3"] not in country_filter:
                    continue

                dataset = pipeline.generate_dataset(country_data)
                if dataset:
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        match_resource_order=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=info["batch"],
                    )


if __name__ == "__main__":
    facade(
        main,
        hdx_site="demo",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
