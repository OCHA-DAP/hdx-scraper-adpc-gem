from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.adpc_gem.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestAdpcGem",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                # Test data for Cambodia
                country_data = None
                for data in pipeline.get_country_data():
                    if data["iso3"] == "KHM":
                        country_data = data
                        break

                dataset = pipeline.generate_dataset(country_data)
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert dataset == {
                    "caveats": None,
                    "name": "khm-adpc-gem",
                    "title": "Cambodia - Gender Equality Monitor",
                    "dataset_date": "[1990-01-01T00:00:00 TO 2019-12-31T23:59:59]",
                    "tags": [
                        {
                            "name": "education",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "employment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "gender",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "health",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by",
                    "methodology": "TBD",
                    "dataset_source": "Asian Disaster Preparedness Center",
                    "groups": [{"name": "khm"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "1",
                    "maintainer": "05127438-567c-48a0-afd6-308a1d1808f6",
                    "owner_org": "372eabc7-d7b8-42a0-af47-52d9849edd02",
                    "data_update_frequency": -2,
                    "notes": "The Gender Equality Monitoring (GEM) platform offers open access to "
                    "officially published data and periodically updated sex "
                    "disaggregated data repository. It visualizes gender gaps at "
                    "subnational level in various sectors such as education, health, "
                    "employment, access to information, intra-household decision making, "
                    "political participation, as well as gender inequality index (GII). "
                    "These are starting points to further explore the causes and more "
                    "complex dynamics of gender inequality.\n"
                    "\n"
                    "For more information, visit the [ADPC Gender Equality "
                    "Monitor](https://gem-servir.adpc.net/).\n",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "khm-gem-gii-national.csv",
                        "description": "National Gender Inequality Index scores for Cambodia",
                        "format": "csv",
                    },
                    {
                        "name": "khm-gem-gii-subnational.csv",
                        "description": "Subnational Gender Inequality Index scores for Cambodia",
                        "format": "csv",
                    },
                    {
                        "name": "khm-gem-dimension-national.csv",
                        "description": "National Gender Inequality Index by dimension for Cambodia",
                        "format": "csv",
                    },
                    {
                        "name": "khm-gem-dimension-subnational.csv",
                        "description": "Subnational Gender Inequality Index by dimension for Cambodia",
                        "format": "csv",
                    },
                    {
                        "name": "khm-gem-indicator-national.csv",
                        "description": "National Gender Inequality Index by indicator for Cambodia",
                        "format": "csv",
                    },
                    {
                        "name": "khm-gem-indicator-subnational.csv",
                        "description": "Subnational Gender Inequality Index by indicator for Cambodia",
                        "format": "csv",
                    },
                    {
                        "name": "khm-gem-sex-disaggregated.csv",
                        "description": "Sex-disaggregated data for Cambodia",
                        "format": "csv",
                    },
                    {
                        "name": "khm-gem-country-boundary.geojson",
                        "description": "Country boundary for Cambodia",
                        "format": "GeoJSON",
                    },
                    {
                        "name": "khm-gem-province-boundaries.geojson",
                        "description": "Province boundaries for Cambodia",
                        "format": "GeoJSON",
                    },
                ]

                for resource in resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)
