#!/usr/bin/python
"""ADPC Gender Equality Monitor scraper"""

import csv
import json
import logging
from os.path import dirname, join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._data_dir = join(dirname(__file__), "data")

        # Load country and province data
        self._countries = self._load_countries()
        self._province_mapping = self._build_province_mapping()

    def _load_countries(self) -> list:
        """Load country information from country.json

        Returns:
            List of country dictionaries with iso, name, and area_id
        """
        country_path = join(self._data_dir, "country.json")
        with open(country_path, encoding="utf-8") as f:
            data = json.load(f)

        countries = []
        for feature in data["features"]:
            props = feature["properties"]
            countries.append(
                {
                    "iso3": props["iso"],
                    "name": props["name_0"],
                    "area_id": props["area_id"],
                }
            )

        logger.info(f"Loaded {len(countries)} countries")
        return countries

    def _build_province_mapping(self) -> dict:
        """Build mapping from province area_id to country ISO and province name

        Returns:
            Dictionary mapping province area_id to dict with iso and name
        """
        province_path = join(self._data_dir, "provinces.json")
        with open(province_path, encoding="utf-8") as f:
            data = json.load(f)

        mapping = {}
        for feature in data["features"]:
            props = feature["properties"]
            mapping[props["area_id"]] = {
                "iso3": props["iso"],
                "name": props["name_1"],
            }

        logger.info(f"Built province mapping with {len(mapping)} entries")
        return mapping

    def _load_csv_data(self, filename: str) -> list:
        """Load CSV data from the data directory

        Args:
            filename: CSV filename

        Returns:
            List of row dictionaries
        """
        csv_path = join(self._data_dir, f"{filename}.csv")
        with open(csv_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _load_geojson(self, filename: str) -> dict:
        """Load GeoJSON data from the data directory

        Args:
            filename: GeoJSON filename

        Returns:
            GeoJSON dictionary
        """
        geojson_path = join(self._data_dir, f"{filename}.json")
        with open(geojson_path, encoding="utf-8") as f:
            return json.load(f)

    def _get_country_area_id(self, country_code: str) -> int | None:
        """Get area_id for a country

        Args:
            country_code: ISO3 code

        Returns:
            Country area_id or None
        """
        for country in self._countries:
            if country["iso3"] == country_code:
                return country["area_id"]
        return None

    def _get_province_area_ids(self, country_code: str) -> set[int]:
        """Get province area_ids for a country

        Args:
            country_code: ISO3 code

        Returns:
            Set of province area_ids (exclude country area_id)
        """
        area_ids = set()
        for area_id, info in self._province_mapping.items():
            if info["iso3"] == country_code:
                area_ids.add(area_id)
        return area_ids

    def _filter_csv_by_country(self, rows: list, country_code: str) -> list:
        """Filter CSV rows for a specific country

        Args:
            rows: List of CSV row dictionaries
            country_code: ISO3 code

        Returns:
            Filtered list of rows for specific country
        """
        country_area_id = self._get_country_area_id(country_code)
        province_area_ids = self._get_province_area_ids(country_code)
        filtered = []

        for row in rows:
            try:
                area_id = int(row.get("area_id", -1))
                admin_level = row.get("admin_level", "")

                # National data: match country area_id
                if admin_level == "country" and area_id == country_area_id:
                    filtered.append(row)
                # Subnational data: match province area_ids
                elif admin_level == "province" and area_id in province_area_ids:
                    filtered.append(row)
            except (ValueError, TypeError):
                continue

        return filtered

    def _filter_geojson_by_country(self, geojson: dict, country_code: str) -> dict:
        """Filter GeoJSON features for specific country

        Args:
            geojson: GeoJSON FeatureCollection
            country_code: ISO3 code

        Returns:
            Filtered GeoJSON with features for specific country
        """
        filtered_features = [
            feature
            for feature in geojson["features"]
            if feature["properties"].get("iso") == country_code
        ]

        return {
            "type": "FeatureCollection",
            "features": filtered_features,
        }

    def _transform_gii_national(self, rows: list, country_code: str) -> list:
        """Transform national GII rows

        Args:
            rows: List of GII row dictionaries
            country_code: ISO3 code

        Returns:
            Transformed list of rows sorted by year in descending order
        """
        transformed = []
        for row in rows:
            if row.get("admin_level") != "country":
                continue
            new_row = {
                "iso3": country_code,
                "country": row.get("admin_name", ""),
                "year": row.get("year", ""),
                "gender_inequality_index": row.get("gii", ""),
            }
            transformed.append(new_row)
        return sorted(transformed, key=lambda x: int(x["year"]), reverse=True)

    def _transform_gii_subnational(
        self, rows: list, country_code: str, country_name: str
    ) -> list:
        """Transform subnational GII rows

        Args:
            rows: List of GII row dictionaries
            country_code: ISO3 code to add
            country_name: Country name to add

        Returns:
            Transformed list of rows
        """
        transformed = []
        for row in rows:
            if row.get("admin_level") != "province":
                continue
            area_id = row.get("area_id", "")
            admin1_info = self._province_mapping.get(int(area_id)) if area_id else None
            province_name = admin1_info["name"] if admin1_info else ""

            new_row = {
                "iso3": country_code,
                "country": country_name,
                "province": province_name,
                "year": row.get("year", ""),
                "gender_inequality_index": row.get("gii", ""),
            }
            transformed.append(new_row)
        return sorted(
            transformed,
            key=lambda x: (-int(x["year"]), x["province"]),
        )

    def _transform_dimension_national(self, rows: list, country_code: str) -> list:
        """Transform national dimension rows

        Args:
            rows: List of dimension row dictionaries
            country_code: ISO3 code to add

        Returns:
            Transformed list of rows
        """
        transformed = []
        for row in rows:
            if row.get("admin_level") != "country":
                continue
            new_row = {
                "iso3": country_code,
                "country": row.get("admin_name", ""),
                "dimension_category": row.get("", ""),
                "dimension": row.get("dimension_name", ""),
                "gender": row.get("F/M", ""),
                "year": row.get("year", ""),
                "value": row.get("value", ""),
                "unit": row.get("Unit", ""),
            }
            transformed.append(new_row)
        return sorted(
            transformed,
            key=lambda x: (-int(x["year"]), x["dimension_category"], x["dimension"]),
        )

    def _transform_dimension_subnational(
        self, rows: list, country_code: str, country_name: str
    ) -> list:
        """Transform subnational dimension rows

        Args:
            rows: List of dimension row dictionaries
            country_code: ISO3 code to add
            country_name: Country name to add

        Returns:
            Transformed list of rows
        """
        transformed = []
        for row in rows:
            if row.get("admin_level") != "province":
                continue
            area_id = row.get("area_id", "")
            admin1_info = self._province_mapping.get(int(area_id)) if area_id else None
            province_name = admin1_info["name"] if admin1_info else ""

            new_row = {
                "iso3": country_code,
                "country": country_name,
                "province": province_name,
                "dimension_category": row.get("", ""),
                "dimension": row.get("dimension_name", ""),
                "gender": row.get("F/M", ""),
                "year": row.get("year", ""),
                "value": row.get("value", ""),
                "unit": row.get("Unit", ""),
            }
            transformed.append(new_row)
        return sorted(
            transformed,
            key=lambda x: (
                -int(x["year"]),
                x["province"],
                x["dimension_category"],
                x["dimension"],
            ),
        )

    def _transform_indicator_national(
        self, rows: list, country_code: str, country_name: str
    ) -> list:
        """Transform national indicator rows

        Args:
            rows: List of indicator row dictionaries
            country_code: ISO3 code to add
            country_name: Country name to add

        Returns:
            Transformed list of rows
        """
        transformed = []
        for row in rows:
            if row.get("admin_level") != "country":
                continue
            new_row = {
                "iso3": country_code,
                "country": country_name,
                "indicator_category": row.get("common_name", ""),
                "indicator": row.get("indicator_name", ""),
                "gender": row.get("F/M", ""),
                "year": row.get("year", ""),
                "value": row.get("value", ""),
                "unit": row.get("Unit", ""),
            }
            transformed.append(new_row)
        return sorted(
            transformed,
            key=lambda x: (-int(x["year"]), x["indicator_category"], x["indicator"]),
        )

    def _transform_indicator_subnational(
        self, rows: list, country_code: str, country_name: str
    ) -> list:
        """Transform subnational indicator rows

        Args:
            rows: List of indicator row dictionaries
            country_code: ISO3 code to add
            country_name: Country name to add

        Returns:
            Transformed list of rows
        """
        transformed = []
        for row in rows:
            if row.get("admin_level") != "province":
                continue
            area_id = row.get("area_id", "")
            admin1_info = self._province_mapping.get(int(area_id)) if area_id else None
            province_name = admin1_info["name"] if admin1_info else ""

            new_row = {
                "iso3": country_code,
                "country": country_name,
                "province": province_name,
                "indicator_category": row.get("common_name", ""),
                "indicator": row.get("indicator_name", ""),
                "gender": row.get("F/M", ""),
                "year": row.get("year", ""),
                "value": row.get("value", ""),
                "unit": row.get("Unit", ""),
            }
            transformed.append(new_row)
        return sorted(
            transformed,
            key=lambda x: (
                -int(x["year"]),
                x["province"],
                x["indicator_category"],
                x["indicator"],
            ),
        )

    def _transform_sex_disaggregated(
        self, rows: list, country_code: str, country_name: str
    ) -> list:
        """Transform sex-disaggregated rows

        Args:
            rows: List of sex-disaggregated row dictionaries
            country_code: ISO3 code to add
            country_name: Country name to add

        Returns:
            Transformed list of rows
        """
        transformed = []
        for row in rows:
            area_id = row.get("area_id", "")
            admin1_info = self._province_mapping.get(int(area_id)) if area_id else None
            province_name = admin1_info["name"] if admin1_info else ""

            new_row = {
                "iso3": country_code,
                "country": country_name,
                "province": province_name,
                "indicator": row.get("dataset_name_l1", ""),
                "sub_indicator": row.get("dataset_name_l2", ""),
                "gender": row.get("F/M", ""),
                "year": row.get("year", ""),
                "value": row.get("value", ""),
                "unit": row.get("Unit", ""),
                "calculation_type": row.get("calc", ""),
                "definition": row.get("Definition", ""),
            }
            transformed.append(new_row)
        return sorted(
            transformed,
            key=lambda x: (-int(x["year"]), x["province"], x["indicator"]),
        )

    def _get_years_from_rows(self, rows: list) -> tuple:
        """Extract min and max years from CSV rows

        Args:
            rows: List of CSV row dictionaries

        Returns:
            Tuple of (min_year, max_year)
        """
        years = set()
        for row in rows:
            year = row.get("year")
            if year:
                try:
                    years.add(int(year))
                except (ValueError, TypeError):
                    continue

        if years:
            return min(years), max(years)
        return 2000, 2024  # Default fallback

    def get_country_data(self) -> list[dict]:
        """Get filtered data for each country, split by admin level

        Returns:
            List of dictionaries containing country info and filtered data
        """
        # Load CSV data
        gii_data = self._load_csv_data("GEM-GII")
        dimension_data = self._load_csv_data("GEM-GII_dimension")
        indicator_data = self._load_csv_data("GEM-GII_indicator")
        sex_disagg_data = self._load_csv_data("GEM-Sex-disaggregated")

        # Load GeoJSON data
        country_geojson = self._load_geojson("country")
        province_geojson = self._load_geojson("provinces")

        results = []
        for country in self._countries:
            country_code = country["iso3"]
            country_name = country["name"]

            logger.info(f"Processing {country_name} ({country_code})")

            # Filter by country
            gii_filtered = self._filter_csv_by_country(gii_data, country_code)
            dimension_filtered = self._filter_csv_by_country(
                dimension_data, country_code
            )
            indicator_filtered = self._filter_csv_by_country(
                indicator_data, country_code
            )
            sex_disagg_filtered = self._filter_csv_by_country(
                sex_disagg_data, country_code
            )

            # Transform and split by admin level
            csv_data = {
                "gii-national": self._transform_gii_national(
                    gii_filtered, country_code
                ),
                "gii-subnational": self._transform_gii_subnational(
                    gii_filtered, country_code, country_name
                ),
                "dimension-national": self._transform_dimension_national(
                    dimension_filtered, country_code
                ),
                "dimension-subnational": self._transform_dimension_subnational(
                    dimension_filtered, country_code, country_name
                ),
                "indicator-national": self._transform_indicator_national(
                    indicator_filtered, country_code, country_name
                ),
                "indicator-subnational": self._transform_indicator_subnational(
                    indicator_filtered, country_code, country_name
                ),
                "sex-disaggregated": self._transform_sex_disaggregated(
                    sex_disagg_filtered, country_code, country_name
                ),
            }

            # Filter GeoJSON by country
            geojson_data = {
                "country-boundary": self._filter_geojson_by_country(
                    country_geojson, country_code
                ),
                "province-boundaries": self._filter_geojson_by_country(
                    province_geojson, country_code
                ),
            }

            # Calculate time period from all rows
            all_rows = []
            for rows in csv_data.values():
                all_rows.extend(rows)
            min_year, max_year = self._get_years_from_rows(all_rows)

            results.append(
                {
                    "iso3": country_code,
                    "name": country_name,
                    "csv_data": csv_data,
                    "geojson_data": geojson_data,
                    "min_year": min_year,
                    "max_year": max_year,
                }
            )

        return results

    def _add_csv_resource(
        self,
        dataset: Dataset,
        rows: list,
        resource_name: str,
        description: str,
    ) -> None:
        """Add a CSV resource to the dataset"""
        if not rows:
            logger.warning(f"No data for resource {resource_name}, skipping")
            return

        headers = list(rows[0].keys())
        resource_data = {
            "name": resource_name,
            "description": description,
        }

        dataset.generate_resource(
            folder=self._temp_dir,
            filename=resource_name,
            rows=rows,
            headers=headers,
            resourcedata=resource_data,
        )

    def _add_geojson_resource(
        self,
        dataset: Dataset,
        geojson: dict,
        resource_name: str,
        description: str,
    ) -> None:
        """Add a GeoJSON resource to the dataset"""
        if not geojson.get("features"):
            logger.warning(f"No features for resource {resource_name}, skipping")
            return

        # Write GeoJSON to temp file
        geojson_path = join(self._temp_dir, resource_name)
        with open(geojson_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f)

        resource = Resource(
            {
                "name": resource_name,
                "description": description,
                "format": "GeoJSON",
            }
        )
        resource.set_file_to_upload(geojson_path)
        dataset.add_update_resource(resource)

    def generate_dataset(self, country_data: dict) -> Dataset | None:
        """Create HDX dataset for each country"""
        country_code = country_data["iso3"]
        country_name = country_data["name"]
        iso_lower = country_code.lower()

        dataset_name = f"{iso_lower}-adpc-gem"
        dataset_title = f"{country_name} - Gender Equality Monitor"

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period_year_range(
            country_data["min_year"],
            country_data["max_year"],
        )
        dataset.add_tags(self._configuration["tags"])
        dataset.set_subnational(True)

        try:
            dataset.add_country_location(country_code)
        except HDXError:
            logger.error(f"Couldn't find country {country_code}, skipping")
            return None

        # Add CSV resources
        csv_descriptions = {
            "gii-national": f"National Gender Inequality Index scores for {country_name}",
            "gii-subnational": f"Subnational Gender Inequality Index scores for {country_name}",
            "dimension-national": f"National Gender Inequality Index by dimension for {country_name}",
            "dimension-subnational": f"Subnational Gender Inequality Index by dimension for {country_name}",
            "indicator-national": f"National Gender Inequality Index by indicator for {country_name}",
            "indicator-subnational": f"Subnational Gender Inequality Index by indicator for {country_name}",
            "sex-disaggregated": f"Sex-disaggregated data for {country_name}",
        }

        for suffix, rows in country_data["csv_data"].items():
            resource_name = f"{iso_lower}-gem-{suffix}.csv"
            description = csv_descriptions.get(suffix, "")
            self._add_csv_resource(dataset, rows, resource_name, description)

        # Add GeoJSON resources
        geojson_descriptions = {
            "country-boundary": f"Country boundary for {country_name}",
            "province-boundaries": f"Province boundaries for {country_name}",
        }

        for suffix, geojson in country_data["geojson_data"].items():
            resource_name = f"{iso_lower}-gem-{suffix}.geojson"
            description = geojson_descriptions.get(suffix, "")
            self._add_geojson_resource(dataset, geojson, resource_name, description)

        return dataset
