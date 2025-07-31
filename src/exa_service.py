import os
from typing import Any

import email_validator
import pandas as pd
from dotenv import load_dotenv
from exa_py import Exa
from exa_py.websets.types import (
    CreateCriterionParameters,
    CreateEnrichmentParameters,
    CreateWebsetParameters,
    CreateWebsetParametersSearch,
    Format,
    GetWebsetResponse,
    ListWebsetsResponse,
    UpdateWebsetRequest,
    WebsetItemCompanyProperties,
)

from src.logger import setup_logger

load_dotenv()

logger = setup_logger("src.exa_service")


class ExaService:
    LIMIT = 25
    API_KEY = os.getenv("EXA_API_KEY", None)
    print(API_KEY)
    DEFAULT_DATAFRAME_FOLDER = "data"
    CLEAN_DF_NAME_PATTERN = "clean_df_part"

    def __init__(self):
        self.exa = Exa(api_key=self.API_KEY)
        logger.debug("ExaService initialized")

    def __create_search_query(self, vertical: str) -> CreateWebsetParametersSearch:
        query = (
            f"Find US-based companies that work in the vertical of {vertical}"
            + " with the number of money they raised"
            + " and the contact information for their CEO."
        )

        criteria = [
            CreateCriterionParameters(
                description="company is headquartered in the united states",
            ),
            CreateCriterionParameters(
                description=f"company operates in the {vertical} industry",
            ),
        ]

        return CreateWebsetParametersSearch(
            query=query,
            criteria=criteria,
            count=self.LIMIT,
        )

    def __create_standard_enrichment(self) -> list[CreateEnrichmentParameters]:
        return [
            CreateEnrichmentParameters(
                description="CEO Email",
                format=Format.text,
            ),
            CreateEnrichmentParameters(
                description="Money Raised",
                format=Format.number,
            ),
        ]

    def create_webset(
        self,
        vertical: str,
        enrichment: list[CreateEnrichmentParameters] | None = None,
    ):
        query = self.__create_search_query(vertical)

        if enrichment is None:
            logger.debug("No enrichment provided, using standard enrichment")
            enrichment = self.__create_standard_enrichment()

        else:
            logger.debug("Criteria provided, using custom enrichment, validating...")
            assert isinstance(enrichment, list), "Criteria must be a list"
            assert all(
                isinstance(item, CreateEnrichmentParameters) for item in enrichment
            ), "Criteria must be a list of CreateEnrichmentParameters"
            logger.debug("Validation checks passed")

        params = CreateWebsetParameters(
            search=query,
            enrichments=enrichment,
        )

        logger.debug(f"Creating webset with params: {params}")

        webset = self.exa.websets.create(params=params)
        webset_id = webset.id
        self.exa.websets.update(
            id=webset_id,
            params=UpdateWebsetRequest(
                metadata={"vertical": vertical},
            ),
        )

        logger.debug(f"Webset created: {webset}")

        return webset

    def list_websets(
        self, cursor: str | None = None, limit: int | None = None
    ) -> ListWebsetsResponse:
        return self.exa.websets.list(cursor=cursor)

    def get_webset(self, id: str) -> GetWebsetResponse:
        return self.exa.websets.get(id=id, expand=["items"])

    def __validate_email(self, email: str) -> str | None:
        try:
            email_validator.validate_email(email)
            return email
        except email_validator.EmailNotValidError:
            return None
        except Exception as e:
            logger.error(f"Error validating email: {e}")
            return None

    def webset_to_dataframe(self, webset_id: str, save: bool = True) -> pd.DataFrame:
        if not os.path.exists(self.DEFAULT_DATAFRAME_FOLDER):
            os.makedirs(self.DEFAULT_DATAFRAME_FOLDER)

        webset = self.get_webset(id=webset_id)
        vertical_title = webset.title  # type: ignore | for some reason schema doesn't show it lol
        items_for_dataframe: list[dict[str, Any]] = []

        assert webset.items is not None, "Webset items are not None"
        logger.debug(f"Webset {webset_id} has {len(webset.items)} items")

        for item in webset.items:
            # dynamic fields, so ignore linter
            properties = item.properties
            assert isinstance(properties, WebsetItemCompanyProperties), (
                "Properties must be a company"
            )
            company_properties = properties.company

            name = company_properties.name  # type: ignore
            location = company_properties.location  # type: ignore
            employees = company_properties.employees  # type: ignore
            industry = company_properties.industry  # type: ignore

            url = properties.url  # type: ignore
            description = properties.description  # type: ignore

            assert len(item.enrichments) == 2, "Expected 2 enrichments"

            if item.enrichments[0].result:
                email = item.enrichments[0].result[0]  # type: ignore
            else:
                email = None

            email_reasoning = item.enrichments[0].reasoning  # type: ignore

            if item.enrichments[1].result:
                financials = item.enrichments[1].result[0]  # type: ignore
            else:
                financials = None

            financials_reasoning = item.enrichments[1].reasoning  # type: ignore

            item_to_add: dict[str, Any] = {
                "Company Name": name,
                "Vertical": vertical_title,
                "Money Raised": financials,
                "CEO Email": email,
                "Location": location,
                "Employees": employees,
                "Industry": industry,
                "URL": url,
                "Description": description,
                "Email Reasoning": email_reasoning,
                "Financials Reasoning": financials_reasoning,
            }

            used_email = item_to_add.get("CEO Email", None)

            if used_email:
                validated_email = self.__validate_email(used_email)
                if validated_email:
                    item_to_add["CEO Email"] = validated_email
                else:
                    item_to_add["CEO Email"] = None

            items_for_dataframe.append(item_to_add)

        df = pd.DataFrame(items_for_dataframe)

        if save:
            logger.debug(
                f"Saving dataframe to {self.DEFAULT_DATAFRAME_FOLDER}/{vertical_title}.csv"
            )
            df.to_csv(
                f"{self.DEFAULT_DATAFRAME_FOLDER}/{vertical_title}.csv", index=False
            )

        return df

    def websets_to_dataframe(self, save: bool = True) -> pd.DataFrame:
        websets = self.list_websets()
        dfs: list[pd.DataFrame] = []
        logger.debug(f"Found {len(websets.data)} websets")

        for id, webset in enumerate(websets.data):
            webset_id = webset.id
            logger.debug(
                f"Processing webset: {webset_id} ({id + 1} of {len(websets.data)})"
            )

            df = self.webset_to_dataframe(webset_id, save=False)
            dfs.append(df)

        combined_df = pd.concat(dfs)
        combined_df = combined_df.drop_duplicates(subset=["Company Name"])
        combined_df = combined_df.sort_values(by="Vertical", ascending=True)  # type: ignore

        if save:
            logger.debug(
                f"Saving combined dataframe to {self.DEFAULT_DATAFRAME_FOLDER}/combined_df.csv"
            )
            combined_df.to_csv(
                f"{self.DEFAULT_DATAFRAME_FOLDER}/combined_df.csv", index=False
            )

        return combined_df

    def combine_saved_df(self, save: bool = True) -> pd.DataFrame:
        df_files = os.listdir(self.DEFAULT_DATAFRAME_FOLDER)
        df_files = [
            file for file in df_files if file.startswith(self.CLEAN_DF_NAME_PATTERN)
        ]
        dfs: list[pd.DataFrame] = []
        for file in df_files:
            df = pd.read_csv(f"{self.DEFAULT_DATAFRAME_FOLDER}/{file}")  # type: ignore
            dfs.append(df)
        combined_df = pd.concat(dfs)
        combined_df = combined_df.drop_duplicates(subset=["Company Name"])
        combined_df = combined_df.sort_values(by="Vertical", ascending=True)  # type: ignore

        if save:
            logger.debug(
                f"Saving combined dataframe to {self.DEFAULT_DATAFRAME_FOLDER}/combined_df.csv"
            )
            combined_df.to_csv(
                f"{self.DEFAULT_DATAFRAME_FOLDER}/total_combined_df.csv", index=False
            )
        return combined_df
