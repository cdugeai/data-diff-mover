import requests
from polars import DataFrame, concat, Schema
from typing import Tuple, Dict, List, Any
import os

from requests import Response

schema_dossiers: Schema = Schema(
    {
        "dossier_number": int,
        "dossier_url": str,
        "state": str,
        "dateDepot": str,  # datetime ??
        "id": str,
    }
)


def exec_query(
    query_str: str, query_variables: Dict[str, Any], ds_api_token: str, ds_api_url: str
) -> Response:
    return requests.post(
        url=ds_api_url,
        json={"query": query_str, "variables": query_variables},
        headers={"Authorization": "Bearer " + ds_api_token},
    )


def build_dossier_url(demarche_id: int, dossier_number: int) -> str:
    procedures_base_url: str = "https://www.demarches-simplifiees.fr/procedures"
    return "/".join(
        [procedures_base_url, str(demarche_id), "dossiers", str(dossier_number)]
    )


def get_dossiers_partiel(
    demarche_id: int, start_cursor: str, ds_api_token: str, ds_api_url: str
) -> Tuple[DataFrame, bool, str]:
    """
    Returns 100 first Dossier starting from start_cursor
    :param demarche_id:
    :param start_cursor:
    :param ds_api_token:
    :param ds_api_url:
    :return:
    """
    LIMIT_DOSSIERS: int = 100

    response_api_raw: Response = exec_query(
        load_query("getDossiersAcceptes.graphql"),
        {"demarcheNumber": demarche_id, "after": start_cursor, "limit": LIMIT_DOSSIERS},
        ds_api_token,
        ds_api_url,
    )
    response_api: Any = response_api_raw.json()

    try:
        dossiers_root: Dict[str, Any] = response_api["data"]["demarche"]["dossiers"]
    except KeyError:
        raise RuntimeError(
            "Error with Démarches Simplifiées API: " + response_api_raw.text
        )

    dossiers_clean: List[Dict[str, Any]] = [
        {
            "dossier_number": x["number"],
            "dossier_url": build_dossier_url(demarche_id, x["number"]),
            "state": x["state"],
            "dateDepot": x["dateDepot"],
            "id": x["id"],
        }
        for x in dossiers_root["nodes"]
    ]

    dossiers_as_df: DataFrame = DataFrame(dossiers_clean, schema=schema_dossiers)
    end_cursor: str = dossiers_root["pageInfo"]["endCursor"]
    has_next_page: bool = dossiers_root["pageInfo"]["hasNextPage"]

    return dossiers_as_df, has_next_page, end_cursor


def load_query(query_file: str) -> str:
    """
    Load query from a file to string, ready for use with the API
    :param query_file: File to load content from
    :return: Content of the file as string
    """
    filepath = os.path.join(os.path.dirname(__file__), "ds-queries", query_file)
    with open(filepath, "r") as f:
        return f.read()


def get_dossiers(demarche_id: int, ds_api_token: str, ds_api_url: str) -> DataFrame:
    """
    Returns all Dossiers for a given démarche
    :param demarche_id:
    :param ds_api_token:
    :param ds_api_url:
    :return: Dataframe with schema schema_dossiers
    """
    dossiers_df, has_next_page, end_cursor = DataFrame(schema=schema_dossiers), True, ""

    while has_next_page:
        dossiers_df_courant, has_next_page, end_cursor = get_dossiers_partiel(
            demarche_id, end_cursor, ds_api_token, ds_api_url
        )
        dossiers_df = concat([dossiers_df, dossiers_df_courant])

    return dossiers_df
