from src.Input import Input
from src.helpers.ds import get_dossiers


class DemarchesSimplifiees(Input):
    """
    Retourne la liste des dossiers associés à une démarche. Les éléments suivants sont retournées:

    - `dossier_number`: Numéro du dossier
    - `dossier_url`: URL du dossier
    - `state`: Etat du dossier (en instruction, sans suite, accepté, ...)
    - `dateDepot`: Date de dépot du dossier
    - `id`: Identifiant unique du dossier
    """

    demarche_id: int
    ds_api_token: str
    api_base_url: str = "https://www.demarches-simplifiees.fr/api/v2/graphql"

    def __init__(
        self,
        name: str,
        demarche_id: int,
        ds_api_token: str,
    ) -> None:
        super().__init__(
            name=name,
            primary_key="dossier_number",
            use_row_index_as_primary_key=False,
        )
        self.demarche_id = int(demarche_id)
        self.ds_api_token = ds_api_token

    def describe(self) -> str:  # pragma: no cover
        return f"{self.base_string()} : Démarche -> {self.demarche_id}"

    def load(self) -> None:
        self.data = get_dossiers(self.demarche_id, self.ds_api_token, self.api_base_url)
        self.has_load = True
