from src.helpers.ds import build_dossier_url


def test_build_dossier_url() -> None:
    assert (
        build_dossier_url(123, 456)
        == "https://www.demarches-simplifiees.fr/procedures/123/dossiers/456"
    )
