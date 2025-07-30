from src.exa_service import ExaService


def main():
    exa_service = ExaService()
    # webset = exa_service.create_webset(vertical="Conservation Agriculture")
    # websets = exa_service.list_websets()
    df = exa_service.websets_to_dataframe()
    print(df)


if __name__ == "__main__":
    main()
