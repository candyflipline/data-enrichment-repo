from src.exa_service import ExaService


def main():
    exa_service = ExaService()
    # webset = exa_service.create_webset(vertical="Water Distribution Efficiency")
    # websets = exa_service.list_websets()
    # for webset in websets.data:
    #     print(webset.id)
    #     print(webset.title)
    #     print("--------------------------------")
    # df = exa_service.websets_to_dataframe(save=True)
    # print(df)

    df = exa_service.combine_saved_df(save=True)
    print(df)


if __name__ == "__main__":
    main()
