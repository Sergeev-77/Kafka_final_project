import argparse
from ksql import search
from prod import send_event_to_topic
from cons import stream_recommendations


def main():
    parser = argparse.ArgumentParser(
        prog="app.py", description="CLI приложение для рекомендаций и поиска"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        title="commands",
        description="Доступные команды",
    )

    # recommendations
    subparsers.add_parser("recommendations", help="Показать рекомендации")

    # search
    search_parser = subparsers.add_parser(
        "search",
        help="Поиск по товарам",
        description="Поиск по имени ИЛИ бренду",
    )

    group = search_parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--name", metavar="NAME", help="Поиск по названию товара"
    )
    group.add_argument("--brand", metavar="BRAND", help="Поиск по бренду")

    args = parser.parse_args()

    if args.command == "recommendations":
        send_event_to_topic(command="recommendations")
        stream_recommendations()

    elif args.command == "search":
        if args.name:
            send_event_to_topic(command="search", type="name", value=args.name)
            print(f"Поиск по имени:  {args.name}")
            search(name=args.name)

        if args.brand:
            send_event_to_topic(
                command="search", type="brand", value=args.brand
            )
            print(f"Поиск по бренду: {args.brand}")
            search(brand=args.brand)


if __name__ == "__main__":
    main()
