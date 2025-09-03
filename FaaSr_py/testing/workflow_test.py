import argparse
import os


def main():
    """
    Get url, overwritten fields, and secrets from command line. Then, directly invoke entry script
    """
    parser = argparse.ArgumentParser(
        description="Set environment variables from url and payload/secrets files"
    )
    parser.add_argument("url")
    parser.add_argument("overwritten")
    parser.add_argument("secrets_file")

    args = parser.parse_args()

    with open(args.url, "r") as file:
        url = file.read().strip()

    with open(args.overwritten, "r") as file:
        overwritten = file.read().strip()

    with open(args.secrets_file, "r") as file:
        secrets = file.read().strip()

    os.environ["SECRET_PAYLOAD"] = secrets
    os.environ["OVERWRITTEN"] = overwritten
    os.environ["PAYLOAD_URL"] = url

    import workflow_test

    workflow_test.main()


if __name__ == "__main__":
    main()
