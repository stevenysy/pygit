import argparse
import sys
import os
import data


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest="command", required=True)


def main(argv=sys.argv[1:]):
    args = parser.parse_args(argv)
    match args.command:
        case "init":
            cmd_init(args)
        case "hash-object":
            cmd_hash_object(args)
        case "cat-file":
            cmd_cat_file(args)
        case "write-tree":
            cmd_write_tree(args)
        case _:
            parser.print_help()
            sys.exit(1)


# ------------------------------- INIT -------------------------------

init_parser = commands.add_parser("init", help="Initialize a new repository.")
init_parser.add_argument(
    "path", nargs="?", default=".", help="Where to create the repository."
)


def cmd_init(args: argparse.Namespace):
    data.init(args)
    print(f"Initialized empty repository in {os.getcwd()}/{data.GITDIR}")


# ------------------------------- HASH-OBJECT -------------------------------

hash_object_parser = commands.add_parser(
    "hash-object",
    help="Hashes the data and stores it in the objects directory in the git directory.",
)
hash_object_parser.add_argument("file", help="File to hash.")
hash_object_parser.add_argument(
    "-t",
    metavar="type",
    dest="type",
    choices=["blob", "commit", "tag", "tree"],
    default="blob",
    help="Specify the type of the object",
)
hash_object_parser.add_argument(
    "-w",
    dest="write",
    action="store_true",
    help="Actually write the object into the database",
)


def cmd_hash_object(args: argparse.Namespace):
    with open(args.file, "rb") as f:
        print(data.hash_object(f.read(), args.type.encode("ascii"), args.write))


# ------------------------------- CAT-FILE -----------------------------------

cat_file_parser = commands.add_parser(
    "cat-file",
    help="Provide content of repository objects.",
)
cat_file_parser.add_argument(
    "type",
    choices=["blob", "commit", "tag", "tree"],
    help="Specify the type of the object",
)
cat_file_parser.add_argument("object", help="Object to display.")


def cmd_cat_file(args: argparse.Namespace):
    data.cat_file(args.object, args.type)


# ------------------------------- WRITE-TREE -----------------------------------

write_tree_parser = commands.add_parser(
    "write-tree", help="Store a directory in the object database."
)


def cmd_write_tree(_: argparse.Namespace):
    print(data.write_tree())
