import hashlib
import os
import configparser
import argparse
from abc import ABC, abstractmethod
import zlib
import sys


GITDIR = ".pygit"


# ------------------------------- INIT AND REPO -------------------------------


def init(args: argparse.Namespace):
    repo_create(args.path)


class GitRepository(object):
    """A class that represents a git repository."""

    worktree = None
    gitdir = None
    config = None

    def __init__(self, path: str, force: bool = False):
        self.worktree = path
        self.gitdir = os.path.join(path, GITDIR)

        if not (force or os.path.isdir(self.gitdir)):
            raise Exception(f"Not a Git repository: {path}")

        # Read config file in .git/config
        self.config = configparser.ConfigParser()
        config_file = repo_file(self, "config")

        if config_file and os.path.exists(config_file):
            self.config.read([config_file])
        elif not force:
            raise Exception("Error creating repository: configuration file missing")

        # Make sure repositoryformatversion is 0
        if not force:
            version = int(self.config.get("core", "repositoryformatversion"))
            if version != 0:
                raise Exception(
                    f"Error creating repository: unsupported repositoryformatversion {version}"
                )


def repo_create(path: str):
    """Create a new repository at path."""

    repo = GitRepository(path, True)

    # Make sure the path either doesn't exist or is an empty dir
    if os.path.exists(repo.worktree):
        if not os.path.isdir(repo.worktree):
            raise Exception(f"Failed to create repository: {path} is not a directory")
        if os.path.exists(repo.gitdir) and os.listdir(repo.gitdir):
            raise Exception(
                f"Failed to create repository: a pygit repository already exists in {path}"
            )
    else:
        os.makedirs(repo.worktree)

    assert repo_dir(repo, "branches", mkdir=True)
    assert repo_dir(repo, "objects", mkdir=True)  # Object store
    assert repo_dir(repo, "refs", "tags", mkdir=True)  # Reference store
    assert repo_dir(repo, "refs", "heads", mkdir=True)

    # Create .git/description
    with open(repo_file(repo, "description"), "w") as f:
        f.write(
            "Unnamed repository; edit this file 'description' to name the repository.\n"
        )

    # Create .git/HEAD
    with open(repo_file(repo, "HEAD"), "w") as f:
        f.write("ref: refs/heads/main\n")

    # Create .git/config
    with open(repo_file(repo, "config"), "w") as f:
        config: configparser.ConfigParser = repo_default_config()
        config.write(f)

    return repo


def repo_default_config() -> configparser.ConfigParser:
    config_parser = configparser.ConfigParser()
    config_parser.add_section("core")

    # Set repositoryformatversion to 0, which indicates initial format
    # Only supports 0 for now
    config_parser.set("core", "repositoryformatversion", "0")

    # Set filemode to false: disable tracking of file mode changes in the worktree
    config_parser.set("core", "filemode", "false")

    # Set bare to false: this repo has a worktree
    config_parser.set("core", "bare", "false")

    return config_parser


def repo_path(repo, *path: str) -> str:
    """Compute path under repo's gitdir."""
    return os.path.join(repo.gitdir, *path)


def repo_file(repo, *path: str, mkdir=False) -> str | None:
    """Same as repo_path but creates dirname(*path) if absent."""
    if repo_dir(repo, *path[:-1], mkdir=mkdir):
        return repo_path(repo, *path)
    else:
        return None


def repo_dir(repo, *path: str, mkdir=False) -> str | None:
    """Same as repo_path but mkdir *path if absent and mkdir == True."""

    path = repo_path(repo, *path)

    if os.path.exists(path):
        if os.path.isdir(path):
            return path
        else:
            raise Exception(f"Not a directory: {path}")

    if mkdir:
        os.makedirs(path)
        return path
    else:
        return None


def repo_find(path: str = ".", required: bool = True) -> GitRepository | None:
    """Find the git repository the current directory is in."""

    path = os.path.realpath(path)
    if os.path.isdir(os.path.join(path, GITDIR)):
        return GitRepository(path)

    # If we haven't found a git repo in the current directory, check parent
    parent = os.path.realpath(os.path.join(path, ".."))

    # If we're at the root, return None, or raise an exception if required
    if parent == path:
        if required:
            raise Exception("No repository found.")
        else:
            return None

    return repo_find(parent, required)


# ------------------------------- OBJECTS -------------------------------

class GitObject(ABC):
    """Abstract base class for git objects.
    Object format: <type> space <size in ascii> \x00 <data>"""

    def __init__(self, data=None) -> None:
        if data != None:
            self.deserialize(data)
        else:
            self.init()

    def init(self):
        self.type = None

    @abstractmethod
    def serialize(self) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes):
        pass
    
    
class GitBlob(GitObject):
    fmt = b"blob"
    
    def serialize(self) -> bytes:
        return self.blob_data
    
    def deserialize(self, data: bytes):
        self.blob_data = data
        

def read_object(repo: GitRepository, hash: str) -> GitObject | None:
    """Reads object with sha-1 hash from git repository repo.
    Returns a GitObject constructed from the object data."""

    path = repo_file(repo, "objects", hash[:2], hash[2:])

    if not os.path.isfile(path):
        return None

    with open(path, "rb") as f:
        raw = zlib.decompress(f.read())

        # Read object type
        x = raw.find(b" ")
        fmt = raw[0:x]

        # Read object size
        y = raw.find(b"\x00", x)
        size = int(raw[x:y].decode("ascii"))
        if size != len(raw) - y - 1:
            raise Exception("Malformed object {hash}: bad length")

        # Choose constructor based on object type
        match fmt:
            # TODO: implement other object types
            # case b"commit":
            #     c = GitCommit
            case b"tree":
                c = GitTree
            # case b"tag":
            #     c = GitTag
            case b"blob":
                c = GitBlob
            case _:
                raise Exception(f"Unknown type {fmt.decode("ascii")} for object {hash}")

        return c(data=raw[y+1:])


def write_object(obj: GitObject, repo: GitRepository = None) -> str:
    """Writes the data to the repository repo. Returns the hash of the object."""

    data = obj.serialize()

    # Add type header
    data_with_header = obj.fmt + b" " + str(len(data)).encode("ascii") + b"\x00" + data

    # Compute hash
    sha1 = hashlib.sha1(data_with_header).hexdigest()

    if repo:
        path = repo_file(repo, "objects", sha1[:2], sha1[2:], mkdir=True)
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(zlib.compress(data_with_header))

    return sha1


def find_object(repo: GitRepository, name: str, fmt=None, follow=True) -> str:
    """Finds an object specified by name, which can be the full hash, short hash, tags, etc."""

    return name


def hash_object(data: bytes, fmt: bytes, write: bool) -> str:
    """Hashes the data and stores it in the objects directory in the git directory.
    Returns the object id (SHA-1 hash)."""

    if write:
        repo = repo_find()
    else:
        repo = None
    
    match fmt:
        # case b"commit":
        #     obj = GitCommit(data)
        case b"tree":
            obj = GitTree(data)
        # case b"tag":
        #     obj = GitTag(data)
        case b"blob":
            obj = GitBlob(data)
        case _:
            raise Exception(f"Unknown type {fmt.decode("ascii")}")
        
    return write_object(obj, repo)


def cat_file(obj: str, fmt: str) -> bytes:
    repo = repo_find()
    object = read_object(repo, find_object(repo, obj, fmt=fmt))
    sys.stdout.buffer.write(object.serialize())


# ------------------------------- TREE -------------------------------

class GitTreeRecord(object):
    """Represents a single record in a GitTree.
    Format of tree record: <filemode> space <path> space <sha-1>"""

    def __init__(self, fmt: str, path: str, sha: str) -> None:
        self.fmt = fmt
        self.path = path
        self.sha = sha


class GitTree(GitObject):
    """Represents a Git tree object."""

    fmt = b"tree"

    def init(self):
        self.records = list()

    def serialize(self) -> bytes:
        return tree_serialize(self)

    def deserialize(self, data: bytes):
        self.records = parse_tree(data)


def parse_record(raw: bytes, start: int = 0) -> tuple[int, GitTreeRecord]:
    """Parses a single Git record."""

    # Read format
    x = raw.find(b" ", start)
    fmt = raw[start:x].decode("utf-8")

    # Read path
    y = raw.find(b" ", x + 1)
    path = raw[x + 1 : y].decode("utf-8")

    # Read sha and convert to hex string
    sha = format(int.from_bytes(raw[y + 1 : y + 21], "big"), "040x")

    # Return end index of record + 1 (y+21) for next iteration when parsing a tree
    return y + 21, GitTreeRecord(fmt, path, sha)


def parse_tree(raw: bytes) -> list[GitTreeRecord]:
    """Parses a Git tree."""

    records = []
    start = 0
    while start < len(raw):
        start, record = parse_record(raw, start)
        records.append(record)

    return records


def tree_record_sort_key(record: GitTreeRecord) -> str:
    """Returns path of record as key for sorting a Git tree."""

    if record.fmt == "tree":
        # Directory, need to append "/"
        return record.path + "/"
    else:
        # Normal file
        return record.path


def tree_serialize(tree: GitTree) -> bytes:
    """Serializes a Git tree."""

    res = b""
    tree.records.sort(key=tree_record_sort_key)
    for record in tree.records:
        res += (
            record.fmt.encode("utf-8")
            + b" "
            + record.path.encode("utf-8")
            + b" "
            + bytes.fromhex(record.sha)
        )

    return res


def write_tree(directory: str = ".") -> str:
    """Writes a tree object from the given directory and returns the hash of the tree object."""

    with os.scandir(directory) as it:
        tree = GitTree()
        for entry in it:
            full = os.path.join(directory, entry.name)
            if full == None or is_ignored(full):
                continue

            if entry.is_file(follow_symlinks=False):
                with open(full, "rb") as f:
                    raw = f.read()
                    fmt = "blob"
                    oid = hash_object(raw, b"blob", True)
            elif entry.is_dir(follow_symlinks=False):
                fmt = "tree"
                oid = write_tree(full)
            tree.records.append(GitTreeRecord(fmt, entry.name, oid))
        
    return hash_object(tree.serialize(), b"tree", True)


def is_ignored(path: str) -> bool:
    """Returns True if the path is ignored, False otherwise."""

    dirs = path.split("/")
    return ".pygit" in dirs or ".git" in dirs or "__pycache__" in dirs


def read_tree(oid: str):
    """Reads a tree object with the given oid."""

    repo = repo_find()
    for path, oid in get_tree_paths(repo, oid).items():
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(read_object(repo, oid).serialize())
        
        
def get_tree_paths(repo: GitRepository, oid: str, base_path = "./") -> dict[str, str]:
    res = {}
    tree: GitTree = read_object(repo, find_object(repo, oid))
    for record in tree.records:
        assert '/' not in record.path
        assert record.path not in ('..', '.')
        path = base_path + record.path
        if record.fmt == "blob":
            res[path] = record.sha
        elif record.fmt == "tree":
            res.update(get_tree_paths(repo, record.sha, f"{path}/"))
        else:
            raise Exception(f"Unknown tree entry {record.fmt}")
    return res
