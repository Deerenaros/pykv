import base64
import json
import logging
import random
import string

from pathlib import Path
from abc import abstractmethod, abstractproperty


class DBLock:
    def __init__(self, path: Path):
        self.path = path
        self.fo = None

    def __enter__(self):
        self.fo = self.path.open("x")

    def __exit__(self, tp, val, tb):
        self.fo.close()
        self.path.unlink()


class DBInterface:
    class Error(RuntimeError):
        def __str__(self):
            msg = super().__str__()
            if msg.strip():
                return f"{type(self).__name__}: {msg}"
            return f"{type(self).__name__}"

    class LoadError(Error):
        pass

    class NotFound(Error):
        pass

    class CorruptedDatabase(Error):
        pass

    class PasskeyIsWrongOrEmpty(Error):
        pass

    def get_root(self):
        count, key, curr = 0, [], self
        while isinstance(curr, Transaction):
            count += 1
            logging.debug("fetching: %s %s %s", count, key, curr)
            key.append(curr.partkey)
            curr = curr.parent
        return count, curr, "".join(reversed(key))

    def login(self, passkey):
        if self.passkey != passkey:
            raise self.PasskeyIsWrongOrEmpty()
        return True

    @abstractproperty
    def partkey(self):
        pass

    @property
    def passkey(self):
        _, _, p = self.get_root()
        return p

    @property
    def count(self):
        c, _, _ = self.get_root()
        return c

    @property
    def root(self):
        _, r, _ = self.get_root()
        return r

    @abstractmethod
    def get(self, key: str):
        pass

    @abstractmethod
    def set(self, key: str, value):
        pass

    @abstractmethod
    def select_keys(self, value) -> list:
        pass

    @abstractmethod
    def remove(self, key: str):
        pass

    @abstractmethod
    def begin(self) -> "DBInterface":
        pass

    @abstractmethod
    def commit(self) -> "DBInterface":
        pass

    @abstractmethod
    def rollback(self) -> "DBInterface":
        pass


class Transaction(DBInterface):
    partkey = None

    def __init__(self, parent: DBInterface):
        self.partkey = "".join(random.choice(string.ascii_letters) for _ in range(0, 16))
        self.parent = parent
        self.queue = dict()
        self.journal_path = self.root.path / f".{self.count}.journal"
        self.journal = self.journal_path.open("xb")

    @classmethod
    def restore(cls, path: Path):
        data = path.read_bytes()
        result = dict()

        logging.debug("Restoring from %s", repr(data))
        while data:
            try:
                val = None
                if data[0]:
                    lenght = int.from_bytes(data[1:5])
                    val = data[5:5+lenght].decode()
                    data = data[5+lenght:]
                else:
                    data = data[1:]
                lenght = int.from_bytes(data[0:4])
                key = data[4:4+lenght].decode()
                result[key] = val
                data = data[4+lenght:]
            except IndexError:
                break

        return result

    def get(self, key: str):
        return self.root.get(key)
    
    def select_keys(self, value) -> list:
        return self.root.select_keys(value)

    def set(self, key: str, value):
        try:
            val = self.get(key)
            self.root.invalids[key] = val
            self.journal.write(b"\x01")
            val = json.dumps(val)
            self.journal.write(len(val.encode()).to_bytes(4))
            self.journal.write(val.encode())
        except self.NotFound:
            self.root.invalids[key] = None
            self.journal.write(b"\x00")
        finally:
            logging.debug("Current invalids: %s", self.root.invalids)
            self.journal.write(len(key.encode()).to_bytes(4))
            self.journal.write(key.encode())
            self.root.set(key, value)

    def remove(self, key: str):
        self.root.remove(key)

    def begin(self) -> "DBInterface":
        return Transaction(self)

    def commit(self) -> "DBInterface":
        with DBLock(self.root.path.parent.with_name(f".{self.root.path.parent.name}")):
            self.journal.close()
            restore = self.restore(self.journal_path)
            logging.debug("Restored: %s", restore)
            for k in restore.keys():
                del self.root.invalids[k]

            self.journal_path.unlink()

        return self.parent

    def rollback(self) -> "DBInterface":
        with DBLock(self.root.path.parent.with_name(f".{self.root.path.parent.name}")):
            self.journal.close()
            restore = self.restore(self.journal_path)

            for k, v in restore.items():
                self.root.set(k, v)

            self.journal_path.unlink()

        return self.parent


class DB(DBInterface):
    partkey = ""

    def create(self):
        if not self.path.exists():
            self.path.mkdir()
            with (self.path / ".version").open("w") as init:
                init.write("pykv/v1")

        if (self.path / ".version").is_file():
            with (self.path / ".version").open() as version:
                if version.read() != "pykv/v1":
                    raise self.LoadError(
                        "Directory is not a database or db has wrong version")

    def __init__(self, dbpath: str | Path = Path("/data/default")):
        self.path = Path(dbpath)
        self.invalids = dict()

        if self.path.exists() and not self.path.is_dir():
            raise self.LoadError("Path directs to a file, directory allowed")

        if not self.path.parent.exists():
            raise self.LoadError("Databases' directory does not exists")

        with DBLock(self.path.parent / ".lock"):
            self.create()

            for j in self.path.glob("*.journal"):
                self.invalids.update(Transaction.restore(j))
                for k, v in self.invalids:
                    self.set(k, v)
                self.invalids.clear()
                j.unlink()

    def get(self, key: str, none_if_not_found=False):
        if key in self.invalids:
            if self.invalids[key] is not None:
                return self.invalids[key]
            if none_if_not_found:
                return None
            raise self.NotFound()

        guess = self.path / base64.urlsafe_b64encode(key.encode()).decode()

        if not guess.exists():
            if none_if_not_found:
                return None
            raise self.NotFound()

        if not guess.is_file():
            raise self.CorruptedDatabase()

        with guess.open() as reader:
            return json.load(reader)

    def set(self, key: str, value):
        guess = self.path / base64.urlsafe_b64encode(key.encode()).decode()

        with DBLock(guess.with_name(f".{guess.name}.lock")):
            if guess.exists() and not guess.is_file():
                raise self.CorruptedDatabase()

            with guess.open("w") as writer:
                json.dump(value, writer)

    def select_keys(self, value) -> list:
        result = []
        for p in self.path.glob("*"):
            if p.name[0] != ".":
                key = base64.urlsafe_b64decode(p.name).decode()
                val = self.get(key)
                print(key, val, value)
                if value == val:
                    result.append(key)
        return result

    def remove(self, key: str):
        guess = self.path / base64.urlsafe_b64encode(key.encode()).decode()

        if guess.exists() and not guess.is_file():
            raise self.CorruptedDatabase()

        guess.unlink()

    def begin(self) -> "DBInterface":
        return Transaction(self)

    def commit(self) -> "DBInterface":
        return self

    def rollback(self) -> "DBInterface":
        return self
    
    def drop(self):
        import shutil

        with DBLock(self.path.parent / ".lock"):
            shutil.rmtree(self.path)
            self.path.mkdir()