import cdippy.utils.urls as uu
import cdippy.utils.utils as cu


class NcHashes:
    """Methods for working with the online list of historic nc file hashes."""

    hashes_url = "http://cdip.ucsd.edu/data_access/metadata/wavecdf_by_datemod.txt"
    new_hashes = {}

    def __init__(self, hash_file_location=""):
        self.hash_pkl = hash_file_location + "/HASH.pkl"

    def load_hash_table(self):
        lines = uu.read_url(self.hashes_url).strip().split("\n")
        for line in lines:
            if line[0:8] == "filename":
                continue
            fields = line.split("\t")
            if len(fields) < 7:
                continue
            self.new_hashes[fields[0]] = fields[6]

    def get_last_deployment(self, stn: str) -> str:
        """Returns the last deployment string, e.g. 'd03'."""
        last_deployment = "d00"
        for name in self.new_hashes:
            if name[0:5] == stn and name[5:7] == "_d" and last_deployment < name[6:9]:
                last_deployment = name[6:9]
        return last_deployment

    def compare_hash_tables(self) -> list:
        """Return a list of nc files that have changed or are new."""
        old_hashes = self.get_old_hashes()
        changed = []
        if old_hashes:
            if len(self.new_hashes) == 0:
                return []
            for key in self.new_hashes:
                if key not in old_hashes.keys() or (
                    key in old_hashes.keys() and old_hashes[key] != self.new_hashes[key]
                ):
                    changed.append(key)
        return changed

    def save_new_hashes(self):
        cu.pkl_dump(self.new_hashes, self.hash_pkl)

    def get_old_hashes(self):
        return cu.pkl_load(self.hash_pkl)
