import cdippy.utils.urls as uu
import cdippy.utils.utils as cu


class NcHashes:
    """
    A class that checks for changes to datasets since by reading the online list of historic netCDF file hashes.
    """

    hashes_url = "http://cdip.ucsd.edu/data_access/metadata/wavecdf_by_datemod.txt"
    new_hashes = {}

    def __init__(self, hash_file_location=""):
        self.hash_pkl = hash_file_location + "/HASH.pkl"

    def load_hash_table(self):
        """
        Save the list of new hashes loaded into memory by `load_hash_tables` as local pickle file.
        Overwrites last save HASH.pkl.
        """
        """
        """
        lines = uu.read_url(self.hashes_url).strip().split("\n")
        for line in lines:
            if line[0:8] == "filename":
                continue
            fields = line.split("\t")
            if len(fields) < 7:
                continue
            self.new_hashes[fields[0]] = fields[6]

    def compare_hash_tables(self) -> list:
        """
        Compare the current in-memory list of files, loaded by `load_hash_table` to the list saved in HASH.pkl and return a list of stations that are new or have changed.

        Returns:
            changed ([str]): A list of nc files that have changed or are since HASH.pkl was last saved.
        """
        old_hashes = self._get_old_hashes()
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
        """
        Save the list of new hashes loaded into memory by `load_hash_tables` as local pickle file.
        Overwrites last saved HASH.pkl.
        """
        cu.pkl_dump(self.new_hashes, self.hash_pkl)

    def _get_old_hashes(self):
        return cu.pkl_load(self.hash_pkl)
