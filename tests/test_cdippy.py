# CDIP imports
import cdippy.cdipnc as nc
import cdippy.stndata as sd
import cdippy.mopdata as md

# import cdippy.mopdata as md
import cdippy.ncstats as ns
import cdippy.nchashes as nh
import cdippy.utils.urls as uu
import cdippy.utils.location as loc
import cdippy.ndbc as ndbc
import cdippy.spectra as sp

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

import numpy as np
import netCDF4


RESOURCES_DIR = "./tests/resources"

# test file info
archive_stn = "036p1"
archive_stn_file = archive_stn + "_d01.nc"
archive_start = "2000-09-30 00:00:00"
archive_1hr = "2000-09-30 01:00:00"


def get_urllopen_mock(filepath):
    with open(f"{RESOURCES_DIR}/{filepath}", "rb") as f:
        content = f.read()
    mock_response = MagicMock()
    mock_response.read.return_value = content
    mock_response.__enter__.return_value = mock_response
    return mock_response


def get_active_datasets(dataset_name):
    url = (
        f"http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/{dataset_name}/catalog.xml"
    )
    root = uu.load_et_root(url)
    datasets = []
    uu.rfindta(root, datasets, "dataset", "name")
    return datasets


def convert_date(date_str):
    """From '2022-05-26T01:02:03Z' to '2022-05-26 01:02:03'"""
    return date_str[0:10] + " " + date_str[11:19]


class TestCdipnc(unittest.TestCase):

    def setUp(self):
        self.ds = netCDF4.Dataset(f"{RESOURCES_DIR}/predeploy/028p0_d24_rt.nc")

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_url(self, mock_dataset):
        mock_dataset.return_value = self.ds
        a = nc.Archive(archive_stn, data_dir="https://cdip.ucsd.edu")
        self.assertEqual(
            a.url,
            f"https://cdip.ucsd.edu/thredds/dodsC/cdip/archive/{archive_stn}/{archive_stn_file}",
        )

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_data_path(self, mock_dataset):
        mock_dataset.return_value = self.ds
        a = nc.Archive(archive_stn, data_dir="/project/WNC/WNC_DATA")
        self.assertEqual(
            a.url, f"/project/WNC/WNC_DATA/ARCHIVE/{archive_stn}/{archive_stn_file}"
        )

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    @patch("cdippy.utils.urls.request.urlopen")
    def test_active_predeploy(self, mock_urlopen, mock_dataset):
        ds = "predeploy"
        catalog_name = f"{ds}/catalog.xml"
        mock_urlopen.return_value = get_urllopen_mock(catalog_name)
        dataset_names = get_active_datasets(ds)
        mock_dataset.return_value = self.ds
        r = None
        for dn in dataset_names:
            if "_rt" in dn:
                stn = dn[0:3]
                dep = int(dn[7:9])
                a = nc.Active(stn, dep, "predeploy")
                r = {}
                if a is not None:
                    a.set_request_info(pub_set="all")
                    r = a.get_request()
                self.assertTrue("waveTime" in r and len(r["waveTime"]) > 0)


class TestMopData(unittest.TestCase):

    def setUp(self):
        # Dates within test archive deployment BP100
        self.dt1 = "2025-06-26 00:00:00"
        self.dt2 = "2025-06-26 23:59:59"
        self.v = ["waveHs"]

        self.nowcast = netCDF4.Dataset(f"{RESOURCES_DIR}/MOP/BP100_nowcast.nc")
        self.ecmwf = netCDF4.Dataset(f"{RESOURCES_DIR}/MOP/BP100_ecmwf_fc.nc")
        self.forecast = netCDF4.Dataset(f"{RESOURCES_DIR}/MOP/BP100_forecast.nc")

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_read_nc_data(self, mock_dataset):
        mock_dataset.return_value = self.nowcast
        m = md.MopData("BP100", "nowcast")
        d = m.get_series(self.dt1, self.dt2, self.v)
        self.assertEqual(len(d["waveHs"]), 18)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_target_records(self, mock_dataset):
        mock_dataset.return_value = self.nowcast
        m = md.MopData("BP100", "nowcast")
        d = m.get_series(self.dt1, None, self.v, target_records=6)
        self.assertEqual(len(d["waveHs"]), 7)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_parameters(self, mock_dataset):
        mock_dataset.return_value = self.nowcast
        m = md.MopData("BP100", "nowcast")
        d = m.get_parameters(self.dt1, self.dt2)
        self.assertEqual(len(d.keys()), 5)
        self.assertTrue("waveDp" in d.keys())

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_spectra(self, mock_dataset):
        mock_dataset.return_value = self.nowcast
        m = md.MopData("BP100", "nowcast")
        d = m.get_spectra(self.dt1, self.dt2)
        self.assertEqual(len(d.keys()), 8)
        self.assertTrue("waveA1Value" in d.keys())

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_url(self, mock_dataset):
        mock_dataset.return_value = self.ecmwf
        m = md.MopData("BP100", "ecmwf_fc")
        self.assertEqual(
            m.url,
            "https://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/model/MOP_validation/BP100_ecmwf_fc.nc",
        )

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_meta(self, mock_dataset):
        mock_dataset.return_value = self.ecmwf
        m = md.MopData("BP100", "ecmwf_fc")
        d = m.get_mop_meta()
        self.assertTrue("time_coverage_start" in d.keys())
        self.assertEqual(len(d.keys()), 16)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_ecmwf_fc(self, mock_dataset):
        mock_dataset.return_value = self.ecmwf
        m = md.MopData("BP100", "ecmwf_fc")
        start = self.dt1
        d = m.get_series(start, vrs=self.v, target_records=60)
        self.assertEqual(len(d.keys()), 2)
        self.assertEqual(len(d["waveHs"]), 61)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_alongshore(self, mock_dataset):
        mock_dataset.return_value = self.forecast
        m = md.MopData("BP100", "forecast")
        start = self.dt1
        d = m.get_series(start, vrs=self.v, target_records=60)
        self.assertEqual(len(d.keys()), 2)
        self.assertEqual(len(d["waveHs"]), 61)


class TestSpectra(unittest.TestCase):

    def setUp(self):
        self.ds = netCDF4.Dataset(f"{RESOURCES_DIR}/archive/{archive_stn_file}")

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_redistribute(self, mock_dataset):
        mock_dataset.return_value = self.ds
        # Station and dates within an existing archive deployment
        stn = sd.StnData(archive_stn)
        dt_1 = archive_start
        dt_2 = archive_1hr
        data = stn.get_spectra(dt_1, dt_2)
        self.assertEqual(len(data["waveEnergyDensity"]), 2)
        s = sp.Spectra()
        s.set_spectrumArr_fromQuery(data)
        self.assertEqual(s.get_spectraNum(), 2)
        self.assertEqual(s.get_bandSize(), 64)
        s.redist_specArr("Spectrum_9band")
        self.assertEqual(s.get_bandSize(), 9)
        s.redist_specArr("Spectrum_100band")
        self.assertEqual(s.get_bandSize(), 100)
        data = s.specArr_ToDict()
        self.assertTrue("waveA1Value" in data.keys())
        self.assertTrue(len(data["waveA1Value"][0]) == 100)
        self.assertTrue("waveCheckFactor" in data.keys())
        self.assertTrue(len(data["waveCheckFactor"][0]) == 100)


class TestStnData(unittest.TestCase):

    def setUp(self):
        self.ds = netCDF4.Dataset(f"{RESOURCES_DIR}/archive/{archive_stn_file}")
        with patch("netCDF4.Dataset", return_value=self.ds):
            self.s = sd.StnData(archive_stn)
        # Dates within an existing archive deployment
        self.dt1 = archive_start
        self.dt2 = archive_1hr
        self.v = ["waveHs"]

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_read_nc_data(self, mock_dataset):
        mock_dataset.return_value = self.ds
        d = self.s.get_series(self.dt1, self.dt2, self.v)
        self.assertEqual(len(d["waveHs"]), 2)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_pub_set_and_mask(self, mock_dataset):
        mock_dataset.return_value = self.ds
        # Tests mask
        d = self.s.get_series(self.dt1, self.dt2, self.v, "nonpub")
        self.assertEqual(len(d["waveHs"]), 0)
        d = self.s.get_series(self.dt1, self.dt2, self.v, "nonpub", False)
        self.assertEqual(len(d["waveHs"]), 2)
        # Tests pub_set (TODO: Need to use a timespan with both public and nonpub data
        d = self.s.get_series(self.dt1, self.dt2, self.v, "public")
        self.assertEqual(len(d["waveHs"]), 2)
        d = self.s.get_series(self.dt1, self.dt2, self.v, "all")
        self.assertEqual(len(d["waveHs"]), 2)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_across_deployments(self, mock_dataset):
        mock_dataset.return_value = self.ds
        d = self.s.get_series(self.dt1, self.dt2, ["xyzData"], "public")
        self.assertEqual(len(d["xyzTime"]), 4608)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_target_records(self, mock_dataset):
        mock_dataset.return_value = self.ds
        d = self.s.get_series(self.dt1, None, self.v, target_records=6)
        self.assertEqual(len(d["waveHs"]), 7)

    def test_stn_meta(self):
        d = self.s.get_stn_meta()
        self.assertTrue("geospatial_lon_min" in d.keys())

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_mark1_filter_delay(self, mock_dataset):
        mock_dataset.return_value = self.ds
        s = sd.StnData("071p1")
        d = s.get_xyz(self.dt1, self.dt2)
        self.assertEqual(len(d["xyzTime"]), 4608)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_use_archive_if_no_moored_hs(self, mock_dataset):
        mock_dataset.return_value = self.ds
        s = sd.StnData("100p1", deploy_num=15)
        d = s.get_series(self.dt1, self.dt2, ["waveHs"])
        self.assertEqual(len(d["waveHs"]), 2)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_use_archive_if_no_moored_xyz(self, mock_dataset):
        mock_dataset.return_value = self.ds
        s = sd.StnData("100p1", deploy_num=15)
        d = s.get_series(self.dt1, self.dt2, ["xyzZDisplacement"])
        self.assertEqual(len(d["xyzZDisplacement"]), 4608)

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    @patch("cdippy.utils.urls.request.urlopen")
    def test_stn_meta_deploy_num(self, mock_urlopen, mock_dataset):
        ds = "predeploy"
        catalog_name = f"{ds}/catalog.xml"
        mock_urlopen.return_value = get_urllopen_mock(catalog_name)
        dataset_names = get_active_datasets("predeploy")
        mock_dataset.return_value = self.ds

        self.assertEqual(len(dataset_names), 11)
        for dn in dataset_names:
            if "_rt" in dn:
                stn = dn[0:3]
                dep = int(dn[7:9])
                s = sd.StnData(stn, deploy_num=dep)
                r = s.get_stn_meta()
                self.assertTrue("metaStationName" in r)

    def test_remove_duplicates(self):
        dd = {}
        dd["waveTime"] = np.array([1, 2, 3, 2, 4])
        dd["waveHs"] = np.array([0.1, 0.2, 0.3, 0.2, 0.4])
        r = self.s.remove_duplicates(dd)
        self.assertTrue(len(r["waveTime"]) == 4 and r["waveHs"][3] == 0.4)


class TestLatest(unittest.TestCase):
    def setUp(self):
        self.ds = netCDF4.Dataset(f"{RESOURCES_DIR}/realtime/latest_3day.nc")

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_get_latest(self, mock_dataset):
        mock_dataset.return_value = self.ds
        latest = nc.Latest()

        d = latest.get_latest(
            pub_set="both-all",
            meta_vars=[
                "metaLongitude",
                "metaLatitude",
                "metaWaterDepth",
                "metaDeployLabel",
                "metaWMOid",
            ],
            params=[
                "waveHs",
                "sstSeaSurfaceTemperature",
                "gpsLatitude",
                "gpsLongitude",
                "acmSpeed",
                "acmDirection",
            ],
        )
        self.assertEqual("gpsLatitude" in d, True) and self.assertEqual(
            "groupTime" in d, True
        ) and self.assertEqual("sstTimeBounds" in d, True) and self.assertEqual(
            "metaWMOid" in d, True
        )


class TestNcStats(unittest.TestCase):

    def setUp(self):
        self.ds = netCDF4.Dataset(f"{RESOURCES_DIR}/archive/{archive_stn_file}")

    @patch("cdippy.cdipnc.netCDF4.Dataset")
    def test_summary(self, mock_dataset):
        mock_dataset.return_value = self.ds
        stats = ns.NcStats(archive_stn)
        summary = stats.deployment_summary()
        self.assertEqual(
            summary["d01"]["time_coverage_start"], datetime(2000, 9, 27, 16, 0)
        )


class TestNcHashes(unittest.TestCase):

    @patch("cdippy.utils.urls.request.urlopen")
    def test_compare_hash_tables(self, mock_urlopen):
        mock_urlopen.return_value = get_urllopen_mock("HASH")

        hashes = nh.NcHashes(hash_file_location=RESOURCES_DIR)
        hashes.load_hash_table()
        compare = hashes.compare_hash_tables()
        self.assertEqual(len(compare), 0)

        mock_urlopen.return_value = get_urllopen_mock("HASH_new")
        hashes.load_hash_table()
        compare = hashes.compare_hash_tables()
        self.assertEqual(len(compare), 1)


class TestLocation(unittest.TestCase):

    def setUp(self):
        lat1 = 21.6689
        lon1 = -158.1156
        lat2 = 21.66915
        lon2 = -158.11487
        self.l1 = loc.Location(lat1, lon1)
        self.l2 = loc.Location(lat2, lon2)

    def test_write_loc(self):
        self.assertEqual(self.l1.write_loc(), "21.6689 -158.1156")

    def test_decimal_min_loc(self):
        self.assertEqual(self.l1.decimal_min_loc()["mlat"], "40.134")

    def test_get_distance(self):
        self.assertEqual(self.l1.get_distance(self.l2), 0.04342213936740085)

    def test_get_distance_formatted(self):
        self.assertEqual(self.l1.get_distance_formatted(self.l2), "0.04")


class TestNDBC(unittest.TestCase):

    @patch("cdippy.utils.urls.request.urlopen")
    def test_get_wmo_id(self, mock_urlopen):
        mock_urlopen.return_value = get_urllopen_mock("wmo_ids")
        self.assertEqual(ndbc.get_wmo_id("100", store=False), "46225")


class TestRequests(unittest.TestCase):
    user_agent = None

    # TODO: set header when netcdf dataset is accessed
    # class MockRequestHandler(BaseHTTPRequestHandler):
    #     user_agent = None

    #     def do_GET(self):
    #         print("CAPTURED")
    #         with open(f"{RESOURCES_DIR}/realtime/latest_3day.nc", "rb") as f:
    #             body = f.read()

    #         self.send_response(200)
    #         self.send_header("Content-Type", "applicaion/octet-stream")
    #         self.send_header("Content-Length", str(len(body)))
    #         self.end_headers()
    #         self.wfile.write(body)

    #     def log_message(self, format, *args):
    #         pass  # Disable default logging

    # def test_netcdf4_dataset_headers(self):
    #     httpd = HTTPServer(("127.0.0.1", 0), TestRequests.MockRequestHandler)
    #     port = httpd.server_port
    #     server_thread = threading.Thread(target=httpd.serve_forever)
    #     server_thread.start()

    #     try:
    #         test_cdip = nc.CDIPnc()
    #         url = f"http://127.0.0.1:{port}/thredds/dodsC/fake.nc"
    #         try:
    #             test_cdip.get_nc(url)
    #         except Exception:
    #             pass

    #         user_agent = TestRequests.MockRequestHandler.user_agent
    #         self.assertEqual(user_agent, uu.cdippy_lib)
    #     finally:
    #         httpd.shutdown()
    #         server_thread.join()   print(self.headers)
    #         TestRequests.MockRequestHandler.user_agent = dict(self.headers)[
    #             "User-Agent"
    #         ]

    def setUp(self):
        self.user_agent = None

    def mock_urlopen_response(self, request, *args, **kwargs):
        self.user_agent = dict(request.header_items())["User-agent"]

        class MockResponse:
            def read(self_inner):
                return b"mock data"

            def close(self_inner):
                pass

        return MockResponse()

    @patch("cdippy.utils.urls.request.urlopen")
    def test_url_open_headers(self, mock_urlopen):
        mock_urlopen.side_effect = self.mock_urlopen_response
        ndbc.get_wmo_id(stn=100, store=False)
        self.assertEqual(self.user_agent, uu.cdippy_lib)
