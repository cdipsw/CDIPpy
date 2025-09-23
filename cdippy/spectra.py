"""
Author: Sarah Heim. (Some of which is a port of Corey Olfe's code)

This code was originally taken from the cdip_mobile site.
"""

import numpy as np
import math
import importlib

MODULE = importlib.import_module("cdippy.spectra")
# cls = getattr(module, class_name)


class Spectra(object):
    def __init__(self):
        """initializing Spectra. Meant for using methods to create array
            of Spectrum subClass objects

        :ivar specArr: array of Spectrum subClass objects
        """
        self.specArr = []

    def get_spectraNum(self):
        """
        Gets the number of objects (spectrum) in the specArr

        Returns:
            len (num): the number of objects in the sprectra array.
        """
        return len(self.specArr)

    def __str__(self):
        return "Spectra is an array of {0} Spectrum(s)".format(self.get_spectraNum())

    def get_spectraType(self):
        """returns the type of Class of the first object in specArr,
        all should be the same i.e. Spectrum_64band"""
        if self.get_spectraNum() > 0:
            return type(self.specArr[0])
        else:
            return None

    def get_bandSize(self):
        """returns the size (number of freq/bands) of the spectrum in spectra"""
        if self.get_spectraNum() > 0:
            return len(self.specArr[0].freq)
        else:
            return 0

    def whichSpecClass(self, length):
        """
        Return the type subClass of Spectrum is appropriate according length passed
        i.e. Spectrum_64band
        :var int lenght: length/count of the number of frequencies
        """
        specObjs = Spectrum.__subclasses__()
        for sObj in specObjs:
            objNum = len(sObj().freq)
            if objNum == length:
                return sObj
        return None

    # def get_spectrumArr_from_StnData(self, stn, start, end):
    def set_spectrumArr_fromQuery(self, dataDict):
        """
        specArr is empty. Create Spectrum objects and put in specArr

        :var dataDict: dictionary (output from cdippy.stndata query)
        """
        bandNum = len(dataDict["waveEnergyDensity"][0])
        specCls = self.whichSpecClass(bandNum)
        for e, ep in enumerate(dataDict["waveTime"]):
            # create Spectrum object of appropriate type for each time
            # i.e.: spec = Spectrum_64band(stn)
            spec = specCls()
            spec.set_specAtts(dataDict, e)
            self.specArr.append(spec)

    def specArr_ToDict(self):
        """Output the specArr as a dictionary with keys like waveA1Value, waveEnergyDensity etc."""
        newDict = {}
        if len(self.specArr) == 0:
            return newDict

        names = [
            "waveTime",
            "waveEnergyDensity",
            "waveMeanDirection",
            "waveA1Value",
            "waveA2Value",
            "waveB1Value",
            "waveB2Value",
        ]
        lists = {}
        for name in names:
            lists[name] = []
        if hasattr(self.specArr[0], "check") and self.specArr[0].check is not None:
            lists["waveCheckFactor"] = []

        for s in self.specArr:
            lists["waveTime"].append(s.wTime)
            lists["waveEnergyDensity"].append(s.ener_dens)
            lists["waveMeanDirection"].append(s.dMean)
            lists["waveA1Value"].append(s.a1)
            lists["waveA2Value"].append(s.a2)
            lists["waveB1Value"].append(s.b1)
            lists["waveB2Value"].append(s.b2)
            if "waveCheckFactor" in lists:
                lists["waveCheckFactor"].append(s.check)

        newDict["waveTime"] = np.ma.array(lists["waveTime"])
        newDict["waveEnergyDensity"] = np.ma.array(lists["waveEnergyDensity"])
        newDict["waveMeanDirection"] = np.ma.array(lists["waveMeanDirection"])
        newDict["waveA1Value"] = np.ma.array(lists["waveA1Value"])
        newDict["waveA2Value"] = np.ma.array(lists["waveA2Value"])
        newDict["waveB1Value"] = np.ma.array(lists["waveB1Value"])
        newDict["waveB2Value"] = np.ma.array(lists["waveB2Value"])
        if "waveCheckFactor" in lists:
            newDict["waveCheckFactor"] = np.ma.array(lists["waveCheckFactor"])

        return newDict

    def redist_specArr(self, objName):
        """
        Will redistribute spectrum if necessary (if different type)

        :var int objName: name of the subClass to redistribute to. .i.e. ``Spectrum_9band``
        """
        if self.get_spectraType() != objName:
            for i, sp in enumerate(self.specArr):
                self.specArr[i] = sp.redistribute_sp(objName)


class Spectrum(object):
    def __init__(self):
        pass

    def __str__(self):
        return str(self.__dict__)
        # return "Station %s: \n\tstart: %s \n\tend : %s" % (self.stn, self.start.isoformat(), self.end.isoformat())

    def set_specAtts(self, query, i):
        """Set spectra attributes from cdippy.stndata query

        :var mArr query: multi-dimentional array returned from cdippy.stndata
        :var int i: index
        """
        self.wTime = query["waveTime"][i]
        self.dMean = query["waveMeanDirection"][i]
        self.ener_dens = query["waveEnergyDensity"][i]
        self.a1 = query["waveA1Value"][i]
        self.b1 = query["waveB1Value"][i]
        self.a2 = query["waveA2Value"][i]
        self.b2 = query["waveB2Value"][i]
        self.check = (
            query["waveCheckFactor"][i] if "waveCheckFactor" in query.keys() else None
        )

    def set_FreqBands(self, num, sz):
        """Makes array of frequencies
        :var int num: frequency or bandwith?
        :var int sz: size, number of bands
        """

        self.freq = np.ma.array(list(map(lambda x: x * num, range(1, sz + 1))))
        self.bandwidth = np.ma.array(([num] * sz), dtype=np.float32)
        # return list(map(lambda x: x*num, range(1, sz+1)))

    def freq_cutoffs(self):
        """returns array of tuples of all the (low,high) frequencies;
        a.k.a.bots/tops"""
        arr = []
        for i, f in enumerate(self.freq):
            b = self.bandwidth[i]
            # if i< 25: print(i, f, b)
            arr.append((f - b / 2, f + b / 2))
        return arr

    def recip(self, f):
        """returns INTEGER of reciprocal of number.
        Specifically for converting frequency (float) to period(integer)"""
        return round(1 / f)

    def peri_cutoffs(self):
        """returns array of tuples of all the (low,high) periods"""
        return list(map(lambda x: tuple(map(self.recip, x)), self.freq_cutoffs()))

    # def get_center_periods(self):
    #     return list(map(lambda x: "%.1f" % (1/x), self.freq))

    def ma_to_list(self, marray):
        """
        :var str marray: string name of attribute that contains a masked array
        """
        return list(np.ma.getdata(getattr(self, marray)))

    def get_Energy(self):
        """units:meters**2 per bandwidth.
        sum(get_energy) is Total Energy"""
        return self.ener_dens * self.bandwidth

    def get_SigWaveHt(self):
        """units: meters"""
        # return list(map(lambda x: self.calc_Hs(x), self.get_Energy()))
        return map(lambda x: self.calc_Hs(x), self.get_Energy())

    def get_Tp(self):
        # index with the most energy
        ind = np.argmax(list(self.get_SigWaveHt()))
        return 1 / (self.freq[ind])

    def get_Dp(self):
        # index with the most energy
        ind = np.argmax(list(self.get_SigWaveHt()))
        return self.dMean[ind]

    def calc_Hs(self, energy):
        """returns the square root of energy x 4"""
        return energy**0.5 * 4

    def total_Hs(self):
        """square root of Total Energy x 4"""
        # return self.calc_Hs(np.sum(self.get_Energy()))
        return self.calc_Hs(np.sum(self.get_Energy()))

    def redistribute_sp(self, specInstClass):
        """
        translation of Corey's redistribute_sp code:
        c  Subroutine that redistributes a spectrum into a new spectral layout.

        :var specInstClass: the class to redistribute to can be instance or name of Class
        """
        # c--   Initialize the new spectral dist (redist_sp)
        try:
            cls = getattr(MODULE, specInstClass)
        except Exception:
            return
        redist_sp = cls()
        reBands = len(redist_sp.freq)
        redist_sp.wTime = self.wTime
        redist_sp.dMean = np.ma.array(([-1] * reBands), dtype=np.float32)
        redist_sp.ener_dens = np.ma.zeros(reBands, dtype=np.float32)
        redist_sp.a1 = np.ma.zeros(reBands, dtype=np.float32)
        redist_sp.b1 = np.ma.zeros(reBands, dtype=np.float32)
        redist_sp.a2 = np.ma.zeros(reBands, dtype=np.float32)
        redist_sp.b2 = np.ma.zeros(reBands, dtype=np.float32)
        if hasattr(self, "check") and self.check is not None:
            self.check.mask = False
            redist_sp.check = np.ma.zeros(reBands, dtype=np.float32)

        redist_botsTops = redist_sp.freq_cutoffs()
        orig_botsTops = self.freq_cutoffs()

        # c--   Do the business - loop over the new bins, adding in each of the original
        # c--   spectral bands to the appropriate bin. Partition bands where necessary.
        for i in range(reBands):
            cos_sum = 0
            sin_sum = 0
            miss_dir = False
            rBot, rTop = redist_botsTops[i][0], redist_botsTops[i][1]
            # print('%s: (%.3f, %.3f)' % (i, rBot, rTop))
            for j, ob in enumerate(self.freq):
                # minor re-write of bot/top
                # c--   If the full band falls into the current bin, add the entire contents
                # c--   If the bottom of the band falls in the bin, add in the appropriate portion
                # c--   If the top of the band falls in the bin, add in the appropriate portion
                # c--   If the middle of the band falls in the bin, add in the appropriate portion
                oBot, oTop = orig_botsTops[j][0], orig_botsTops[j][1]
                bot = rBot if rBot >= oBot else oBot
                top = rTop if rTop <= oTop else oTop
                if bot < top:
                    # Moved band_calcs here:
                    # c  Helper for REDISTRIBUTE_SP; adds components of original spectral layout
                    # c  into the redistributed layout, weighting by energy
                    curr_energy = self.ener_dens[j] * (top - bot)
                    # [redist_sp, miss_dir, sin_sum, cos_sum] = self.band_calcs(redist_sp, curr_energy, sin_sum, cos_sum, miss_dir, i, j)
                    if curr_energy != 0:
                        redist_sp.ener_dens[i] += curr_energy
                        # print('\tredist(%.3f, %.3f) new(%.3f, %.3f), %f, %f, %f' %
                        # (oBot, oTop, bot, top, self.ener_dens[j],
                        # curr_energy, redist_sp.ener_dens[i]))
                        if self.dMean[j] == -1:
                            miss_dir = True
                        else:
                            redist_sp.a1[i] += curr_energy * self.a1[j]
                            redist_sp.b1[i] += curr_energy * self.b1[j]
                            redist_sp.a2[i] += curr_energy * self.a2[j]
                            redist_sp.b2[i] += curr_energy * self.b2[j]
                            if hasattr(self, "check") and self.check is not None:
                                redist_sp.check[i] += curr_energy * self.check[j]
                            sin_sum += curr_energy * math.sin(
                                math.radians(self.dMean[j])
                            )
                            cos_sum += curr_energy * math.cos(
                                math.radians(self.dMean[j])
                            )

            # c--   Calculate direction and calc ener_dens once bin is complete
            if redist_sp.ener_dens[i] > 0:
                redist_sp.ener_dens[i] /= redist_sp.bandwidth[i]
                if not miss_dir:
                    sin_avg = sin_sum / redist_sp.ener_dens[i]
                    cos_avg = cos_sum / redist_sp.ener_dens[i]
                    redist_sp.dMean[i] = math.degrees(math.atan2(sin_avg, cos_avg))
                    if redist_sp.dMean[i] < 0:
                        redist_sp.dMean[i] += 360
                    redist_sp.a1[i] /= redist_sp.bandwidth[i]
                    redist_sp.b1[i] /= redist_sp.bandwidth[i]
                    redist_sp.a2[i] /= redist_sp.bandwidth[i]
                    redist_sp.b2[i] /= redist_sp.bandwidth[i]
                    if hasattr(self, "check") and self.check is not None:
                        redist_sp.check[i] /= redist_sp.bandwidth[i]

            # c--   Normalize once energy redistributed
            # c  Subroutine that normalizes the coefficients in a sp_data_block. Direction
            # c  is set to -1 for any band in which the coeffs can't be normalized
            # c  by energy.
            if redist_sp.dMean[i] != -1:
                redist_sp.a1[i] /= redist_sp.ener_dens[i]
                redist_sp.b1[i] /= redist_sp.ener_dens[i]
                redist_sp.a2[i] /= redist_sp.ener_dens[i]
                redist_sp.b2[i] /= redist_sp.ener_dens[i]
                if hasattr(self, "check") and self.check is not None:
                    redist_sp.check[i] /= redist_sp.ener_dens[i]
                    if redist_sp.check[i] > 2.55:
                        redist_sp.check[i] = 2.55
                max_coeff = max(
                    redist_sp.a1[i], redist_sp.b1[i], redist_sp.a2[i], redist_sp.b2[i]
                )
                min_coeff = min(
                    redist_sp.a1[i], redist_sp.b1[i], redist_sp.a2[i], redist_sp.b2[i]
                )
                if max_coeff > 1 or min_coeff < -1:
                    redist_sp.dMean[i] = -1

        return redist_sp


class Spectrum_64band(Spectrum):
    def __init__(self):
        super().__init__()
        self.freq = [
            0.025,
            0.03,
            0.035,
            0.04,
            0.045,
            0.05,
            0.055,
            0.06,
            0.065,
            0.07,
            0.075,
            0.08,
            0.085,
            0.09,
            0.095,
            0.10125,
            0.11,
            0.12,
            0.13,
            0.14,
            0.15,
            0.16,
            0.17,
            0.18,
            0.19,
            0.2,
            0.21,
            0.22,
            0.23,
            0.24,
            0.25,
            0.26,
            0.27,
            0.28,
            0.29,
            0.3,
            0.31,
            0.32,
            0.33,
            0.34,
            0.35,
            0.36,
            0.37,
            0.38,
            0.39,
            0.4,
            0.41,
            0.42,
            0.43,
            0.44,
            0.45,
            0.46,
            0.47,
            0.48,
            0.49,
            0.5,
            0.51,
            0.52,
            0.53,
            0.54,
            0.55,
            0.56,
            0.57,
            0.58,
        ]
        self.bandwidth = [
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.0075,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
        ]


class Spectrum_9band(Spectrum):
    def __init__(self):
        super().__init__()
        self.freq = [
            0.0352,
            0.0505,
            0.0590,
            0.0670,
            0.0774,
            0.0917,
            0.1125,
            0.1458,
            0.3333,
        ]
        self.bandwidth = [
            0.0205,
            0.0101,
            0.0069,
            0.0089,
            0.0119,
            0.0167,
            0.0250,
            0.0417,
            0.3333,
        ]


class Spectrum_100band(Spectrum):
    def __init__(self):
        super().__init__()
        self.freq = [
            0.025,
            0.03,
            0.035,
            0.04,
            0.045,
            0.05,
            0.055,
            0.06,
            0.065,
            0.07,
            0.075,
            0.08,
            0.085,
            0.09,
            0.095,
            0.1,
            0.105,
            0.11,
            0.115,
            0.12,
            0.125,
            0.13,
            0.135,
            0.14,
            0.145,
            0.15,
            0.155,
            0.16,
            0.165,
            0.17,
            0.175,
            0.18,
            0.185,
            0.19,
            0.195,
            0.2,
            0.205,
            0.21,
            0.215,
            0.22,
            0.225,
            0.23,
            0.235,
            0.24,
            0.245,
            0.25125,
            0.26,
            0.27,
            0.28,
            0.29,
            0.3,
            0.31,
            0.32,
            0.33,
            0.34,
            0.35,
            0.36,
            0.37,
            0.38,
            0.39,
            0.4,
            0.41,
            0.42,
            0.43,
            0.44,
            0.45,
            0.46,
            0.47,
            0.48,
            0.49,
            0.5,
            0.51,
            0.52,
            0.53,
            0.54,
            0.55,
            0.56,
            0.57,
            0.5825,
            0.6,
            0.62,
            0.64,
            0.66,
            0.68,
            0.7,
            0.72,
            0.74,
            0.76,
            0.78,
            0.8,
            0.82,
            0.84,
            0.86,
            0.88,
            0.9,
            0.92,
            0.94,
            0.96,
            0.98,
            1.0,
        ]
        self.bandwidth = [
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.005,
            0.0075,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.015,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
            0.02,
        ]


class Spectrum_128band(Spectrum):
    def __init__(self):
        super().__init__()
        self.set_FreqBands(0.00390625, 128)


class Spectrum_custom(Spectrum):
    def __init__(self, fr=[], bw=[]):
        super().__init__()
        """
        :var arr fr: array of frequency(ies)
        :var arr bw: array of bandwidth(s)
        """
        self.freq = fr
        self.bandwidth = bw
