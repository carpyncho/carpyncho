import feets

import numpy as np

from scipy.optimize import curve_fit

from astropy.stats import lombscargle

from feets.libs import ls_fap

from feets import Extractor


# =============================================================================
# CONSTANTS
# =============================================================================

EPS = np.finfo(float).eps


# =============================================================================
# FUNCTIONS
# =============================================================================

def lscargle_error(time, magnitude, error,
                   model_kwds=None, autopower_kwds=None):
    model_kwds = model_kwds or {}
    autopower_kwds = autopower_kwds or {}
    model = lombscargle.LombScargle(time, magnitude, error, **model_kwds)
    frequency, power = model.autopower(**autopower_kwds)

    fmax = np.argmax(power)

    return frequency, power, fmax


def fap_error(max_power, fmax, time, magnitude, error,
        method, normalization, method_kwds=None):
    method_kwds = method_kwds or {}
    dy = 0.01 if error is None else error
    return ls_fap.false_alarm_probability(
        Z=max_power, fmax=fmax, t=time, y=magnitude,
        dy=dy, method=method, normalization=normalization,
        method_kwds=method_kwds)


# =============================================================================
# EXTRACTOR CLASS
# =============================================================================

@feets.register_extractor
class LombScargleWithError(Extractor):

    data = ['magnitude', 'time', 'error']
    features = ["PeriodLS", "Period_fit", "Psi_CS", "Psi_eta"]
    params = {
        "lscargle_kwds": {
            "autopower_kwds": {
                "normalization": "standard",
                "maximum_frequency": 10.,
                "minimum_frequency": 1./200}},
        "fap_kwds": {
            "normalization": "standard",
            "method": "simple"}}

    def _compute_ls(self, magnitude, time, error, lscargle_kwds):
        frequency, power, fmax = lscargle_error(
            time=time, magnitude=magnitude, error=error, **lscargle_kwds)
        best_period = 1 / frequency[fmax]
        return frequency, power, fmax, best_period

    def _compute_fap(self, power, fmax, time, magnitude, error, fap_kwds):
        return fap_error(
            max_power=np.max(power), fmax=fmax, time=time,
            magnitude=magnitude, error=error, **fap_kwds)

    def _compute_cs(self, folded_data, N):
        sigma = np.std(folded_data)
        m = np.mean(folded_data)
        s = np.cumsum(folded_data - m) * 1.0 / (N * sigma)
        R = np.max(s) - np.min(s)
        return R

    def _compute_eta(self, folded_data, N):
        sigma2 = np.var(folded_data)
        Psi_eta = (1.0 / ((N - 1) * sigma2) *
                   np.sum(np.power(folded_data[1:] - folded_data[:-1], 2)))
        return Psi_eta

    def fit(self, magnitude, time, error, lscargle_kwds, fap_kwds):
        # first we retrieve the frequencies, power,
        # max frequency and best_period
        frequency, power, fmax, best_period = self._compute_ls(
            magnitude=magnitude, time=time, error=error,
            lscargle_kwds=lscargle_kwds)

        # false alarm probability
        fap = self._compute_fap(
            power=power, fmax=fmax, time=time,
            magnitude=magnitude, error=error, fap_kwds=fap_kwds)

        # fold the data
        new_time = np.mod(time, 2 * best_period) / (2 * best_period)
        folded_data = magnitude[np.argsort(new_time)]
        N = len(folded_data)

        # CS and Psi_eta
        R = self._compute_cs(folded_data, N)
        Psi_eta = self._compute_eta(folded_data, N)

        return {"PeriodLS": best_period, "Period_fit": fap,
                "Psi_CS": R, "Psi_eta": Psi_eta}


# =============================================================================
# EXTRACTOR CLASS
# =============================================================================

@feets.register_extractor
class FourierComponentsWithError(Extractor):

    data = ['magnitude', 'time', "error"]
    features = ['Freq1_harmonics_amplitude_0',
                'Freq1_harmonics_amplitude_1',
                'Freq1_harmonics_amplitude_2',
                'Freq1_harmonics_amplitude_3',
                'Freq2_harmonics_amplitude_0',
                'Freq2_harmonics_amplitude_1',
                'Freq2_harmonics_amplitude_2',
                'Freq2_harmonics_amplitude_3',
                'Freq3_harmonics_amplitude_0',
                'Freq3_harmonics_amplitude_1',
                'Freq3_harmonics_amplitude_2',
                'Freq3_harmonics_amplitude_3',
                'Freq1_harmonics_rel_phase_0',
                'Freq1_harmonics_rel_phase_1',
                'Freq1_harmonics_rel_phase_2',
                'Freq1_harmonics_rel_phase_3',
                'Freq2_harmonics_rel_phase_0',
                'Freq2_harmonics_rel_phase_1',
                'Freq2_harmonics_rel_phase_2',
                'Freq2_harmonics_rel_phase_3',
                'Freq3_harmonics_rel_phase_0',
                'Freq3_harmonics_rel_phase_1',
                'Freq3_harmonics_rel_phase_2',
                'Freq3_harmonics_rel_phase_3']
    params = {
        "lscargle_kwds": {
            "autopower_kwds": {
                "normalization": "standard",
                "maximum_frequency": 10.,
                "minimum_frequency": 1./200}}
    }

    def _model(self, x, a, b, c, Freq):
        return (a * np.sin(2 * np.pi * Freq * x) +
                b * np.cos(2 * np.pi * Freq * x) + c)

    def _yfunc_maker(self, Freq):
        def func(x, a, b, c):
            return (a * np.sin(2 * np.pi * Freq * x) +
                    b * np.cos(2 * np.pi * Freq * x) + c)
        return func

    def _components(self, magnitude, time, error, lscargle_kwds):
        time = time - np.min(time)
        A, PH = [], []
        for i in range(3):
            frequency, power, fmax = lscargle_error(
                time=time, magnitude=magnitude, error=error, **lscargle_kwds)

            fundamental_Freq = frequency[fmax]
            Atemp, PHtemp = [], []
            omagnitude = magnitude

            for j in range(4):
                function_to_fit = self._yfunc_maker((j + 1) * fundamental_Freq)
                popt0, popt1, popt2 = curve_fit(
                    function_to_fit, time, omagnitude)[0][:3]

                Atemp.append(np.sqrt(popt0 ** 2 + popt1 ** 2))
                PHtemp.append(np.arctan(popt1 / popt0))

                model = self._model(
                    time, popt0, popt1, popt2,
                    (j + 1) * fundamental_Freq)
                magnitude = np.array(magnitude) - model

            A.append(Atemp)
            PH.append(PHtemp)

        PH = np.asarray(PH)
        scaledPH = PH - PH[:, 0].reshape((len(PH), 1))

        return A, scaledPH

    def fit(self, magnitude, time, error, lscargle_kwds):
        lscargle_kwds = lscargle_kwds or {}

        A, sPH = self._components(
            magnitude=magnitude, time=time, error=error,
            lscargle_kwds=lscargle_kwds)
        result = {
            "Freq1_harmonics_amplitude_0": A[0][0],
            "Freq1_harmonics_amplitude_1": A[0][1],
            "Freq1_harmonics_amplitude_2": A[0][2],
            "Freq1_harmonics_amplitude_3": A[0][3],

            "Freq2_harmonics_amplitude_0": A[1][0],
            "Freq2_harmonics_amplitude_1": A[1][1],
            "Freq2_harmonics_amplitude_2": A[1][2],
            "Freq2_harmonics_amplitude_3": A[1][3],

            "Freq3_harmonics_amplitude_0": A[2][0],
            "Freq3_harmonics_amplitude_1": A[2][1],
            "Freq3_harmonics_amplitude_2": A[2][2],
            "Freq3_harmonics_amplitude_3": A[2][3],

            "Freq1_harmonics_rel_phase_0": sPH[0][0],
            "Freq1_harmonics_rel_phase_1": sPH[0][1],
            "Freq1_harmonics_rel_phase_2": sPH[0][2],
            "Freq1_harmonics_rel_phase_3": sPH[0][3],

            "Freq2_harmonics_rel_phase_0": sPH[1][0],
            "Freq2_harmonics_rel_phase_1": sPH[1][1],
            "Freq2_harmonics_rel_phase_2": sPH[1][2],
            "Freq2_harmonics_rel_phase_3": sPH[1][3],

            "Freq3_harmonics_rel_phase_0": sPH[2][0],
            "Freq3_harmonics_rel_phase_1": sPH[2][1],
            "Freq3_harmonics_rel_phase_2": sPH[2][2],
            "Freq3_harmonics_rel_phase_3": sPH[2][3]}
        return result


# =============================================================================
# EXTRACTOR CLASS
# =============================================================================

@feets.register_extractor
class GskewNearest(Extractor):
    """Median-of-magnitudes based measure of the skew.

    .. math::

        Gskew = m_{q3} + m_{q97} - 2m

    Where:

    - :math:`m_{q3}` is the median of magnitudes lesser or equal than the
      quantile 3.
    - :math:`m_{q97}` is the median of magnitudes greater or equal than the
      quantile 97.
    - :math:`m` is the median of magnitudes.

    """

    data = ['magnitude']
    features = ["Gskew"]
    params = {
        "interpolation": "nearest"}

    def calculate_gskew(self, magnitude, interpolation):
        median_mag = np.median(magnitude)
        F_3_value = np.percentile(magnitude, 3, interpolation=interpolation)
        F_97_value = np.percentile(magnitude, 97, interpolation=interpolation)

        skew = (np.median(magnitude[magnitude <= F_3_value]) +
                np.median(magnitude[magnitude >= F_97_value]) - 2 * median_mag)

        return skew

    def fit(self, magnitude, interpolation):
        skew = self.calculate_gskew(magnitude, interpolation=interpolation)
        return {"Gskew": skew}
