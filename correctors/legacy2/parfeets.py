
import numpy as np

from carpyncho.lib import feets_patch

import feets
from feets import preprocess


only_all = [
 'Amplitude',
 'Autocor_length',
 'Beyond1Std',
 'Con',
 'Eta_e',
 'FluxPercentileRatioMid20',
 'FluxPercentileRatioMid35',
 'FluxPercentileRatioMid50',
 'FluxPercentileRatioMid65',
 'FluxPercentileRatioMid80',
 'Freq1_harmonics_amplitude_0',
 'Freq1_harmonics_amplitude_1',
 'Freq1_harmonics_amplitude_2',
 'Freq1_harmonics_amplitude_3',
 'Freq1_harmonics_rel_phase_1',
 'Freq1_harmonics_rel_phase_2',
 'Freq1_harmonics_rel_phase_3',
 'Freq2_harmonics_amplitude_0',
 'Freq2_harmonics_amplitude_1',
 'Freq2_harmonics_amplitude_2',
 'Freq2_harmonics_amplitude_3',
 'Freq2_harmonics_rel_phase_1',
 'Freq2_harmonics_rel_phase_2',
 'Freq2_harmonics_rel_phase_3',
 'Freq3_harmonics_amplitude_0',
 'Freq3_harmonics_amplitude_1',
 'Freq3_harmonics_amplitude_2',
 'Freq3_harmonics_amplitude_3',
 'Freq3_harmonics_rel_phase_1',
 'Freq3_harmonics_rel_phase_2',
 'Freq3_harmonics_rel_phase_3',
 'Gskew',
 'LinearTrend',
 'MaxSlope',
 'Mean',
 'Meanvariance',
 'MedianAbsDev',
 'MedianBRP',
 'PairSlopeTrend',
 'PercentAmplitude',
 'PercentDifferenceFluxPercentile',
 'PeriodLS',
 'Period_fit',
 'Psi_CS',
 'Psi_eta',
 'Q31',
 'Rcs',
 'Skew',
 'SmallKurtosis',
 'Std',
 'StetsonK']



fs = feets.FeatureSpace(
    data=["magnitude", "time", "error"],
    only=only_all)



def extract(sid, obs, old_feats):
    time = obs.pwp_stack_src_hjd.values
    magnitude = obs.pwp_stack_src_mag3.values
    error = obs.pwp_stack_src_mag_err3.values

    sort = np.argsort(time)
    time, magnitude, error = time[sort], magnitude[sort], error[sort]
    time, magnitude, error = preprocess.remove_noise(time, magnitude, error, std_limit=3)

    new_feats = dict(zip(*fs.extract(time=time, magnitude=magnitude, error=error)))
    new_feats["ppmb"] = old_feats["ppmb"] * old_feats["PeriodLS"] / new_feats["PeriodLS"]
    new_feats["cnt"] = len(time)

    feats = old_feats.copy()
    feats.update(new_feats)

    return feats
