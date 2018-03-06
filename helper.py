import multiprocessing as mp

import numpy as np

class PPMB(mp.Process):

    def __init__(self, srcs, obs, feats):
        self._srcs = srcs
        self._obs = obs
        self._feats = feats
        self._queue = mp.Queue()

    def get_phase(self, src, obs, feats):
        print src["id"]
        sobs = obs[obs["bm_src_id"] == src["id"]]
        max_mag_idx = np.argmax(sobs["pwp_stack_src_mag3"])
        t0 = sobs[max_mag_idx]["pwp_stack_src_hjd"]

        period = feats[feats["id"] == src["id"]]["PeriodLS"][0]

        # multi-band pseudo phase
        mb_hjd = np.mean([src["hjd_h"], src["hjd_j"], src["hjd_k"]])
        return np.abs(np.modf(mb_hjd  - t0)[0]) / period

    def run(self):
        ppmbs = np.empty(len(srcs))
        for idx, src in enumerate(srcs):
            ppmbs[idx] = self.get_phase(src, self._obs, self._feats)
        self._queue.put(ppmbs)

    def ppmb(self):
        return self._queue.get()
