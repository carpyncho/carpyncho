#~ python in_corral.py freshload;
python in_corral.py run -s ReadTile ReadPawprintStack;
python in_corral.py run -s OGLE3TagTile;
python in_corral.py run -s PrepareForMatch;
time python in_corral.py run -s Match;
time python in_corral.py run -s MergeLightCurves;
