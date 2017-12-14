M = LOAD '$M' USING PigStorage(',') AS (mi, mj, mvalue);
N = LOAD '$N' USING PigStorage(',') AS (ni, nj, nvalue);
J = JOIN M by mj, N by ni;
I = FOREACH J GENERATE mi,nj,mvalue*nvalue;
G = GROUP I by (mi,nj);
O = FOREACH G {
	V = FOREACH I GENERATE $2;
	GENERATE group, SUM(V);
	}
STORE O INTO '$O' USING PigStorage (',');
