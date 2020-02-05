

/*********************/
/* ETR Insights Data */
/*********************/



/***************/
/* Data Import */
/***************/

/*
Clients will need to enter the file path for the associated files (Source Dataset, Insight Dataset) into the following let statements.
Please leave the single quotation marks before and after the file path.
*/

%let SourceData_FilePath = 'INSERT_CLIENT_FILEPATH';
%let ReturnsData_FilePath = 'INSERT_CLIENT_FILEPATH';
%let FTECReturnsData_FilePath = "INSERT_CLIENT_FILEPATH";


proc import datafile = &SourceData_FilePath.
	out = Source1
	dbms = csv replace;
	guessingrows= MAX;
run;


proc import datafile = &StockReturnsData_FilePath.
	out = SPReturns
	dbms = csv replace;
	guessingrows= 1000;
run;

proc import datafile = &FTECReturnsData_FilePath.
	out = FTECReturns
	dbms = csv replace;
run;

/*
The most recent Survey_ID is set to a macro variable for later use.
*/
proc sort data = Source1; by Survey_ID; run;

proc means data = Source1 noprint;
	var Survey_ID;
	output out = Survey_Max1 max = / autoname;
run;

data Survey_Max2;
	set Survey_Max1;
	call symputx ("Survey_Max",Survey_ID_Max);
run;
%put n of x is &Survey_Max;


/**************************************************/
/* Expected Enterprise Spend + Market Share Theme */
/**************************************************/

/*
Spending intentions for each vendor are aggregated to calculate spend metrics:
Citations, Adoption %, Increase %, Flat %, Decrease %, Replacing %, Net Score.
Unique number of respondents in each sector is merged on to calculate Market Share.
*/
proc sort data = Source1 out = Spend1a; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
	Respondent_ID Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current; run;
proc sort data = Source1 (where = (Metric ^= "REPLACING")) nodupkey out = Spend1b; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
	Respondent_ID Sector_Current; run;

proc freq data = Spend1a noprint;
	tables Survey_Description_1 * Survey_ID * Survey_Launch * Survey_Close * Announcement_Date * Sector_Current * Vendor_Current * Product_Current * Symbol_ID_Current * Bloomberg_ID_Current * FIGI_ID_Current * 
		 Sector_Historical * Vendor_Historical * Product_Historical * Symbol_ID_Historical * Bloomberg_ID_Historical * FIGI_ID_Historical * Metric / out = Spend2a (drop = percent);
run;
proc freq data = Spend1b noprint;
	tables Survey_Description_1 * Survey_ID * Survey_Launch * Survey_Close * Announcement_Date * Sector_Current / out = Spend2b (drop = percent);
run;

proc transpose data = Spend2a out = Spend3 (drop = _:);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical;
	id Metric;
	var Count;
run;

proc stdize data = Spend3 reponly missing = 0 out = Spend4;
	var _numeric_;
run;

data Spend5;
	merge Spend4 Spend2b (rename = (Count = Sector_N_ExR));
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Sector_Current;
	Citations = sum(Adoption, Increase, Flat, Decrease, Replacing);
	Citations_ExR = sum(Adoption, Increase, Flat, Decrease);
	AdoptionP = Adoption / Citations;
	IncreaseP = Increase / Citations;
	FlatP = Flat / Citations;
	DecreaseP = Decrease / Citations;
	ReplacingP = Replacing / Citations;
	NetScore = (Adoption + Increase - Decrease - Replacing) / Citations;
	MarketShare = Citations_ExR / Sector_N_ExR;
	drop Adoption Increase Flat Decrease Replacing Citations_ExR Sector_N_ExR;
run;

proc sort data = Spend5; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical; run;

proc transpose data = Spend5 out = Spend6 (drop = _label_ rename = (_name_ = Metric Col1 = Value));
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical Citations;
	var AdoptionP IncreaseP FlatP DecreaseP ReplacingP NetScore MarketShare;
run;

/*
Survey-over-Survey and Year-over-Year values for each metric are merged on to calculate deltas.
Deltas are used to measure recent inflections and longer-term trends.
*/

data Spend7
		Spend7_sos (keep = Survey_ID Sector_Current Vendor_Current Product_Current Metric Value
			rename = (Survey_ID = Survey_ID_sos Value = Value_sos))
		Spend7_yoy (keep = Survey_ID Sector_Current Vendor_Current Product_Current Metric Value
			rename = (Survey_ID = Survey_ID_yoy Value = Value_yoy));
	set Spend6;

	Survey_ID_sos = Survey_ID - 1;

	if Survey_ID = 3 then Survey_ID_yoy = 1;
	if Survey_ID = 4 or Survey_ID = 5 then Survey_ID_yoy = 2;
	if Survey_ID >= 6 then do;
		if mod(Survey_ID, 2) = 0 then Survey_ID_yoy = Survey_ID - 3;
		if mod(Survey_ID, 2) = 1 then Survey_ID_yoy = Survey_ID - 4;
	end;
run;

proc sort data = Spend7; by Sector_Current Vendor_Current Product_Current Metric Survey_ID_sos; run;
proc sort data = Spend7_sos; by Sector_Current Vendor_Current Product_Current Metric Survey_ID_sos; run;

data Spend8a;
	merge Spend7 (in = x) Spend7_sos;
	by Sector_Current Vendor_Current Product_Current Metric Survey_ID_sos;
	if x;
run;

proc sort data = Spend8a; by Sector_Current Vendor_Current Product_Current Metric Survey_ID_yoy; run;
proc sort data = Spend7_yoy; by Sector_Current Vendor_Current Product_Current Metric Survey_ID_yoy; run;

data Spend8b;
	merge Spend8a (in = x) Spend7_yoy;
	by Sector_Current Vendor_Current Product_Current Metric Survey_ID_yoy;
	if x;
	Delta_sos = Value - Value_sos;
	Delta_yoy = Value - Value_yoy;
	drop Survey_ID_sos Survey_ID_yoy;
run;

/*
Weighted survey averages for each metric value and delta are calculated to create z-scores.
*/

proc sort data = Spend8b; by Survey_ID Metric; run;

proc means data = Spend8b noprint vardef = weight;
	by Survey_ID Metric;
	var Value Delta_sos Delta_yoy;
	weight Citations;
	output out = SurveyAvg1 (drop = _:) mean = std = / autoname;
run;

proc sort data = Spend8b; by Survey_ID Metric; run;
proc sort data = SurveyAvg1; by Survey_ID Metric; run;

data Spend9;
	merge Spend8b SurveyAvg1 (rename = (Value_Mean = Value_SurveyMean Value_StdDev = Value_SurveyStdDev
									Delta_sos_Mean = Delta_sos_SurveyMean Delta_sos_StdDev = Delta_sos_SurveyStdDev
									Delta_yoy_Mean = Delta_yoy_SurveyMean Delta_yoy_StdDev = Delta_yoy_SurveyStdDev));
	by Survey_ID Metric;

	Value_SurveyZ = (Value - Value_SurveyMean) / Value_SurveyStdDev;
	Delta_sos_SurveyZ = (Delta_sos - Delta_sos_SurveyMean) / Delta_sos_SurveyStdDev;
	Delta_yoy_SurveyZ = (Delta_yoy - Delta_yoy_SurveyMean) / Delta_yoy_SurveyStdDev;
run;

proc sort data = Spend9; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical Citations Metric; run;

proc transpose data = Spend9 out = Spend10 (rename = (_name_ = Metric2 Col1 = Value));
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical Citations Metric;
	var Value Delta_sos Delta_yoy Value_SurveyZ Delta_sos_SurveyZ Delta_yoy_SurveyZ;
run;

data Spend11;
	set Spend10;
	if Value ^= .;
	Metric3 = cats(Metric, "_", Metric2);
run;

proc sort data = Spend11; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical Citations Metric3; run;

proc transpose data = Spend11 out = Spend12 (drop = _name_);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical Citations;
	id Metric3;
	var Value;
run;

/*
The Adoption_Rating, Increase_Rating, Decrease_Rating, Replacing_Rating, NetScore_Rating and MarketShare_Rating ratings
are assigned based on the following decision tree algorithms.
See Appendix A1-A5 and Appendix B of the accompanying methodology documentation for graphical representations of the decision tree algorithms.
*/

%let zscore_value_cutoff = 1.000;
%let zscore_delta_cutoff = 0.675;

data Spend13;
	set Spend12;

	if Citations >= 30 then do;

		/*Adoption Rating*/
		if AdoptionP_Delta_sos_SurveyZ ^= . and AdoptionP_Delta_yoy_SurveyZ ^= . then do;
			if AdoptionP_Value_SurveyZ >= &zscore_value_cutoff. then Adoption_Rating = "Positive";
			else if AdoptionP_Value_SurveyZ >= 0 and AdoptionP_Delta_sos_SurveyZ >= 0 and AdoptionP_Delta_yoy_SurveyZ >= 0 and
				(AdoptionP_Delta_sos_SurveyZ >= &zscore_delta_cutoff. or AdoptionP_Delta_yoy_SurveyZ >= &zscore_delta_cutoff.) then Adoption_Rating = "Positive";
			else if AdoptionP_Delta_sos_SurveyZ <= 0 and AdoptionP_Delta_yoy_SurveyZ <= 0 and
				(AdoptionP_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. or AdoptionP_Delta_yoy_SurveyZ <= -&zscore_delta_cutoff.) then Adoption_Rating = "Negative";
		end;
		else if AdoptionP_Delta_sos_SurveyZ ^= . and AdoptionP_Delta_yoy_SurveyZ = . then do;
			if AdoptionP_Value_SurveyZ >= &zscore_value_cutoff. then Adoption_Rating = "Positive";
			else if AdoptionP_Value_SurveyZ >= 0 and AdoptionP_Delta_sos_SurveyZ >= &zscore_delta_cutoff. then Adoption_Rating = "Positive";
			else if AdoptionP_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. then Adoption_Rating = "Negative";
		end;
		else if AdoptionP_Delta_sos_SurveyZ = . and AdoptionP_Delta_yoy_SurveyZ = . then do;
			if AdoptionP_Value_SurveyZ >= &zscore_value_cutoff. then Adoption_Rating = "Positive";
		end;

		/*Increase Rating*/
		if IncreaseP_Delta_sos_SurveyZ ^= . and IncreaseP_Delta_yoy_SurveyZ ^= . then do;
			if IncreaseP_Value_SurveyZ >= &zscore_value_cutoff. then Increase_Rating = "Positive";
			else if IncreaseP_Value_SurveyZ >= 0 and IncreaseP_Delta_sos_SurveyZ >= 0 and IncreaseP_Delta_yoy_SurveyZ >= 0 and
				(IncreaseP_Delta_sos_SurveyZ >= &zscore_delta_cutoff. or IncreaseP_Delta_yoy_SurveyZ >= &zscore_delta_cutoff.) then Increase_Rating = "Positive";
			else if IncreaseP_Delta_sos_SurveyZ <= 0 and IncreaseP_Delta_yoy_SurveyZ <= 0 and
				(IncreaseP_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. or IncreaseP_Delta_yoy_SurveyZ <= -&zscore_delta_cutoff.) then Increase_Rating = "Negative";
		end;
		else if IncreaseP_Delta_sos_SurveyZ ^= . and IncreaseP_Delta_yoy_SurveyZ = . then do;
			if IncreaseP_Value_SurveyZ >= &zscore_value_cutoff. then Increase_Rating = "Positive";
			else if IncreaseP_Value_SurveyZ >= 0 and IncreaseP_Delta_sos_SurveyZ >= &zscore_delta_cutoff. then Increase_Rating = "Positive";
			else if IncreaseP_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. then Increase_Rating = "Negative";
		end;
		else if IncreaseP_Delta_sos_SurveyZ = . and IncreaseP_Delta_yoy_SurveyZ = . then do;
			if IncreaseP_Value_SurveyZ >= &zscore_value_cutoff. then Increase_Rating = "Positive";
		end;

		/*Decrease Rating*/
		if DecreaseP_Delta_sos_SurveyZ ^= . and DecreaseP_Delta_yoy_SurveyZ ^= . then do;
			if DecreaseP_Value_SurveyZ >= &zscore_value_cutoff. then Decrease_Rating = "Negative";
			else if DecreaseP_Value_SurveyZ >= 0 and DecreaseP_Delta_sos_SurveyZ >= 0 and DecreaseP_Delta_yoy_SurveyZ >= 0 and
				(DecreaseP_Delta_sos_SurveyZ >= &zscore_delta_cutoff. or DecreaseP_Delta_yoy_SurveyZ >= &zscore_delta_cutoff.) then Decrease_Rating = "Negative";
			else if DecreaseP_Delta_sos_SurveyZ <= 0 and DecreaseP_Delta_yoy_SurveyZ <= 0 and
				(DecreaseP_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. or DecreaseP_Delta_yoy_SurveyZ <= -&zscore_delta_cutoff.) then Decrease_Rating = "Positive";
		end;
		else if DecreaseP_Delta_sos_SurveyZ ^= . and DecreaseP_Delta_yoy_SurveyZ = . then do;
			if DecreaseP_Value_SurveyZ >= &zscore_value_cutoff. then Decrease_Rating = "Negative";
			else if DecreaseP_Value_SurveyZ >= 0 and DecreaseP_Delta_sos_SurveyZ >= &zscore_delta_cutoff. then Decrease_Rating = "Negative";
			else if DecreaseP_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. then Decrease_Rating = "Positive";
		end;
		else if DecreaseP_Delta_sos_SurveyZ = . and DecreaseP_Delta_yoy_SurveyZ = . then do;
			if DecreaseP_Value_SurveyZ >= &zscore_value_cutoff. then Decrease_Rating = "Negative";
		end;

		/*Replacing Rating*/
		if ReplacingP_Delta_sos_SurveyZ ^= . and ReplacingP_Delta_yoy_SurveyZ ^= . then do;
			if ReplacingP_Value_SurveyZ >= &zscore_value_cutoff. then Replacing_Rating = "Negative";
			else if ReplacingP_Value_SurveyZ >= 0 and ReplacingP_Delta_sos_SurveyZ >= 0 and ReplacingP_Delta_yoy_SurveyZ >= 0 and
				(ReplacingP_Delta_sos_SurveyZ >= &zscore_delta_cutoff. or ReplacingP_Delta_yoy_SurveyZ >= &zscore_delta_cutoff.) then Replacing_Rating = "Negative";
			else if ReplacingP_Delta_sos_SurveyZ <= 0 and ReplacingP_Delta_yoy_SurveyZ <= 0 and
				(ReplacingP_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. or ReplacingP_Delta_yoy_SurveyZ <= -&zscore_delta_cutoff.) then Replacing_Rating = "Positive";
		end;
		else if ReplacingP_Delta_sos_SurveyZ ^= . and ReplacingP_Delta_yoy_SurveyZ = . then do;
			if ReplacingP_Value_SurveyZ >= &zscore_value_cutoff. then Replacing_Rating = "Negative";
			else if ReplacingP_Value_SurveyZ >= 0 and ReplacingP_Delta_sos_SurveyZ >= &zscore_delta_cutoff. then Replacing_Rating = "Negative";
			else if ReplacingP_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. then Replacing_Rating = "Positive";
		end;
		else if ReplacingP_Delta_sos_SurveyZ = . and ReplacingP_Delta_yoy_SurveyZ = . then do;
			if ReplacingP_Value_SurveyZ >= &zscore_value_cutoff. then Replacing_Rating = "Negative";
		end;

		/*Net Score Rating*/
		if NetScore_Delta_sos_SurveyZ ^= . and NetScore_Delta_yoy_SurveyZ ^= . then do;
			if NetScore_Value_SurveyZ >= &zscore_value_cutoff. then NetScore_Rating = "Positive";
			else if NetScore_Value_SurveyZ >= 0 and NetScore_Delta_sos_SurveyZ >= 0 and NetScore_Delta_yoy_SurveyZ >= 0 and
				(NetScore_Delta_sos_SurveyZ >= &zscore_delta_cutoff. or NetScore_Delta_yoy_SurveyZ >= &zscore_delta_cutoff.) then NetScore_Rating = "Positive";
			else if NetScore_Value_SurveyZ <= 0 and NetScore_Delta_sos_SurveyZ <= 0 and NetScore_Delta_yoy_SurveyZ <= 0 and
				(NetScore_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. or NetScore_Delta_yoy_SurveyZ <= -&zscore_delta_cutoff.) then NetScore_Rating = "Negative";
			else if NetScore_Value_SurveyZ <= -&zscore_value_cutoff. then NetScore_Rating = "Negative";
		end;
		else if NetScore_Delta_sos_SurveyZ ^= . and NetScore_Delta_yoy_SurveyZ = . then do;
			if NetScore_Value_SurveyZ >= &zscore_value_cutoff. then NetScore_Rating = "Positive";
			else if NetScore_Value_SurveyZ >= 0 and NetScore_Delta_sos_SurveyZ >= &zscore_delta_cutoff. then NetScore_Rating = "Positive";
			else if NetScore_Value_SurveyZ <= 0 and NetScore_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. then NetScore_Rating = "Negative";
			else if NetScore_Value_SurveyZ <= -&zscore_value_cutoff. then NetScore_Rating = "Negative";
		end;
		else if NetScore_Delta_sos_SurveyZ = . and NetScore_Delta_yoy_SurveyZ = . then do;
			if NetScore_Value_SurveyZ >= &zscore_value_cutoff. then NetScore_Rating = "Positive";
			else if NetScore_Value_SurveyZ <= -&zscore_value_cutoff. then NetScore_Rating = "Negative";
		end;

		/*Market Share Rating*/
		if MarketShare_Delta_sos_SurveyZ ^= . and MarketShare_Delta_yoy_SurveyZ ^= . then do;
			if MarketShare_Delta_sos_SurveyZ >= 0 and MarketShare_Delta_yoy_SurveyZ >= 0 and
				(MarketShare_Delta_sos_SurveyZ >= &zscore_delta_cutoff. or MarketShare_Delta_yoy_SurveyZ >= &zscore_delta_cutoff.) then MarketShare_Rating = "Positive";
			else if MarketShare_Delta_sos_SurveyZ <= 0 and MarketShare_Delta_yoy_SurveyZ <= 0 and
				(MarketShare_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. or MarketShare_Delta_yoy_SurveyZ <= -&zscore_delta_cutoff.) then MarketShare_Rating = "Negative";
		end;
		else if MarketShare_Delta_sos_SurveyZ ^= . and MarketShare_Delta_yoy_SurveyZ = . then do;
			if MarketShare_Delta_sos_SurveyZ >= &zscore_delta_cutoff. then MarketShare_Rating = "Positive";
			if MarketShare_Delta_sos_SurveyZ <= -&zscore_delta_cutoff. then MarketShare_Rating = "Negative";
		end;
	end;

	drop MarketShare_Value_SurveyZ;
run;

proc sort data = Spend13; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical; run;

data Spend_Final; set Spend13; run;


/*****************************************/
/* Peer Benchmarking / Competition Theme */
/*****************************************/

/*
Pairwise combinations of vendors within the same sector are matched.
Shared accounts Citations and Net Scores are calculated for each pairwise combination.
*/
proc sort data = Source1; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
	Respondent_ID Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current; run;

data Peer1a (rename = (Vendor_Current = Vendor_Filter Product_Current = Product_Filter Metric = Metric_Filter))
		Peer1b (keep = Survey_ID Respondent_ID Sector_Current Vendor_Current Product_Current Metric
				rename = (Vendor_Current = Vendor_Calc Product_Current = Product_Calc Metric = Metric_Calc));
	set Source1;
run;

proc sql;
	create table Peer2 as
		select * from Peer1a, Peer1b
		where Peer1a.Survey_ID = Peer1b.Survey_ID and Peer1a.Respondent_ID = Peer1b.Respondent_ID and
			Peer1a.Sector_Current = Peer1b.Sector_Current and (Peer1a.Vendor_Filter ^= Peer1b.Vendor_Calc or Peer1a.Product_Filter ^= Peer1b.Product_Calc);
quit;

data Peer3;
	set Peer2 (in = x where = (Metric_Filter = "ADOPTION" or Metric_Filter = "INCREASE"))
		Peer2 (in = y where = (Metric_Filter = "FLAT" or Metric_Filter = "DECREASE" or Metric_Filter = "REPLACING"));
	if x then Metric_Filter_Group = "Pos";
	if y then Metric_Filter_Group = "Neg";
run;

proc freq data = Peer3 noprint;
	tables Survey_Description_1 * Survey_ID * Survey_Launch * Survey_Close * Announcement_Date *
		Sector_Current * Vendor_Filter * Product_Filter * Symbol_ID_Current * Bloomberg_ID_Current * FIGI_ID_Current *
		Sector_Historical * Vendor_Historical * Product_Historical * Symbol_ID_Historical * Bloomberg_ID_Historical * FIGI_ID_Historical *
		Metric_Filter_Group * Vendor_Calc * Product_Calc * Metric_Calc / out = Peer4 (drop = percent);
run;

proc transpose data = Peer4 out = Peer5 (drop = _:);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Filter Product_Filter Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current 
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical 
		Metric_Filter_Group Vendor_Calc Product_Calc;
	id Metric_Calc;
	var Count;
run;

proc stdize data = Peer5 reponly missing = 0 out = Peer6;
	var _numeric_;
run;

data Peer7 Peer7_yoy (keep = Survey_ID Sector_Current Vendor_Filter Product_Filter Metric_Filter_Group Vendor_Calc Product_Calc Peer_Citations Peer_NetScore
						rename = (Survey_ID = Survey_ID_yoy Peer_Citations = Peer_Citations_yoy Peer_NetScore = Peer_NetScore_yoy));
	set Peer6;
	Peer_Citations = sum(Adoption, Increase, Flat, Decrease, Replacing);
	Peer_NetScore = (Adoption + Increase - Decrease - Replacing) / Peer_Citations;
	drop Adoption Increase Flat Decrease Replacing;

	if Survey_ID = 3 then Survey_ID_yoy = 1;
	if Survey_ID = 4 or Survey_ID = 5 then Survey_ID_yoy = 2;
	if Survey_ID >= 6 then do;
		if mod(Survey_ID, 2) = 0 then Survey_ID_yoy = Survey_ID - 3;
		if mod(Survey_ID, 2) = 1 then Survey_ID_yoy = Survey_ID - 4;
	end;
run;

/*
Year-over-Year values for each metric are merged on to calculate deltas.
Deltas are used to measure longer-term trends.
*/

proc sort data = Peer7; by Sector_Current Vendor_Filter Product_Filter Metric_Filter_Group Vendor_Calc Product_Calc Survey_ID_yoy; run;
proc sort data = Peer7_yoy; by Sector_Current Vendor_Filter Product_Filter Metric_Filter_Group Vendor_Calc Product_Calc Survey_ID_yoy; run;

data Peer8;
	merge Peer7 (in = x) Peer7_yoy;
	by Sector_Current Vendor_Filter Product_Filter Metric_Filter_Group Vendor_Calc Product_Calc Survey_ID_yoy;
	if x;
	drop Survey_ID_yoy;
run;

proc sort data = Peer8; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Filter Product_Filter Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical
		Vendor_Calc Product_Calc Metric_Filter_Group; run;

/*
Each Competitor (Vendor_Calc) is assigned an Accelerating, Decelerating, or None Net Effect within the primary vendor's (Vendor_Filter) accounts.
See Appendix C of the accompanying methodology documentation for a graphical representation of this decision tree algorithm.
*/

data Peer9;
	merge Peer8 (where = (Metric_Filter_Group = "Pos") rename = (Peer_Citations = PeerPos_Citations Peer_NetScore = PeerPos_NetScore Peer_Citations_yoy = PeerPos_Citations_yoy Peer_NetScore_yoy = PeerPos_NetScore_yoy))
		  Peer8 (where = (Metric_Filter_Group = "Neg") rename = (Peer_Citations = PeerNeg_Citations Peer_NetScore = PeerNeg_NetScore Peer_Citations_yoy = PeerNeg_Citations_yoy Peer_NetScore_yoy = PeerNeg_NetScore_yoy));
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Filter Product_Filter Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical
		Vendor_Calc Product_Calc;
	drop Metric_Filter_Group;

	if PeerPos_Citations >= 10 and PeerPos_Citations_yoy >= 10 and PeerNeg_Citations >= 10 and PeerNeg_Citations_yoy >= 10 then do;
		if PeerPos_NetScore - PeerPos_NetScore_yoy > 0 and PeerNeg_NetScore - PeerNeg_NetScore_yoy > 0 and (PeerPos_NetScore - PeerPos_NetScore_yoy >= 0.05 or PeerNeg_NetScore - PeerNeg_NetScore_yoy >= 0.05)
			then NetEffect = "Accelerating";
		if PeerPos_NetScore - PeerPos_NetScore_yoy < 0 and PeerNeg_NetScore - PeerNeg_NetScore_yoy < 0 and (PeerPos_NetScore - PeerPos_NetScore_yoy <= -0.05 or PeerNeg_NetScore - PeerNeg_NetScore_yoy <= -0.05)
			then NetEffect = "Decelerating";
	end;
run;

proc freq data = Peer9 noprint;
	tables Survey_Description_1 * Survey_ID * Survey_Launch * Survey_Close * Announcement_Date *
		Sector_Current * Vendor_Filter * Product_Filter * Symbol_ID_Current * Bloomberg_ID_Current * FIGI_ID_Current *
		Sector_Historical * Vendor_Historical * Product_Historical * Symbol_ID_Historical * Bloomberg_ID_Historical * FIGI_ID_Historical * NetEffect / out = Peer10;
run;

proc transpose data = Peer10 out = Peer11 (drop = _:);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Filter Product_Filter Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical;
	id NetEffect;
	var Count;
run;

proc stdize data = Peer11 reponly missing = 0 out = Peer12;
	var _numeric_;
run;

/*
The Peer_Rating rating is assigned based on the following decision tree algorithm.
See Appendix C of the accompanying methodology documentation for a graphical representation of this decision tree algorithm.
*/

data Peer13 (rename = (Vendor_Filter = Vendor_Current Product_Filter = Product_Current Accelerating = Peer_Accelerating Decelerating = Peer_Decelerating));
	set Peer12;
	if Accelerating - Decelerating >= 2 then Peer_Rating = "Negative";
	if Decelerating - Accelerating >= 2 then Peer_Rating = "Positive";
run;

proc sort data = Peer13; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical; run;

data Peer_Final; set Peer13; run;


/***************************************************/
/* Alignment With Major Public Cloud Vendors Theme */
/***************************************************/

/*
Two customer groups are identified: a Cloud Group and a Control Group.
The Cloud Group consists of customers who are Adopting or Increasing spend with a Public Cloud vendor (AWS, Microsoft, Google),
while the Control Group consists of all others.
Each vendor's Net Score and Citations are calculated among each of these customer groups.
*/

proc sort data = Source1; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
	Respondent_ID Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current; run;

data Cloud1;
	set Source1;
	if Sector_Current = "CLOUD COMPUTING";
	if Vendor_Current = "AWS" or Vendor_Current = "Microsoft" or Vendor_Current = "Google";
	if Metric = "ADOPTION" or Metric = "INCREASE";
run;

proc sort data = Cloud1; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Respondent_ID; run;
proc sort data = Cloud1 nodupkey out = Cloud2 (keep = Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Respondent_ID);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Respondent_ID; run;

proc freq data = Cloud2 noprint;
	tables Survey_Description_1 * Survey_ID * Survey_Launch * Survey_Close * Announcement_Date / out = Cloud_N (drop = percent rename = (Count = Cloud_N));
run;

data Cloud3;
	merge Source1 Cloud2 (in = x);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Respondent_ID;
	format Group $7.;
	if x then Group = "Cloud";
	if not x then Group = "Control";
run;

proc freq data = Cloud3 noprint;
	tables Group * Survey_Description_1 * Survey_ID * Survey_Launch * Survey_Close * Announcement_Date * Sector_Current * Vendor_Current * Product_Current * Symbol_ID_Current * Bloomberg_ID_Current * FIGI_ID_Current * 
		 Sector_Historical * Vendor_Historical * Product_Historical * Symbol_ID_Historical * Bloomberg_ID_Historical * FIGI_ID_Historical * Metric / out = Cloud4;
run;

proc transpose data = Cloud4 out = Cloud5 (drop = _:);
	by Group Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		 Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical;
	id Metric;
	var Count;
run;

proc stdize data = Cloud5 reponly missing = 0 out = Cloud6;
	var _numeric_;
run;

data Cloud7a Cloud7b;
	set Cloud6;

	Citations = sum(Adoption, Increase, Flat, Decrease, Replacing);
	NetScore = (Adoption + Increase - Decrease - Replacing) / Citations;

	if Group = "Cloud" then output Cloud7a;
	if Group = "Control" then output Cloud7b;

	drop Adoption Increase Flat Decrease Replacing Group;
run;

data Cloud8;
	merge Cloud7a (rename = (Citations = Cloud_Citations NetScore = Cloud_NetScore)) Cloud7b (rename = (Citations = Control_Citations NetScore = Control_NetScore));
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		 Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical;
run;

/*
Survey-over-Survey and Year-over-Year values for each metric are merged on to calculate deltas.
Deltas are used to measure recent inflections and longer-term trends.
*/

data Cloud9
		Cloud9_sos (keep = Survey_ID Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current Cloud_Citations Cloud_NetScore Cloud_Share
			rename = (Survey_ID = Survey_ID_sos Cloud_Citations = Cloud_Citations_sos Cloud_NetScore = Cloud_NetScore_sos Cloud_Share = Cloud_Share_sos))
		Cloud9_yoy (keep = Survey_ID Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current Cloud_Citations Cloud_NetScore Cloud_Share
			rename = (Survey_ID = Survey_ID_yoy Cloud_Citations = Cloud_Citations_yoy Cloud_NetScore = Cloud_NetScore_yoy Cloud_Share = Cloud_Share_yoy));
	merge Cloud8 Cloud_N;
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date;

	Cloud_NetScore_Delta_Control = Cloud_NetScore - Control_NetScore;
	Survey_Citations = sum(Cloud_Citations, Control_Citations);
	Cloud_Overlap = Cloud_Citations / Survey_Citations;
	Cloud_Share = Cloud_Citations / Cloud_N;

	Survey_ID_sos = Survey_ID - 1;

	if Survey_ID = 3 then Survey_ID_yoy = 1;
	if Survey_ID = 4 or Survey_ID = 5 then Survey_ID_yoy = 2;
	if Survey_ID >= 6 then do;
		if mod(Survey_ID, 2) = 0 then Survey_ID_yoy = Survey_ID - 3;
		if mod(Survey_ID, 2) = 1 then Survey_ID_yoy = Survey_ID - 4;
	end;
run;

proc sort data = Cloud9; by Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current Survey_ID_sos; run;
proc sort data = Cloud9_sos; by Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current Survey_ID_sos; run;

data Cloud10a;
	merge Cloud9 (in = x) Cloud9_sos;
	by Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current Survey_ID_sos;
	if x;
run;

proc sort data = Cloud10a; by Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current Survey_ID_yoy; run;
proc sort data = Cloud9_yoy; by Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current Survey_ID_yoy; run;

data Cloud10b;
	merge Cloud10a (in = x) Cloud9_yoy;
	by Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current Survey_ID_yoy;
	if x;

	Cloud_NetScore_Delta_sos = Cloud_NetScore - Cloud_NetScore_sos;
	Cloud_NetScore_Delta_yoy = Cloud_NetScore - Cloud_NetScore_yoy;

	drop Survey_ID_sos Survey_ID_yoy;
run;

/*
The Cloud_Rating rating is assigned based on the following decision tree algorithm.
See Appendix D of the accompanying methodology documentation for a graphical representation of this decision tree algorithm.
*/

data Cloud11;
	set Cloud10b;

	if Cloud_NetScore_sos ^= . and Cloud_NetScore_yoy ^= . then do;
		if Cloud_NetScore >= 0.70 then Cloud_Rating = "Positive";
		else if Cloud_NetScore >= 0.35 and Cloud_NetScore_Delta_sos > 0 and Cloud_NetScore_Delta_yoy > 0 and (Cloud_NetScore_Delta_sos + Cloud_NetScore_Delta_yoy) / 2 > 0.02 and Cloud_NetScore_Delta_Control > 0 then Cloud_Rating = "Positive";
		else if Cloud_NetScore >= 0.35 and Cloud_NetScore_Delta_sos > -0.05 and Cloud_NetScore_Delta_yoy > -0.05 and (Cloud_NetScore_Delta_sos + Cloud_NetScore_Delta_yoy) / 2 > -0.02 and Cloud_NetScore_Delta_Control > 0.05 and Control_Citations >= 5 then Cloud_Rating = "Positive";
		else if Cloud_NetScore_Delta_sos < 0 and Cloud_NetScore_Delta_yoy < 0 and (Cloud_NetScore_Delta_sos + Cloud_NetScore_Delta_yoy) / 2 < -0.02 and Cloud_NetScore_Delta_Control < 0 then Cloud_Rating = "Negative";
		else if Cloud_NetScore_Delta_sos < 0.05 and Cloud_NetScore_Delta_yoy < 0.05 and (Cloud_NetScore_Delta_sos + Cloud_NetScore_Delta_yoy) / 2 < 0.02 and Cloud_NetScore_Delta_Control < -0.05 and Control_Citations >= 5 then Cloud_Rating = "Negative";
		else if Cloud_NetScore <= 0.10 then Cloud_Rating = "Negative";

		if Cloud_Rating = "Positive" and (Cloud_NetScore_sos <= 0 or Cloud_NetScore_yoy <= 0) then Cloud_Rating = "";
		if Cloud_Rating = "Positive" and (Cloud_Share < Cloud_Share_sos * 0.50 or Cloud_Share < Cloud_Share_yoy * 0.50) then Cloud_Rating = "";
		if Cloud_Rating = "Negative" and (Cloud_Share > Cloud_Share_sos * 1.50 or Cloud_Share > Cloud_Share_yoy * 1.50) then Cloud_Rating = "";

		if Cloud_NetScore <= 0.10 then Cloud_Rating = "Negative";
	end;
	if Cloud_NetScore_sos ^= . and Cloud_NetScore_yoy = . then do;
		if Cloud_NetScore >= 0.70 then Cloud_Rating = "Positive";
		else if Cloud_NetScore >= 0.35 and Cloud_NetScore_Delta_sos > 0.02 and Cloud_NetScore_Delta_Control > 0.02 then Cloud_Rating = "Positive";
		else if Cloud_NetScore >= 0.35 and Cloud_NetScore_Delta_sos > -0.02 and Cloud_NetScore_Delta_Control > 0.05 and Control_Citations >= 5 then Cloud_Rating = "Positive";
		else if Cloud_NetScore_Delta_sos < -0.02 and Cloud_NetScore_Delta_Control < -0.02 then Cloud_Rating = "Negative";
		else if Cloud_NetScore_Delta_sos < 0.02 and Cloud_NetScore_Delta_Control < -0.05 and Control_Citations >= 5 then Cloud_Rating = "Negative";
		else if Cloud_NetScore <= 0.10 then Cloud_Rating = "Negative";

		if Cloud_Rating = "Positive" and Cloud_NetScore_sos <= 0 then Cloud_Rating = "";
		if Cloud_Rating = "Positive" and Cloud_Share < Cloud_Share_sos * 0.50 then Cloud_Rating = "";
		if Cloud_Rating = "Negative" and Cloud_Share > Cloud_Share_sos * 1.50 then Cloud_Rating = "";

		if Cloud_NetScore <= 0.10 then Cloud_Rating = "Negative";
	end;
	if Cloud_NetScore_sos = . and Cloud_NetScore_yoy = . then do;
		if Cloud_NetScore >= 0.70 then Cloud_Rating = "Positive";
		else if Cloud_NetScore <= 0.10 then Cloud_Rating = "Negative";
	end;

	if Survey_Citations < 30 or Cloud_Citations < 5 or Cloud_Overlap < 0.4 then Cloud_Rating = "";

	if Sector_Current = "CLOUD COMPUTING" and (Vendor_Current = "AWS" or Vendor_Current = "Microsoft" or Vendor_Current = "Google")
		then Cloud_Rating = "Positive";

	drop Cloud_Citations_sos Cloud_NetScore_sos Cloud_Citations_yoy Cloud_NetScore_yoy Control_Citations;
run;

proc sort data = Cloud11; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical; run;

data Cloud_Final; set Cloud11; run;


/******************************************/
/* Model Creation and Performance Testing */
/******************************************/

/*
Metrics from all themes are merged together.
Stock Price returns are merged on.
Stock Price return survey averages and z-scores are calculated to determine outperformers and underperformers.
See Appendix E of the accompanying methodology documentation for a graphical representation of this process.

Please note, only data from 2015 and on is used for the remainder of this program.
The time difference between the start of the Source Dataset (2010) and the remainder of Insight Dataset (2015) is primarily due to two factors:
[1] a shift in technology spend that occurred during that time frame from a largely CapEx model to a mix between CapEx and OpEx; and
[2] ETRs sample of respondents (i.e., the number of CIOs and IT Decision Makers participating in ETRs ecosystem and taking our surveys) approximately doubled between 2010 and 2015.
*/

proc sort data = Spend_Final; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
	Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
	Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical; run;
proc sort data = Peer_Final; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
	Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
	Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical; run;
proc sort data = Cloud_Final; by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
	Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
	Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical; run;

data Returns1;
	merge Spend_Final Peer_Final Cloud_Final (drop = Cloud_N Survey_Citations);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical;

	if Survey_ID >= 18;
	if Citations >= 30;
run;

proc sort data = Returns1; by Survey_ID Bloomberg_ID_Historical; run;
proc sort data = SPReturns; by Survey_ID Bloomberg_ID_Historical; run;


data Returns2;
	merge Returns1 (in = x) SPReturns (in = y keep = Survey_ID Bloomberg_ID_Historical Return_End);
	by Survey_ID Bloomberg_ID_Historical;
	if x;
run;

proc means data = Returns2 noprint;
	by Survey_ID;
	var Return_End;
	output out = Returns2_Means (drop = _:) mean = std = / autoname;
run;

data Returns3;
	merge Returns2 Returns2_Means;
	by Survey_ID;

	Return_EndZ = (Return_End - Return_End_Mean) / Return_End_StdDev;
	if Return_EndZ ^= . and Return_EndZ >= 0.253 then PosNeg_EndZ = "Positive";
	if Return_EndZ ^= . and Return_EndZ <= -0.253 then PosNeg_EndZ = "Negative";
run;

/*
The logistic regression model is trained to model the probability of outperformance.
The original dataset is passed through the trained model to determine the model's historical performance.
*/

proc logistic data = Returns3 (where = (Survey_ID < &Survey_Max.)) outmodel = InsightModel;
	class Adoption_Rating (missing ref = "") Increase_Rating (missing ref = "") Decrease_Rating (missing ref = "") Replacing_Rating (missing ref = "")
		NetScore_Rating (missing ref = "") MarketShare_Rating (missing ref = "") Peer_Rating (missing ref = "") Cloud_Rating (missing ref = "");
	model PosNeg_EndZ (Event = 'Positive') = Adoption_Rating Increase_Rating Decrease_Rating Replacing_Rating NetScore_Rating MarketShare_Rating Peer_Rating Cloud_Rating
		/ selection = backward slstay = 0.2 details;
	output out = InsightModel_Details;
run;

proc logistic inmodel = InsightModel;
	score data = Returns3 out = Forecast1;
run;

/*
Vendors across multiple sectors or products are combined using a citation-weighted average to determine the vendor's overall probability of outperformance.
Only vendors above a certain threshold are assigned a Positive/Negative forecast.
*/

proc sort data = Forecast1; by Survey_ID Vendor_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical; run;

proc means data = Forecast1 noprint vardef = weight;
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Vendor_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical;
	var P_Positive;
	weight Citations;
	output out = Forecast2 (drop = _:) mean = / autoname;
run;

proc sort data = Forecast2; by Survey_ID Bloomberg_ID_Historical; run;
proc sort data = SPReturns; by Survey_ID Bloomberg_ID_Historical; run;

data Forecast3;
	merge Forecast2 (in = x) SPReturns (keep = Survey_ID Bloomberg_ID_Historical Window_Start Window_End Return_End);
	by Survey_ID Bloomberg_ID_Historical;
	if x;
	if P_Positive_Mean ^= .;
	if P_Positive_Mean >= 0.55 then Insight_Forecast = "Positive";
	if P_Positive_Mean <= 0.45 then Insight_Forecast = "Negative";
	if Insight_Forecast ^= "";
	if Bloomberg_ID_Historical ^= "";
run;

/*
Historical returns are calculated to measure model performance.
*/

proc sort data = Forecast3; by Survey_ID Insight_Forecast Bloomberg_ID_Historical; run;

proc means data = Forecast3 noprint;
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Window_Start Window_End Insight_Forecast;
	var Return_End;
	output out = Forecast4 (rename = (_freq_ = Vendor_Count)) mean = / autoname;
run;

proc transpose data = Forecast4 out = Forecast5a (drop = _:);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Window_Start Window_End;
	id Insight_Forecast;
	var Return_End_Mean;
run;
proc transpose data = Forecast4 out = Forecast5b (drop = _:);
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Window_Start Window_End;
	id Insight_Forecast;
	var Vendor_Count;
run;

data Forecast6;
	merge Forecast5a (rename = (Positive = Positive_Returns Negative = Negative_Returns))
		Forecast5b (rename = (Positive = Positive_VendorCount Negative = Negative_VendorCount));
	by Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Window_Start Window_End;
run;

proc sort data = Forecast6; by Survey_ID; run;
proc sort data = FTECReturns; by Survey_ID; run;




data Forecast7;
	merge Forecast6 (in = x) FTECReturns (keep = Survey_ID Benchmark_Fidelity_MSCI_IT_ETF);
	by Survey_ID;
	if x;


	if Announcement_Date >= today() then delete;

	Index_Type = "Single Insight Dataset";
	Index_Window = catx("-",put(Window_Start,mmddyy.),put(Window_End,mmddyy.));

	Window_Year = Year(Window_Start);
	Positive_Returns_log = log(Positive_Returns + 1);
	Negative_Returns_log = log(Negative_Returns + 1);
	Benchmark_Return_log = log(Benchmark_Fidelity_MSCI_IT_ETF + 1);
run;

proc means data = Forecast7 noprint;
	by Window_Year;
	var Positive_Returns_log Negative_Returns_log Benchmark_Return_log;
	output out = Forecast7b (drop = _:) sum = / autoname;
run;
proc means data = Forecast7 noprint;
	by Window_Year;
	var Window_Start Window_End;
	output out = Window7b (drop = _:) min = max = / autoname;
run;
proc means data = Forecast7 noprint;
	var Positive_Returns_log Negative_Returns_log Benchmark_Return_log;
	output out = Forecast7c (drop = _:) sum = / autoname;
run;
proc means data = Forecast7 noprint;
	var Window_Start Window_End;
	output out = Window7c (drop = _:) min = max = / autoname;
run;

data Forecast8b;
	merge Forecast7b Window7b;
	by Window_Year;
	Index_Type = "Four Consecutive Insight Datasets";
	Index_Window = catx("-",put(Window_Start_Min,mmddyy.),put(Window_End_Max,mmddyy.));
	Positive_Returns_Consecutive = exp(Positive_Returns_log_sum) - 1;
	Negative_Returns_Consecutive = exp(Negative_Returns_log_sum) - 1;
	Benchmark_Return_Consecutive = exp(Benchmark_Return_log_sum) - 1;
	
run;
data Forecast8c;
	merge Forecast7c Window7c;
	Index_Type = "Cumulative Insight Dataset";
	Index_Window = catx("-",put(Window_Start_Min,mmddyy.),put(Window_End_Max,mmddyy.));
	Positive_Returns_Cumulative = exp(Positive_Returns_log_sum) - 1;
	Negative_Returns_Cumulative = exp(Negative_Returns_log_sum) - 1;
	Benchmark_Return_Cumulative = exp(Benchmark_Return_log_sum) - 1;

run;

data Forecast9;
	format Index_Type $35.;
	set Forecast7 (keep = Index_Type Index_Window Positive_Returns Negative_Returns Benchmark_Fidelity_MSCI_IT_ETF)
		Forecast8b (keep = Index_Type Index_Window Positive_Returns_Consecutive Negative_Returns_Consecutive Benchmark_Return_Consecutive
			rename = (Positive_Returns_Consecutive = Positive_Returns Negative_Returns_Consecutive = Negative_Returns Benchmark_Return_Consecutive = Benchmark_Fidelity_MSCI_IT_ETF))
		Forecast8c (keep = Index_Type Index_Window Positive_Returns_Cumulative Negative_Returns_Cumulative Benchmark_Return_Cumulative
			rename = (Positive_Returns_Cumulative = Positive_Returns Negative_Returns_Cumulative = Negative_Returns Benchmark_Return_Cumulative = Benchmark_Fidelity_MSCI_IT_ETF));
run;


/******************/
/* Final Datasets */
/******************/

proc sort data = Returns1; by Survey_ID Sector_Current Vendor_Current Product_Current; run;

data InsightData_Final;
	retain Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date
		Sector_Current Vendor_Current Product_Current Symbol_ID_Current Bloomberg_ID_Current FIGI_ID_Current
		Sector_Historical Vendor_Historical Product_Historical Symbol_ID_Historical Bloomberg_ID_Historical FIGI_ID_Historical Citations
		AdoptionP_Value AdoptionP_Value_SurveyZ AdoptionP_Delta_sos AdoptionP_Delta_sos_SurveyZ AdoptionP_Delta_yoy AdoptionP_Delta_yoy_SurveyZ Adoption_Rating
		IncreaseP_Value IncreaseP_Value_SurveyZ IncreaseP_Delta_sos IncreaseP_Delta_sos_SurveyZ IncreaseP_Delta_yoy IncreaseP_Delta_yoy_SurveyZ Increase_Rating
		FlatP_Value FlatP_Value_SurveyZ FlatP_Delta_sos FlatP_Delta_sos_SurveyZ FlatP_Delta_yoy FlatP_Delta_yoy_SurveyZ
		DecreaseP_Value DecreaseP_Value_SurveyZ DecreaseP_Delta_sos DecreaseP_Delta_sos_SurveyZ DecreaseP_Delta_yoy DecreaseP_Delta_yoy_SurveyZ Decrease_Rating
		ReplacingP_Value ReplacingP_Value_SurveyZ ReplacingP_Delta_sos ReplacingP_Delta_sos_SurveyZ ReplacingP_Delta_yoy ReplacingP_Delta_yoy_SurveyZ Replacing_Rating
		NetScore_Value NetScore_Value_SurveyZ NetScore_Delta_sos NetScore_Delta_sos_SurveyZ NetScore_Delta_yoy NetScore_Delta_yoy_SurveyZ NetScore_Rating
		MarketShare_Value MarketShare_Delta_sos MarketShare_Delta_sos_SurveyZ MarketShare_Delta_yoy MarketShare_Delta_yoy_SurveyZ MarketShare_Rating
		Peer_Accelerating Peer_Decelerating Peer_Rating
		Cloud_Citations Cloud_NetScore Cloud_Share Cloud_Share_sos Cloud_NetScore_Delta_sos
		Cloud_Share_yoy Cloud_NetScore_Delta_yoy Control_NetScore Cloud_Overlap Cloud_NetScore_Delta_Control Cloud_Rating;


	set Returns1;
run;

proc sort data = Forecast3; by Survey_ID Insight_Forecast Vendor_Historical; run;
data InsightForecast_Final;
	retain Survey_Description_1 Survey_ID Survey_Launch Survey_Close Announcement_Date Insight_Forecast
		Vendor_Historical Bloomberg_ID_Historical Symbol_ID_Historical FIGI_ID_Historical;
	set Forecast3;
	drop P_Positive_Mean Return_End Window_Start Window_End;
run;

data InsightPerformance_Final;
	retain Index_Type Index_Window Positive_Returns Negative_Returns Benchmark_Fidelity_MSCI_IT_ETF;
	set Forecast9;
run;




proc export data = InsightData_Final
	outfile = "OUTFILE PATH"
	dbms = csv replace;
run;
	proc export data = InsightForecast_Final
		outfile = "OUTFILE PATH"
		dbms = csv replace;
	run;
	proc export data = InsightPerformance_Final
		outfile = "OUTFILE PATH"
		dbms = csv replace;

