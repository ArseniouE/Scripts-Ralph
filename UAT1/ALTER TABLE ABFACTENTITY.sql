--==========================================
--ALTER TABLE-------------------------------
--==========================================
	
ALTER TABLE olapts.abfactentity
ADD COLUMN arrearunit text,
ADD	COLUMN arrearunitval text,
ADD COLUMN customercategory text,
ADD	COLUMN customercategoryval text,
ADD COLUMN delaydaysods integer, 
ADD	COLUMN delaysoverbalancesratioOds numeric,
ADD	COLUMN epidikosleasing text,
ADD	COLUMN epidikosleasingval text,
ADD	COLUMN idnumberods text,
ADD	COLUMN industrypeersector text,
ADD	COLUMN iscustabcfactors boolean,
ADD	COLUMN iscustleasing text,
ADD	COLUMN iscustleasingval text, 
ADD	COLUMN iscustlux text, 
ADD	COLUMN iscustluxval text, 
ADD COLUMN isnpl text,
ADD	COLUMN isnplval text,
ADD COLUMN isvisible text,
ADD	COLUMN isvisibleval text,
ADD	COLUMN iswholesale text,
ADD	COLUMN iswholesaleval text,
ADD	COLUMN legalentitytype text,
ADD	COLUMN legalentitytypeval text,
ADD	COLUMN marker integer,
ADD	COLUMN monitoringunit text,
ADD	COLUMN monitoringunitval text,
ADD	COLUMN nplchanged text,
ADD	COLUMN nplchangedval text,
ADD	COLUMN odsinsertdate text,
ADD	COLUMN odsisupdated boolean,
ADD	COLUMN odsupdatedate text,
ADD	COLUMN offbalancesdwh numeric,
ADD	COLUMN onbalancesdwh numeric,
ADD	COLUMN totalbalanceods numeric,
ADD	COLUMN totaldelaysods numeric,
ADD	COLUMN totalexposuresdwh numeric,
ADD	COLUMN internationalsyndicationloan boolean,
ADD     COLUMN jurisdiction text;	

--=================================================
--CREATE BACKUP OF OLD ABFACTENTITY----------------
--=================================================
create table olapts.abfactentity_backup as
select * from olapts.abfactentity


--=================================================
------CHECK DATA-----------------------------------
--=================================================
select * from olapts.abfactentity_backup limit 100


--=================================================
-------DROP OLD ABFACTENTITY-----------------------
--=================================================
DROP TABLE IF EXISTS olapts.abfactentity;

--=================================================
--CREATE NEW abfactentity--------------------------
--=================================================
CREATE TABLE IF NOT EXISTS olapts.abfactentity
(
    factentityid_ character varying COLLATE pg_catalog."default",
    pkid_ character varying COLLATE pg_catalog."default",
    gc12 text COLLATE pg_catalog."default",
    gc13 text COLLATE pg_catalog."default",
    gc16 text COLLATE pg_catalog."default",
    gc17 text COLLATE pg_catalog."default",
    gc18 text COLLATE pg_catalog."default",
    gc19 text COLLATE pg_catalog."default",
    gc22 text COLLATE pg_catalog."default",
    gc23 text COLLATE pg_catalog."default",
    gc100 text COLLATE pg_catalog."default",
    gc108 text COLLATE pg_catalog."default",
    gc109 text COLLATE pg_catalog."default",
    gc110 text COLLATE pg_catalog."default",
    gc111 text COLLATE pg_catalog."default",
    gc112 text COLLATE pg_catalog."default",
    gc113 text COLLATE pg_catalog."default",
    gc114 text COLLATE pg_catalog."default",
    gc115 text COLLATE pg_catalog."default",
    gc116 text COLLATE pg_catalog."default",
    gc117 text COLLATE pg_catalog."default",
    taxid text COLLATE pg_catalog."default",
    gender text COLLATE pg_catalog."default",
    idtype text COLLATE pg_catalog."default",
    onlist text COLLATE pg_catalog."default",
    cdicode text COLLATE pg_catalog."default",
    groupid text COLLATE pg_catalog."default",
    currency text COLLATE pg_catalog."default",
    division text COLLATE pg_catalog."default",
    entityid text COLLATE pg_catalog."default",
    firmtype text COLLATE pg_catalog."default",
    idnumber text COLLATE pg_catalog."default",
    islocked text COLLATE pg_catalog."default",
    lockedby text COLLATE pg_catalog."default",
    longname text COLLATE pg_catalog."default",
    prospect text COLLATE pg_catalog."default",
    systemid text COLLATE pg_catalog."default",
    shortname text COLLATE pg_catalog."default",
    stockcode text COLLATE pg_catalog."default",
    moduleid_ text COLLATE pg_catalog."default",
    currentemp text COLLATE pg_catalog."default",
    entitytype text COLLATE pg_catalog."default",
    lockeddate text COLLATE pg_catalog."default",
    occupation text COLLATE pg_catalog."default",
    reviewtype text COLLATE pg_catalog."default",
    salutation text COLLATE pg_catalog."default",
    dateofbirth text COLLATE pg_catalog."default",
    designation text COLLATE pg_catalog."default",
    guarantorid text COLLATE pg_catalog."default",
    legalentity text COLLATE pg_catalog."default",
    listingdate text COLLATE pg_catalog."default",
    yearstarted text COLLATE pg_catalog."default",
    businesstype text COLLATE pg_catalog."default",
    countryofinc text COLLATE pg_catalog."default",
    countryofres text COLLATE pg_catalog."default",
    creditnumber text COLLATE pg_catalog."default",
    descriptions text COLLATE pg_catalog."default",
    industrycode text COLLATE pg_catalog."default",
    issuecountry text COLLATE pg_catalog."default",
    monthstarted text COLLATE pg_catalog."default",
    nationality1 text COLLATE pg_catalog."default",
    nationality2 text COLLATE pg_catalog."default",
    nationality3 text COLLATE pg_catalog."default",
    countryofrisk text COLLATE pg_catalog."default",
    customersince text COLLATE pg_catalog."default",
    stockexchange text COLLATE pg_catalog."default",
    yearinservice text COLLATE pg_catalog."default",
    connectedparty text COLLATE pg_catalog."default",
    lastreviewdate text COLLATE pg_catalog."default",
    nextreviewdate text COLLATE pg_catalog."default",
    placeoflisting text COLLATE pg_catalog."default",
    corporationtype text COLLATE pg_catalog."default",
    creditcommittee text COLLATE pg_catalog."default",
    creditportfolio text COLLATE pg_catalog."default",
    nameofguarantor text COLLATE pg_catalog."default",
    restrictedusers text COLLATE pg_catalog."default",
    schedreviewdate text COLLATE pg_catalog."default",
    countryoflisting text COLLATE pg_catalog."default",
    relationshiptype text COLLATE pg_catalog."default",
    restrictedentity text COLLATE pg_catalog."default",
    businessportfolio text COLLATE pg_catalog."default",
    establishmentdate text COLLATE pg_catalog."default",
    indclassification text COLLATE pg_catalog."default",
    permanentresident text COLLATE pg_catalog."default",
    responsibleoffice text COLLATE pg_catalog."default",
    reviewedfrequency text COLLATE pg_catalog."default",
    registrationnumber text COLLATE pg_catalog."default",
    responsibleofficer text COLLATE pg_catalog."default",
    externaldatasources text COLLATE pg_catalog."default",
    placeofincorporation text COLLATE pg_catalog."default",
    primarycreditofficer text COLLATE pg_catalog."default",
    multiplenationalities text COLLATE pg_catalog."default",
    primarybankingofficer text COLLATE pg_catalog."default",
    sourcesystemidentifier text COLLATE pg_catalog."default",
    consolidatedbalancesheet text COLLATE pg_catalog."default",
    provincestateofincorporation text COLLATE pg_catalog."default",
    enterprisevaluetototaldebt text COLLATE pg_catalog."default",
    expirydate text COLLATE pg_catalog."default",
    valuationmethodology text COLLATE pg_catalog."default",
    jurisdiction text COLLATE pg_catalog."default",--add column 2024-12-02	
    bankruptcy text COLLATE pg_catalog."default",
    governmentbailoutoffirm text COLLATE pg_catalog."default",
    industrysector text COLLATE pg_catalog."default",
	--
    arrearunit text COLLATE pg_catalog."default",
    arrearunitval text COLLATE pg_catalog."default",
    customercategory text COLLATE pg_catalog."default",
    customercategoryval text COLLATE pg_catalog."default",
    delaydaysods integer,
    delaysoverbalancesratioods numeric,
    epidikosleasing text COLLATE pg_catalog."default",
    epidikosleasingval text COLLATE pg_catalog."default",
    idnumberods text COLLATE pg_catalog."default",
    industrypeersector text COLLATE pg_catalog."default",
    iscustabcfactors boolean,
    iscustleasing text COLLATE pg_catalog."default",
    iscustleasingval text COLLATE pg_catalog."default",
    iscustlux text COLLATE pg_catalog."default",
    iscustluxval text COLLATE pg_catalog."default",
    isnpl text COLLATE pg_catalog."default",
    isnplval text COLLATE pg_catalog."default",
    isvisible text COLLATE pg_catalog."default",
    isvisibleval text COLLATE pg_catalog."default",
    iswholesale text COLLATE pg_catalog."default",
    iswholesaleval text COLLATE pg_catalog."default",
    legalentitytype text COLLATE pg_catalog."default",
    legalentitytypeval text COLLATE pg_catalog."default",
    marker integer,
    monitoringunit text COLLATE pg_catalog."default",
    monitoringunitval text COLLATE pg_catalog."default",
    nplchanged text COLLATE pg_catalog."default",
    nplchangedval text COLLATE pg_catalog."default",
    odsinsertdate text COLLATE pg_catalog."default",
    odsisupdated boolean,
    odsupdatedate text COLLATE pg_catalog."default",
    offbalancesdwh numeric,
    onbalancesdwh numeric,
    totalbalanceods numeric,
    totaldelaysods numeric,
    totalexposuresdwh numeric,
	internationalsyndicationloan boolean,
	--
    wfid_ character varying COLLATE pg_catalog."default",
    taskid_ character varying COLLATE pg_catalog."default",
    snapshotid_ integer,
    contextuserid_ character varying COLLATE pg_catalog."default",
    createdby_ character varying COLLATE pg_catalog."default",
    createddate_ timestamp without time zone,
    updatedby_ character varying COLLATE pg_catalog."default",
    updateddate_ timestamp without time zone,
    isvalid_ boolean,
    baseversionid_ integer,
    versionid_ integer,
    isdeleted_ boolean,
    isvisible_ boolean,
    islatestversion_ boolean,
    t_ character varying COLLATE pg_catalog."default",
    sourcepopulatedby_ character varying COLLATE pg_catalog."default",
    sourcepopulateddate_ timestamp without time zone
)

--=============================================================================
--INSERT INTO olapts.abfactentity data from olapts.abfactentity_backup
--=============================================================================

INSERT INTO olapts.abfactentity(
	factentityid_, 
	pkid_, 
	gc12, 
	gc13, 
	gc16, 
	gc17, 
	gc18, 
	gc19, 
	gc22, 
	gc23, 
	gc100, 
	gc108, 
	gc109, 
	gc110, 
	gc111, 
	gc112, 
	gc113, 
	gc114, 
	gc115, 
	gc116, 
	gc117, 
	taxid, 
	gender, 
	idtype, 
	onlist, 
	cdicode, 
	groupid,
	currency, 
	division,
	entityid, 
	firmtype, 
	idnumber, 
	islocked, 
	lockedby, 
	longname, 
	prospect, 
	systemid, 
	shortname, 
	stockcode, 
	moduleid_, 
	currentemp, 
	entitytype, 
	lockeddate, 
	occupation, 
	reviewtype, 
	salutation, 
	dateofbirth, 
	designation, 
	guarantorid, 
	legalentity, 
	listingdate, 
	yearstarted, 
	businesstype, 
	countryofinc, 
	countryofres, 
	creditnumber, 
	descriptions, 
	industrycode, 
	issuecountry,
	monthstarted, 
	nationality1, 
	nationality2, 
	nationality3, 
	countryofrisk, 
	customersince, 
	stockexchange, 
	yearinservice, 
	connectedparty, 
	lastreviewdate,
	nextreviewdate,
	placeoflisting, 
	corporationtype, 
	creditcommittee, 
	creditportfolio, 
	nameofguarantor, 
	restrictedusers, 
	schedreviewdate,
	countryoflisting, 
	relationshiptype, 
	restrictedentity, 
	businessportfolio, 
	establishmentdate, 
	indclassification, 
	permanentresident, 
	responsibleoffice, 
	reviewedfrequency, 
	registrationnumber,
	responsibleofficer, 
	externaldatasources, 
	placeofincorporation, 
	primarycreditofficer,
	multiplenationalities, 
	primarybankingofficer, 
	sourcesystemidentifier,
	consolidatedbalancesheet, 
	provincestateofincorporation, 
	enterprisevaluetototaldebt, 
	expirydate, 
	valuationmethodology, 
	jurisdiction,--added 2024-12-02	
	bankruptcy,
	governmentbailoutoffirm, 
	industrysector, 
	----------------
	 --new fields included
	arrearunit, 
	arrearunitval, 
	customercategory, 
	customercategoryval, 
	delaydaysods, 
	delaysoverbalancesratioods, 
	epidikosleasing,
	epidikosleasingval, 
	idnumberods,
	industrypeersector, 
	iscustabcfactors,
	iscustleasing, 
	iscustleasingval,
	iscustlux,
	iscustluxval, 
	isnpl, 
	isnplval,
	isvisible, 
	isvisibleval, 
	iswholesale, 
	iswholesaleval,
	legalentitytype,
	legalentitytypeval,
	marker, 
	monitoringunit,
	monitoringunitval,
	nplchanged, 
	nplchangedval, 
	odsinsertdate, 
	odsisupdated, 
	odsupdatedate, 
	offbalancesdwh, 
	onbalancesdwh, 
	totalbalanceods, 
	totaldelaysods,
	totalexposuresdwh,
	internationalsyndicationloan,
	---------
	wfid_, 
	taskid_,
	snapshotid_, 
	contextuserid_, 
	createdby_, 
	createddate_, 
	updatedby_, 
	updateddate_,
	isvalid_, 
	baseversionid_,
	versionid_, 
	isdeleted_, 
	isvisible_, 
	islatestversion_,
	t_, 
	sourcepopulatedby_,
	sourcepopulateddate_)
	
	select 
	factentityid_, pkid_, gc12, gc13, gc16, gc17, gc18, gc19, gc22, gc23, gc100, gc108, gc109, gc110, gc111, gc112, gc113, gc114, gc115, gc116, gc117, taxid, 
	gender, idtype, onlist, cdicode, groupid, currency, division, entityid, firmtype, idnumber, islocked, lockedby, longname, prospect, systemid, shortname, 
	stockcode, moduleid_, currentemp, entitytype, lockeddate, occupation, reviewtype, salutation, dateofbirth, designation, guarantorid, legalentity, listingdate,
	yearstarted, businesstype, countryofinc, countryofres, creditnumber, descriptions, industrycode, issuecountry, monthstarted, nationality1, nationality2, nationality3,
	countryofrisk, customersince, stockexchange, yearinservice, connectedparty, lastreviewdate, nextreviewdate, placeoflisting, corporationtype, creditcommittee,
	creditportfolio, nameofguarantor, restrictedusers, schedreviewdate, countryoflisting, relationshiptype, restrictedentity, businessportfolio, establishmentdate, 
	indclassification, permanentresident, responsibleoffice, reviewedfrequency, registrationnumber, responsibleofficer, externaldatasources, placeofincorporation, 
	primarycreditofficer, multiplenationalities, primarybankingofficer, sourcesystemidentifier, consolidatedbalancesheet, provincestateofincorporation, 
	enterprisevaluetototaldebt, expirydate, valuationmethodology, jurisdiction, bankruptcy, governmentbailoutoffirm,
	industrysector, 
	--
	arrearunit, arrearunitval, customercategory, customercategoryval, delaydaysods, delaysoverbalancesratioods, epidikosleasing, epidikosleasingval, idnumberods, industrypeersector, iscustabcfactors, iscustleasing, iscustleasingval, iscustlux, iscustluxval, isnpl, isnplval, isvisible, isvisibleval, iswholesale, iswholesaleval, legalentitytype, legalentitytypeval, marker, monitoringunit, monitoringunitval, nplchanged, nplchangedval, odsinsertdate, odsisupdated, odsupdatedate, offbalancesdwh, onbalancesdwh, totalbalanceods, totaldelaysods, totalexposuresdwh,internationalsyndicationloan, 
	--
	wfid_, taskid_, snapshotid_, contextuserid_, createdby_, createddate_, updatedby_, updateddate_, isvalid_, baseversionid_, versionid_, isdeleted_, isvisible_, islatestversion_, t_, sourcepopulatedby_, sourcepopulateddate_
	from olapts.abfactentity_backup
	
	
	--=================================================
	--CHECK DATA--------------------------------
	--=================================================
	--select * from olapts.abfactentity
	
	--=================================================
	--DROP BACKUP TABLE--------------------------------
	--=================================================
	DROP TABLE olapts.abfactentity_backup
	
	--=================================================
	--Run Ralph Script--------------------------------
	--=================================================
	

