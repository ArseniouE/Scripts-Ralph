--==========================================
--ALTER TABLE-------------------------------
--==========================================
ALTER TABLE olapts.abfactentity
ADD COLUMN IF NOT EXISTS internationalsyndicationloan boolean;

--=================================================
--CREATE BACKUP OF OLD abfactentity----------------
--=================================================
create table olapts.abfactentity_backup as
select * from olapts.abfactentity;

--=================================================
------CHECK DATA-----------------------------------
--=================================================
--select * from olapts.abfactentity_backup limit 100

--=================================================
-------DROP OLD abfactentity-----------------------
--=================================================

--***uncomment row 23 after the backup of table***
--DROP TABLE IF EXISTS olapts.abfactentity;

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
    jurisdiction text COLLATE pg_catalog."default",
    bankruptcy text COLLATE pg_catalog."default",
    governmentbailoutoffirm text COLLATE pg_catalog."default",
    industrysector text COLLATE pg_catalog."default",
    internationalsyndicationloan text COLLATE pg_catalog."default",
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

WITH (
    parallel_workers = 16
)
TABLESPACE olap_data;

ALTER TABLE olapts.abfactentity
    OWNER to olap;

GRANT SELECT ON TABLE olapts.abfactentity TO c23568;

GRANT ALL ON TABLE olapts.abfactentity TO olap;

GRANT SELECT ON TABLE olapts.abfactentity TO db_reader;
-- Index: abfactentity_idx_btree

-- DROP INDEX olapts.abfactentity_idx_btree;

CREATE INDEX abfactentity_idx_btree
    ON olapts.abfactentity USING btree
    (pkid_ COLLATE pg_catalog."default" ASC NULLS LAST, entityid COLLATE pg_catalog."default" ASC NULLS LAST, versionid_ ASC NULLS LAST, sourcepopulateddate_ ASC NULLS LAST)
    INCLUDE(gc18, cdicode, systemid, isvalid_, isdeleted_, isvisible_, islatestversion_, createdby_, createddate_, updatedby_, updateddate_, sourcepopulatedby_, wfid_)
    TABLESPACE olap_data;
-- Index: abfactentity_idx_entity_hash

-- DROP INDEX olapts.abfactentity_idx_entity_hash;

CREATE INDEX abfactentity_idx_entity_hash
    ON olapts.abfactentity USING hash
    (entityid COLLATE pg_catalog."default")
    TABLESPACE olap_data;
-- Index: abfactentity_idx_pkid_hash

-- DROP INDEX olapts.abfactentity_idx_pkid_hash;

CREATE INDEX abfactentity_idx_pkid_hash
    ON olapts.abfactentity USING hash
    (pkid_ COLLATE pg_catalog."default")
    TABLESPACE olap_data;
-- Index: abfactentity_idx_versionid_hash

-- DROP INDEX olapts.abfactentity_idx_versionid_hash;

CREATE INDEX abfactentity_idx_versionid_hash
    ON olapts.abfactentity USING hash
    (versionid_)
    TABLESPACE olap_data;
-- Index: abfactentity_idxdate_brin

-- DROP INDEX olapts.abfactentity_idxdate_brin;

CREATE INDEX abfactentity_idxdate_brin
    ON olapts.abfactentity USING brin
    (sourcepopulateddate_)
    TABLESPACE olap_data;

--=============================================================================
--INSERT INTO olapts.abfactentity data from olapts.abfactentity_backup
--=============================================================================
INSERT INTO olapts.abfactentity(
	factentityid_, pkid_, gc12, gc13, gc16, gc17, gc18, gc19, gc22, gc23, gc100, gc108, gc109, gc110, gc111, gc112, gc113, 
    gc114, gc115, gc116, gc117, taxid, gender, idtype, onlist, cdicode, groupid, currency, division, entityid, firmtype, idnumber, 
    islocked, lockedby, longname, prospect, systemid, shortname, stockcode, moduleid_, currentemp, entitytype, lockeddate, occupation, 
    reviewtype, salutation, dateofbirth, designation, guarantorid, legalentity, listingdate, yearstarted, businesstype, countryofinc, 
    countryofres, creditnumber, descriptions, industrycode, issuecountry, monthstarted, nationality1, nationality2, nationality3, 
    countryofrisk, customersince, stockexchange, yearinservice, connectedparty, lastreviewdate, nextreviewdate, placeoflisting, 
    corporationtype, creditcommittee, creditportfolio, nameofguarantor, restrictedusers, schedreviewdate, countryoflisting, 
    relationshiptype, restrictedentity, businessportfolio, establishmentdate, indclassification, permanentresident, responsibleoffice, 
    reviewedfrequency, registrationnumber, responsibleofficer, externaldatasources, placeofincorporation, primarycreditofficer, 
    multiplenationalities, primarybankingofficer, sourcesystemidentifier, consolidatedbalancesheet, provincestateofincorporation,
    enterprisevaluetototaldebt, expirydate, valuationmethodology, jurisdiction, bankruptcy, governmentbailoutoffirm, industrysector, 
    internationalsyndicationloan, wfid_, taskid_, snapshotid_, contextuserid_, createdby_, createddate_, updatedby_, updateddate_, 
    isvalid_, baseversionid_, versionid_, isdeleted_, isvisible_, islatestversion_, t_, sourcepopulatedby_, sourcepopulateddate_
)
select
	factentityid_, pkid_, gc12, gc13, gc16, gc17, gc18, gc19, gc22, gc23, gc100, gc108, gc109, gc110, gc111, gc112, gc113, 
    gc114, gc115, gc116, gc117, taxid, gender, idtype, onlist, cdicode, groupid, currency, division, entityid, firmtype, idnumber, 
    islocked, lockedby, longname, prospect, systemid, shortname, stockcode, moduleid_, currentemp, entitytype, lockeddate, occupation, 
    reviewtype, salutation, dateofbirth, designation, guarantorid, legalentity, listingdate, yearstarted, businesstype, countryofinc, 
    countryofres, creditnumber, descriptions, industrycode, issuecountry, monthstarted, nationality1, nationality2, nationality3, 
    countryofrisk, customersince, stockexchange, yearinservice, connectedparty, lastreviewdate, nextreviewdate, placeoflisting, 
    corporationtype, creditcommittee, creditportfolio, nameofguarantor, restrictedusers, schedreviewdate, countryoflisting, 
    relationshiptype, restrictedentity, businessportfolio, establishmentdate, indclassification, permanentresident, responsibleoffice, 
    reviewedfrequency, registrationnumber, responsibleofficer, externaldatasources, placeofincorporation, primarycreditofficer, 
    multiplenationalities, primarybankingofficer, sourcesystemidentifier, consolidatedbalancesheet, provincestateofincorporation,
    enterprisevaluetototaldebt, expirydate, valuationmethodology, jurisdiction, bankruptcy, governmentbailoutoffirm, industrysector, 
    internationalsyndicationloan, wfid_, taskid_, snapshotid_, contextuserid_, createdby_, createddate_, updatedby_, updateddate_, 
    isvalid_, baseversionid_, versionid_, isdeleted_, isvisible_, islatestversion_, t_, sourcepopulatedby_, sourcepopulateddate_
from olapts.abfactentity_backup;

	
	--=================================================
	--CHECK DATA--------------------------------
	--=================================================
	--select * from olapts.abfactentity;
	
	--=================================================
	--DROP BACKUP TABLE--------------------------------
	--=================================================
	DROP TABLE olapts.abfactentity_backup;
	
	--=================================================
	--Run Ralph Script--------------------------------
	--=================================================
	
