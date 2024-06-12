--==========================================
--            ALTER TABLE ABUTP           --
--==========================================

ALTER TABLE olapts.abutp
ADD COLUMN IF NOT EXISTS lawsuitid boolean, 
ADD COLUMN IF NOT EXISTS loanaccelaratedid boolean, 
ADD COLUMN IF NOT EXISTS materialid boolean,
ADD COLUMN IF NOT EXISTS provisionid boolean,
ADD COLUMN IF NOT EXISTS restructuringid boolean,
ADD COLUMN IF NOT EXISTS ebaid boolean;

--=================================================
--CREATE BACKUP OF OLD ABUTP               --------
--=================================================

--drop table if exists olapts.abutp_backup
create table olapts.abutp_backup as
select * from olapts.abutp;

--=================================================
------CHECK DATA-----------------------------------
--=================================================
select *  from olapts.abutp_backup limit 100;

--=================================================
-------        DROP OLD abutp       ---------------
--=================================================
--****Uncomment row 29 after the backup****--
--DROP TABLE IF EXISTS olapts.abutp;

--=================================================
--              CREATE NEW abutp     --------------
--=================================================
CREATE TABLE olapts.abutp
(
    id_ character varying COLLATE pg_catalog."default",
    pkid_ character varying COLLATE pg_catalog."default",
    utpid numeric,
    active text COLLATE pg_catalog."default",
    adverseid boolean,
    arrearsid boolean,
    assessmentdate timestamp without time zone,
    bankid boolean,
    borrowerexposuresid boolean,
    borrowerid boolean,
    borrowersid boolean,
    borrowerincomeid boolean,
    breachid boolean,
    cdsid boolean,
    connectedid boolean,
    creditcommittee text COLLATE pg_catalog."default",
    creditcommitteeval text COLLATE pg_catalog."default",
    creditinstitutionid boolean,
    defaultdesignator boolean,
    delayedid boolean,
    disactivemarketid boolean,
    disappearanceid boolean,
	ebaid boolean,
    ebitdaid boolean,
    entityid numeric,
    entityversionid numeric,
    expectationid boolean,
    financialid boolean,
    fplmonthid boolean,
    fplmeasuresid boolean,
    fraudid boolean,
    isdaid boolean,
    lawsuitid boolean,
    licenseid boolean,
    loanaccelaratedid boolean,
    loanid boolean,
    lossid boolean,
    materialid boolean,
    modificationsid boolean,
    multipleid boolean,
    negativeid boolean,
    npvbiggerid boolean,
    npvid boolean,
    obligorid boolean,
    origination text COLLATE pg_catalog."default",
    outofcourtid boolean,
    postponementsid boolean,
    provisionid boolean,
    reductionid boolean,
    restrictedid boolean,
    restructuringid boolean,
    saleid boolean,
    thirdpartyid boolean,
    utpassessmentdate timestamp without time zone,
    utpassessmentuser text COLLATE pg_catalog."default",
    utpauthorizationdate date,
    utpauthorizationsysdate timestamp without time zone,
    utpauthorizeduser text COLLATE pg_catalog."default",
    utpboolhidden boolean,
    utpcatchtriggers integer,
    utpcomments text COLLATE pg_catalog."default",
    utpobligor text COLLATE pg_catalog."default",
    utpobligorval text COLLATE pg_catalog."default",
    writeoffid boolean,
    wfid_ character varying COLLATE pg_catalog."default",
    taskid_ character varying COLLATE pg_catalog."default",
    versionid_ integer,
    isdeleted_ boolean,
    islatestversion_ boolean,
    baseversionid_ integer,
    contextuserid_ character varying COLLATE pg_catalog."default",
    isvisible_ boolean,
    isvalid_ boolean,
    snapshotid_ integer,
    t_ character varying COLLATE pg_catalog."default",
    createdby_ character varying COLLATE pg_catalog."default",
    createddate_ timestamp without time zone,
    updatedby_ character varying COLLATE pg_catalog."default",
    updateddate_ timestamp without time zone,
    fkid_entity character varying COLLATE pg_catalog."default",
    sourcepopulatedby_ character varying COLLATE pg_catalog."default",
    sourcepopulateddate_ timestamp without time zone,
    populateddate_ timestamp without time zone
);

--=============================================================================
--INSERT INTO olapts.abutp data from olapts.abutp_backup
--=============================================================================
INSERT INTO olapts.abutp(
id_, pkid_, utpid, active, adverseid, arrearsid, assessmentdate, bankid, 
borrowerexposuresid, borrowerid, borrowersid, borrowerincomeid, breachid, cdsid,
connectedid, creditcommittee, creditcommitteeval, creditinstitutionid, defaultdesignator, 
delayedid, disactivemarketid, disappearanceid, ebaid, ebitdaid, entityid, entityversionid, expectationid, 
financialid, fplmonthid, fplmeasuresid, fraudid, isdaid, lawsuitid, licenseid, loanaccelaratedid, 
loanid, lossid, materialid, modificationsid, multipleid, negativeid, npvbiggerid, npvid, obligorid,
origination, outofcourtid, postponementsid, provisionid, reductionid, restrictedid, restructuringid, 
saleid, thirdpartyid, utpassessmentdate, utpassessmentuser, utpauthorizationdate, utpauthorizationsysdate, 
utpauthorizeduser, utpboolhidden, utpcatchtriggers, utpcomments, utpobligor, utpobligorval, writeoffid, 
wfid_, taskid_, versionid_, isdeleted_, islatestversion_, baseversionid_, contextuserid_, isvisible_, 
isvalid_, snapshotid_, t_, createdby_, createddate_, updatedby_, updateddate_, fkid_entity, sourcepopulatedby_, 
sourcepopulateddate_, populateddate_
)
select
id_, pkid_, utpid, active, adverseid, arrearsid, assessmentdate, bankid, 
borrowerexposuresid, borrowerid, borrowersid, borrowerincomeid, breachid, cdsid,
connectedid, creditcommittee, creditcommitteeval, creditinstitutionid, defaultdesignator, 
delayedid, disactivemarketid, disappearanceid, ebaid, ebitdaid, entityid, entityversionid, expectationid, 
financialid, fplmonthid, fplmeasuresid, fraudid, isdaid, lawsuitid, licenseid, loanaccelaratedid, 
loanid, lossid, materialid, modificationsid, multipleid, negativeid, npvbiggerid, npvid, obligorid,
origination, outofcourtid, postponementsid, provisionid, reductionid, restrictedid, restructuringid, 
saleid, thirdpartyid, utpassessmentdate, utpassessmentuser, utpauthorizationdate, utpauthorizationsysdate, 
utpauthorizeduser, utpboolhidden, utpcatchtriggers, utpcomments, utpobligor, utpobligorval, writeoffid, 
wfid_, taskid_, versionid_, isdeleted_, islatestversion_, baseversionid_, contextuserid_, isvisible_, 
isvalid_, snapshotid_, t_, createdby_, createddate_, updatedby_, updateddate_, fkid_entity, sourcepopulatedby_, 
sourcepopulateddate_, populateddate_
from olapts.abutp_backup;

	
--=================================================
--CHECK DATA
--=================================================
--select * from olapts.abutp

--=================================================
--DROP BACKUP TABLE
--=================================================
--DROP TABLE olapts.abutp_backup

--=================================================
--Run Ralph Script
--=================================================