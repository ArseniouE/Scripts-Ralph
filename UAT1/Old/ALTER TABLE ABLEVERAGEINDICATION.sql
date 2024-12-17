--====================================================================
--             ALTER TABLE ABLEVERAGEINDICATION                     --
--====================================================================

ALTER TABLE olapts.ableverageindication
ADD COLUMN IF NOT EXISTS leverageownedbyasponsor text, 
ADD COLUMN IF NOT EXISTS leverageownedbyasponsorval text, 
ADD COLUMN IF NOT EXISTS leveragesponsorname text,
ADD COLUMN IF NOT EXISTS createduser text;

--=================================================
--CREATE BACKUP OF OLD ableverageindication--------
--=================================================

--drop table if exists olapts.ableverageindication_backup
create table olapts.ableverageindication_backup as
select * from olapts.ableverageindication;

--=================================================
------CHECK DATA-----------------------------------
--=================================================
select *  from olapts.ableverageindication_backup limit 100;

--=================================================
-------DROP OLD ableverageindication---------------
--=================================================

--****Uncomment row 29 after the backup****--
--DROP TABLE IF EXISTS olapts.ableverageindication;

--=================================================
--CREATE NEW ableverageindication------------------
--=================================================
CREATE TABLE olapts.ableverageindication
(
    id_ character varying COLLATE pg_catalog."default",
    pkid_ character varying COLLATE pg_catalog."default",
    leverageid numeric,
    active character varying COLLATE pg_catalog."default",
    createddate timestamp without time zone,
    createduser text COLLATE pg_catalog."default",
    creditcommitteedate date,
    entityid numeric,
    entityversionid numeric,
    highleveragecustomer text COLLATE pg_catalog."default",
    highleveragecustomerval text COLLATE pg_catalog."default",
    leveragefinancialindication text COLLATE pg_catalog."default",
    leveragefinancialindicationval text COLLATE pg_catalog."default",
    leveragefinancingreason text COLLATE pg_catalog."default",
    leveragefinancingreasonval text COLLATE pg_catalog."default",
    leverageownedbyasponsor text COLLATE pg_catalog."default",
    leverageownedbyasponsorval text COLLATE pg_catalog."default",
    leveragesaved integer,
    leveragesponsorname text COLLATE pg_catalog."default",
    leveragetypereview text COLLATE pg_catalog."default",
    leveragetypereviewval text COLLATE pg_catalog."default",
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
--INSERT INTO olapts.ableverageindication data from olapts.ableverageindication_backup
--=============================================================================
INSERT INTO olapts.ableverageindication(
id_, pkid_, leverageid, active, createddate, createduser, creditcommitteedate, entityid, entityversionid, 
highleveragecustomer, highleveragecustomerval, leveragefinancialindication, leveragefinancialindicationval, 
leveragefinancingreason, leveragefinancingreasonval, leverageownedbyasponsor, leverageownedbyasponsorval, 
leveragesaved, leveragesponsorname, leveragetypereview, leveragetypereviewval, wfid_, taskid_, versionid_, 
isdeleted_, islatestversion_, baseversionid_, contextuserid_, isvisible_, isvalid_, snapshotid_, t_, createdby_, 
createddate_, updatedby_, updateddate_, fkid_entity, sourcepopulatedby_, sourcepopulateddate_, populateddate_
)
select
id_, pkid_, leverageid, active, createddate, createduser, creditcommitteedate, entityid, entityversionid, 
highleveragecustomer, highleveragecustomerval, leveragefinancialindication, leveragefinancialindicationval, 
leveragefinancingreason, leveragefinancingreasonval, leverageownedbyasponsor, leverageownedbyasponsorval, 
leveragesaved, leveragesponsorname, leveragetypereview, leveragetypereviewval, wfid_, taskid_, versionid_, 
isdeleted_, islatestversion_, baseversionid_, contextuserid_, isvisible_, isvalid_, snapshotid_, t_, createdby_, 
createddate_, updatedby_, updateddate_, fkid_entity, sourcepopulatedby_, sourcepopulateddate_, populateddate_
from olapts.ableverageindication_backup;

	
--=================================================
--CHECK DATA
--=================================================
--select * from olapts.ableverageindication

--=================================================
--DROP BACKUP TABLE
--=================================================
--DROP TABLE olapts.ableverageindication_backup

--=================================================
--Run Ralph Script
--=================================================