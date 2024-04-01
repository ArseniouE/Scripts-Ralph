-- run with postgres account in olap database
-- GRANT USAGE ON FOREIGN SERVER matenantserver to olap;
--Run once
--ALTER SYSTEM SET checkpoint_completion_target = '0.9';

-- drop table olapts.refreshhistory;
-- run with olap account in olap database
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (addedaccount) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (ratingoverride) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (peeranalysis) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (entityofficer) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (projectionstatement) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (ratingscenarioblockdata) FROM SERVER matenantserver INTO madata;

--enable remote estimate
ALTER FOREIGN TABLE madata.historicalstatement OPTIONS ( set fetch_size '100',set use_remote_estimate 'on' );
ALTER FOREIGN TABLE madata.ratingscenarioblockdata OPTIONS ( set fetch_size '100',set use_remote_estimate 'on' );

-- Increase work memory and effective cache for complex operations/sorts
set work_mem = '10 GB';
set maintenance_work_mem = '10 GB';
set effective_cache_size = '48 GB';
set effective_io_concurrency = 300;

--suggest planner to disregard sequential scanning and enable partitionwise aggregates
set random_page_cost = 2;
set enable_partitionwise_join = on;
set enable_partitionwise_aggregate = on;
set force_parallel_mode = on;
set parallel_setup_cost = 10;
set parallel_tuple_cost = 0.001;

-- Increase workers to reach multiparallel processing
--set max_worker_processes = 32;
set max_parallel_workers_per_gather = 16;
set max_parallel_maintenance_workers = 10;
set max_parallel_workers = 32;
set parallel_leader_participation = on;
set default_statistics_target = 500;


--JIT
set jit = on;
set jit_above_cost = 100000;
set jit_optimize_above_cost = 500000;
set jit_inline_above_cost = 500000;

SET TIMEZONE = 'UTC';
SET SESSION myvariables.popdate = NOW;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

CREATE TABLE IF NOT EXISTS olapts.refreshhistory (
tablename VARCHAR,
asofdate TIMESTAMP WITHOUT TIME ZONE DEFAULT (current_setting('myvariables.popdate')::timestamp at time zone 'utc'),
prevsuccessdate TIMESTAMP WITHOUT TIME ZONE DEFAULT (current_setting('myvariables.popdate')::timestamp at time zone 'utc')
);

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;

BEGIN
-- Entity Related Tables
--abaddress
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABADDRESS') THEN
raise notice '% - Step abaddress - part a start', clock_timestamp();
insert into olapts.abaddress
select
id_ dimaddressid_ ,
pkid_ pkid_ ,
jsondoc_->>'Address1' address1 ,
jsondoc_->>'Address2' address2 ,
jsondoc_->>'AddressId' addressid ,
jsondoc_->>'AddressType' addresstype ,
jsondoc_->>'City' city ,
jsondoc_->>'Country' country ,
jsondoc_->>'EntityId' entityid ,
jsondoc_->>'ExternalDataSource' externaldatasource ,
jsondoc_->>'State' state ,
jsondoc_->>'Zip' zip ,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_::int baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
fkid_entity fkid_entity ,
fkid_landdeveloper fkid_landdeveloper ,
fkid_homebuilder fkid_homebuilder ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.address
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABADDRESS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abaddress - part a end', clock_timestamp();
ELSE
raise notice '% - Step abaddress - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abaddress;
CREATE TABLE olapts.abaddress AS
select
id_ dimaddressid_ ,
pkid_ pkid_ ,
jsondoc_->>'Address1' address1 ,
jsondoc_->>'Address2' address2 ,
jsondoc_->>'AddressId' addressid ,
jsondoc_->>'AddressType' addresstype ,
jsondoc_->>'City' city ,
jsondoc_->>'Country' country ,
jsondoc_->>'EntityId' entityid ,
jsondoc_->>'ExternalDataSource' externaldatasource ,
jsondoc_->>'State' state ,
jsondoc_->>'Zip' zip ,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_::int baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
fkid_entity fkid_entity ,
fkid_landdeveloper fkid_landdeveloper ,
fkid_homebuilder fkid_homebuilder ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.address
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABADDRESS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abaddress - part b end', clock_timestamp();

--abaddress
raise notice '% - Step abaddress_idx - part a start', clock_timestamp(); 
CREATE INDEX IF NOT EXISTS abaddress_idx ON olapts.abaddress (dimaddressid_);
CREATE INDEX IF NOT EXISTS abaddress_idx2 ON olapts.abaddress (pkid_,versionid_);

raise notice '% - Step abaddress_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abaddress - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abaddressflag;
CREATE TABLE olapts.abaddressflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_::int baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.address
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABADDRESS';
delete from olapts.refreshhistory where tablename = 'ABADDRESS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABADDRESS' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABADDRESSFLAG';
delete from olapts.refreshhistory where tablename = 'ABADDRESSFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABADDRESSFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abaddress - part c end', clock_timestamp();

raise notice '% - Step abaddressflag_idx - part a start', clock_timestamp(); 

CREATE INDEX IF NOT EXISTS abaddressflag_idx ON olapts.abaddressflag (id_);
CREATE INDEX IF NOT EXISTS abaddressflag_idx2 ON olapts.abaddressflag (pkid_,versionid_);


raise notice '% - Step abaddressflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

-- abentityindustry
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABENTITYINDUSTRY') THEN
	raise notice '% - Step abentityindustry - part a start', clock_timestamp();
	insert into olapts.abentityindustry
	select 
	id_ dimentityindustryid_ ,
	pkid_ pkid_ ,
	jsondoc_->>'Classification' classification ,
	jsondoc_->>'EntityId' entityid ,
	jsondoc_->>'ExternalDataSource' externaldatasource ,
	jsondoc_->>'Inactive' inactive ,
	jsondoc_->>'IndustryCode' industrycode ,
	jsondoc_->>'IndustryId' industryid ,
	(jsondoc_->>'IsPrimary')::boolean isprimary ,
	jsondoc_->>'Percentage' percentage ,
	jsondoc_->>'UpdatePeerSelection' updatepeerselection ,
	wfid_ wfid_ ,
	taskid_ taskid_ ,
	versionid_ versionid_ ,
	isdeleted_::boolean isdeleted_ ,
	islatestversion_::boolean islatestversion_ ,
	baseversionid_ baseversionid_ ,
	contextuserid_ contextuserid_ ,
	isvisible_::boolean isvisible_ ,
	isvalid_::boolean isvalid_ ,
	snapshotid_ snapshotid_ ,
	t_ t_ ,
	createdby_ createdby_ ,
	createddate_ createddate_ ,
	updatedby_ updatedby_ ,
	updateddate_ updateddate_ ,
	fkid_entity fkid_entity ,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
	GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
	current_setting('myvariables.popdate')::timestamp as populateddate_ 
	from madata.entityindustry
	where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABENTITYINDUSTRY')
	and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
	;

	raise notice '% - Step abentityindustry - part a end', clock_timestamp();
ELSE
	raise notice '% - Step abentityindustry - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abentityindustry;
	CREATE TABLE olapts.abentityindustry AS
	select 
	id_ dimentityindustryid_ ,
	pkid_ pkid_ ,
	jsondoc_->>'Classification' classification ,
	jsondoc_->>'EntityId' entityid ,
	jsondoc_->>'ExternalDataSource' externaldatasource ,
	jsondoc_->>'Inactive' inactive ,
	jsondoc_->>'IndustryCode' industrycode ,
	jsondoc_->>'IndustryId' industryid ,
	(jsondoc_->>'IsPrimary')::boolean isprimary ,
	jsondoc_->>'Percentage' percentage ,
	jsondoc_->>'UpdatePeerSelection' updatepeerselection ,
	wfid_ wfid_ ,
	taskid_ taskid_ ,
	versionid_ versionid_ ,
	isdeleted_::boolean isdeleted_ ,
	islatestversion_::boolean islatestversion_ ,
	baseversionid_ baseversionid_ ,
	contextuserid_ contextuserid_ ,
	isvisible_::boolean isvisible_ ,
	isvalid_::boolean isvalid_ ,
	snapshotid_ snapshotid_ ,
	t_ t_ ,
	createdby_ createdby_ ,
	createddate_ createddate_ ,
	updatedby_ updatedby_ ,
	updateddate_ updateddate_ ,
	fkid_entity fkid_entity ,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
	GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
	current_setting('myvariables.popdate')::timestamp as populateddate_ 
	from madata.entityindustry
	where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABENTITYINDUSTRY')
	and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
	;

	raise notice '% - Step abentityindustry - part b end', clock_timestamp();
	
	--abentityindustry
	raise notice '% - Step abentityindustry_idx - part a start', clock_timestamp(); 
	DROP INDEX if exists olapts.abentityindustry_idx;
	DROP INDEX if exists olapts.abentityindustry_idx2;
	DROP INDEX if exists olapts.abentityindustry_idx3;
	DROP INDEX if exists olapts.abentityindustry_idx4;

	CREATE INDEX IF NOT EXISTS abentityindustry_idx_gin ON olapts.abentityindustry USING GIN (entityid,pkid_,versionid_,wfid_);
	CREATE INDEX IF NOT EXISTS abentityindustry_idx_pkid_hash ON olapts.abentityindustry USING hash (pkid_);
	CREATE INDEX IF NOT EXISTS abentityindustry_idx_date_brin ON olapts.abentityindustry USING BRIN (sourcepopulateddate_);
	CREATE INDEX IF NOT EXISTS abentityindustry_idx_btree ON olapts.abentityindustry (entityid,pkid_,ltrim(rtrim(replace( substring(industrycode,strpos(industrycode,': "')+1,( strpos(industrycode,'",') -strpos(industrycode,': "')-1 ) ),'"','') )),versionid_,sourcepopulateddate_) include (isdeleted_,isprimary,isvalid_,isvisible_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);
		

	raise notice '% - Step abentityindustry_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abentityindustry - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abentityindustryflag;
CREATE TABLE olapts.abentityindustryflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
(jsondoc_->>'IsPrimary')::boolean isprimary ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.entityindustry
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENTITYINDUSTRY';
delete from olapts.refreshhistory where tablename = 'ABENTITYINDUSTRY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENTITYINDUSTRY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENTITYINDUSTRYFLAG';
delete from olapts.refreshhistory where tablename = 'ABENTITYINDUSTRYFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENTITYINDUSTRYFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abentityindustry - part c end', clock_timestamp();

raise notice '% - Step abentityindustryflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abentityindustryflag_idx;
DROP INDEX if exists olapts.abentityindustryflag_idx2;
DROP INDEX if exists olapts.abentityindustryflag_idx3;
DROP INDEX if exists olapts.abentityindustryflag_idx4;

CREATE INDEX IF NOT EXISTS abentityindustryflag_idx_gin ON olapts.abentityindustryflag USING GIN (pkid_,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS abentityindustryflag_idx_pkid_hash ON olapts.abentityindustryflag USING hash (pkid_);
CREATE INDEX IF NOT EXISTS abentityindustryflag_idx_id_hash ON olapts.abentityindustryflag USING hash (id_);
CREATE INDEX IF NOT EXISTS abentityindustryflag_idx_date_brin ON olapts.abentityindustryflag USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abentityindustryflag_idx_btree ON olapts.abentityindustryflag (pkid_,versionid_,sourcepopulateddate_) include (isdeleted_,isvalid_,isvisible_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);

raise notice '% - Step abentityindustryflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

-- abentityofficers
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABENTITYOFFICERS') THEN
raise notice '% - Step abentityofficers - part a start', clock_timestamp();
insert into olapts.abentityofficers
select 
id_ dimentityofficersid_,
pkid_::varchar as pkid_,
(jsondoc_->>'EntityId')::numeric entityid,
(jsondoc_->>'Id')::numeric id,
(jsondoc_->>'IsPrimary')::boolean isprimary,
(jsondoc_->>'Title') title,
(jsondoc_->>'UserId') userid,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar t_ ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
fkid_entity,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.entityofficer
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABENTITYOFFICERS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abentityofficers - part a end', clock_timestamp();
ELSE
raise notice '% - Step abentityofficers - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abentityofficers;
CREATE TABLE olapts.abentityofficers AS
select 
id_ dimentityofficersid_,
pkid_::varchar as pkid_,
(jsondoc_->>'EntityId')::numeric entityid,
(jsondoc_->>'Id')::numeric id,
(jsondoc_->>'IsPrimary')::boolean isprimary,
(jsondoc_->>'Title') title,
(jsondoc_->>'UserId') userid,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar t_ ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
fkid_entity,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.entityofficer
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABENTITYOFFICERS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;
raise notice '% - Step abentityofficers - part b end', clock_timestamp();

--abentityofficers
raise notice '% - Step abentityofficers_idx - part a start', clock_timestamp(); 

CREATE INDEX IF NOT EXISTS abentityofficers_idx ON olapts.abentityofficers (dimentityofficersid_);
CREATE INDEX IF NOT EXISTS abentityofficers_idx2 ON olapts.abentityofficers (pkid_,versionid_);


raise notice '% - Step abentityofficers_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abentityofficers - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abentityofficersflag;
CREATE TABLE olapts.abentityofficersflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.entityofficer
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENTITYOFFICERS';
delete from olapts.refreshhistory where tablename = 'ABENTITYOFFICERS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENTITYOFFICERS' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENTITYOFFICERSFLAG';
delete from olapts.refreshhistory where tablename = 'ABENTITYOFFICERSFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENTITYOFFICERSFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abentityofficers - part c end', clock_timestamp();

raise notice '% - Step abentityofficersflag_idx - part a start', clock_timestamp(); 

CREATE INDEX IF NOT EXISTS abentityofficersflag_idx ON olapts.abentityofficersflag (id_);
CREATE INDEX IF NOT EXISTS abentityofficersflag_idx2 ON olapts.abentityofficersflag (pkid_,versionid_);


raise notice '% - Step abentityofficersflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

-- abupentity
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABUPENTITY') THEN
raise notice '% - Step abupentity - part a start', clock_timestamp();
insert into olapts.abupentity
select
id_ dimupentityid_,
pkid_::varchar as pkid_,
(jsondoc_->>'EntityId') entityid,
(jsondoc_->>'FinancialId') financialid,
(jsondoc_->>'UserId') userid,
((jsondoc_->>'Data')::json->>'AccessGroup') accessgroup,
((jsondoc_->>'Data')::json->>'AsOfDate') asofdate,
((jsondoc_->>'Data')::json->>'CIFNumber') cifnumber,
((jsondoc_->>'Data')::json->>'Consolidation') consolidation,
((jsondoc_->>'Data')::json->>'Country') country,
((jsondoc_->>'Data')::json->>'CustomerName') customername,
((jsondoc_->>'Data')::json->>'DBNumber') dbnumber,
((jsondoc_->>'Data')::json->>'EDFCreditMeasureType') edfcreditmeasuretype,
((jsondoc_->>'Data')::json->>'EDFType') edftype,
((jsondoc_->>'Data')::json->>'EntityType') entitytype,
((jsondoc_->>'Data')::json->>'FinancialStmtOnly') financialstmtonly,
((jsondoc_->>'Data')::json->>'FirmType') firmtype,
((jsondoc_->>'Data')::json->>'FitchRating') fitchrating,
((jsondoc_->>'Data')::json->>'FitchRatingOutlook') fitchratingoutlook,
((jsondoc_->>'Data')::json->>'FYEMonth') fyemonth,
((jsondoc_->>'Data')::json->>'GroupID') groupid,
((jsondoc_->>'Data')::json->>'IndustryClass') industryclass,
((jsondoc_->>'Data')::json->>'IndustryCode') industrycode,
((jsondoc_->>'Data')::json->>'IndustryCode2') industrycode2,
((jsondoc_->>'Data')::json->>'IndustryCode3') industrycode3,
((jsondoc_->>'Data')::json->>'IndustryCode4') industrycode4,
((jsondoc_->>'Data')::json->>'IndustryCode5') industrycode5,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight1') industrycodeweight1,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight2') industrycodeweight2,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight3') industrycodeweight3,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight4') industrycodeweight4,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight5') industrycodeweight5,
((jsondoc_->>'Data')::json->>'IndustryModel') industrymodel,
((jsondoc_->>'Data')::json->>'IndustryType') industrytype,
((jsondoc_->>'Data')::json->>'LASID') lasid,
((jsondoc_->>'Data')::json->>'LEI') lei,
((jsondoc_->>'Data')::json->>'LegalForm') legalform,
((jsondoc_->>'Data')::json->>'MKMVID') mkmvid,
((jsondoc_->>'Data')::json->>'MoodyRatingOutlook') moodyratingoutlook,
((jsondoc_->>'Data')::json->>'MoodysRating') moodysrating,
((jsondoc_->>'Data')::json->>'NAICS') naics,
((jsondoc_->>'Data')::json->>'NumberStatements') numberstatements,
((jsondoc_->>'Data')::json->>'OtherID') otherid,
((jsondoc_->>'Data')::json->>'RCModel') rcmodel,
((jsondoc_->>'Data')::json->>'RCVersion') rcversion,
((jsondoc_->>'Data')::json->>'RelationshipType') relationshiptype,
((jsondoc_->>'Data')::json->>'ResponsOffice') responsoffice,
((jsondoc_->>'Data')::json->>'ResponsOfficer') responsofficer,
((jsondoc_->>'Data')::json->>'RiskGrade') riskgrade,
((jsondoc_->>'Data')::json->>'SIC') sic,
((jsondoc_->>'Data')::json->>'SPRating') sprating,
((jsondoc_->>'Data')::json->>'SPRatingOutlook') spratingoutlook,
((jsondoc_->>'Data')::json->>'SegmentType') segmenttype,
((jsondoc_->>'Data')::json->>'ShortName') shortname,
((jsondoc_->>'Data')::json->>'SourceCurrency') sourcecurrency,
((jsondoc_->>'Data')::json->>'Status') status,
((jsondoc_->>'Data')::json->>'TargetCurrency') targetcurrency,
((jsondoc_->>'Data')::json->>'TaxID') taxid,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
statusid_::int4,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
t_::varchar t_ ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.upentity
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPENTITY')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abupentity - part a end', clock_timestamp();
ELSE
raise notice '% - Step abupentity - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abupentity;
CREATE TABLE olapts.abupentity AS
select
id_ dimupentityid_,
pkid_::varchar as pkid_,
(jsondoc_->>'EntityId') entityid,
(jsondoc_->>'FinancialId') financialid,
(jsondoc_->>'UserId') userid,
((jsondoc_->>'Data')::json->>'AccessGroup') accessgroup,
((jsondoc_->>'Data')::json->>'AsOfDate') asofdate,
((jsondoc_->>'Data')::json->>'CIFNumber') cifnumber,
((jsondoc_->>'Data')::json->>'Consolidation') consolidation,
((jsondoc_->>'Data')::json->>'Country') country,
((jsondoc_->>'Data')::json->>'CustomerName') customername,
((jsondoc_->>'Data')::json->>'DBNumber') dbnumber,
((jsondoc_->>'Data')::json->>'EDFCreditMeasureType') edfcreditmeasuretype,
((jsondoc_->>'Data')::json->>'EDFType') edftype,
((jsondoc_->>'Data')::json->>'EntityType') entitytype,
((jsondoc_->>'Data')::json->>'FinancialStmtOnly') financialstmtonly,
((jsondoc_->>'Data')::json->>'FirmType') firmtype,
((jsondoc_->>'Data')::json->>'FitchRating') fitchrating,
((jsondoc_->>'Data')::json->>'FitchRatingOutlook') fitchratingoutlook,
((jsondoc_->>'Data')::json->>'FYEMonth') fyemonth,
((jsondoc_->>'Data')::json->>'GroupID') groupid,
((jsondoc_->>'Data')::json->>'IndustryClass') industryclass,
((jsondoc_->>'Data')::json->>'IndustryCode') industrycode,
((jsondoc_->>'Data')::json->>'IndustryCode2') industrycode2,
((jsondoc_->>'Data')::json->>'IndustryCode3') industrycode3,
((jsondoc_->>'Data')::json->>'IndustryCode4') industrycode4,
((jsondoc_->>'Data')::json->>'IndustryCode5') industrycode5,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight1') industrycodeweight1,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight2') industrycodeweight2,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight3') industrycodeweight3,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight4') industrycodeweight4,
((jsondoc_->>'Data')::json->>'IndustryCodeWeight5') industrycodeweight5,
((jsondoc_->>'Data')::json->>'IndustryModel') industrymodel,
((jsondoc_->>'Data')::json->>'IndustryType') industrytype,
((jsondoc_->>'Data')::json->>'LASID') lasid,
((jsondoc_->>'Data')::json->>'LEI') lei,
((jsondoc_->>'Data')::json->>'LegalForm') legalform,
((jsondoc_->>'Data')::json->>'MKMVID') mkmvid,
((jsondoc_->>'Data')::json->>'MoodyRatingOutlook') moodyratingoutlook,
((jsondoc_->>'Data')::json->>'MoodysRating') moodysrating,
((jsondoc_->>'Data')::json->>'NAICS') naics,
((jsondoc_->>'Data')::json->>'NumberStatements') numberstatements,
((jsondoc_->>'Data')::json->>'OtherID') otherid,
((jsondoc_->>'Data')::json->>'RCModel') rcmodel,
((jsondoc_->>'Data')::json->>'RCVersion') rcversion,
((jsondoc_->>'Data')::json->>'RelationshipType') relationshiptype,
((jsondoc_->>'Data')::json->>'ResponsOffice') responsoffice,
((jsondoc_->>'Data')::json->>'ResponsOfficer') responsofficer,
((jsondoc_->>'Data')::json->>'RiskGrade') riskgrade,
((jsondoc_->>'Data')::json->>'SIC') sic,
((jsondoc_->>'Data')::json->>'SPRating') sprating,
((jsondoc_->>'Data')::json->>'SPRatingOutlook') spratingoutlook,
((jsondoc_->>'Data')::json->>'SegmentType') segmenttype,
((jsondoc_->>'Data')::json->>'ShortName') shortname,
((jsondoc_->>'Data')::json->>'SourceCurrency') sourcecurrency,
((jsondoc_->>'Data')::json->>'Status') status,
((jsondoc_->>'Data')::json->>'TargetCurrency') targetcurrency,
((jsondoc_->>'Data')::json->>'TaxID') taxid,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
statusid_::int4,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
t_::varchar t_ ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.upentity
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPENTITY')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;
raise notice '% - Step abupentity - part b end', clock_timestamp();

--abupentity
raise notice '% - Step abupentity_idx - part a start', clock_timestamp(); 

CREATE INDEX IF NOT EXISTS abupentity_idx ON olapts.abupentity (dimupentityid_);
CREATE INDEX IF NOT EXISTS abupentity_idx2 ON olapts.abupentity (pkid_,versionid_);

raise notice '% - Step abupentity_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abupentity - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abupentityflag;
CREATE TABLE olapts.abupentityflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.upentity
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPENTITY';
delete from olapts.refreshhistory where tablename = 'ABUPENTITY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPENTITY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPENTITYFLAG';
delete from olapts.refreshhistory where tablename = 'ABUPENTITYFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPENTITYFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abupentity - part c end', clock_timestamp();

raise notice '% - Step abupentityflag_idx - part a start', clock_timestamp(); 

CREATE INDEX IF NOT EXISTS abupentityflag_idx ON olapts.abupentityflag (id_);
CREATE INDEX IF NOT EXISTS abupentityflag_idx2 ON olapts.abupentityflag (pkid_,versionid_);

raise notice '% - Step abupentityflag_idx - part a end', clock_timestamp(); 
END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

--abfactentity
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABFACTENTITY') THEN
	raise notice '% - Step abfactentity - part a start', clock_timestamp(); 
	insert into olapts.abfactentity
	select 
	id_ factentityid_,
	pkid_,
	jsondoc_->>'Gc12' gc12,
	jsondoc_->>'Gc13' gc13,
	jsondoc_->>'Gc16' gc16,
	jsondoc_->>'Gc17' gc17,
	jsondoc_->>'Gc18' gc18,
	jsondoc_->>'Gc19' gc19,
	jsondoc_->>'Gc22' gc22,
	jsondoc_->>'Gc23' gc23,
	jsondoc_->>'Gc100' gc100,
	jsondoc_->>'Gc108' gc108,
	jsondoc_->>'Gc109' gc109,
	jsondoc_->>'Gc110' gc110,
	jsondoc_->>'Gc111' gc111,
	jsondoc_->>'Gc112' gc112,
	jsondoc_->>'Gc113' gc113,
	jsondoc_->>'Gc114' gc114,
	jsondoc_->>'Gc115' gc115,
	jsondoc_->>'Gc116' gc116,
	jsondoc_->>'Gc117' gc117,
	jsondoc_->>'TaxId' taxid,
	jsondoc_->>'Gender' gender,
	jsondoc_->>'IdType' idtype,
	jsondoc_->>'OnList' onlist,
	jsondoc_->>'CdiCode' cdicode,
	jsondoc_->>'GroupId' groupid,
	jsondoc_->>'Currency' currency,
	jsondoc_->>'Division' division,
	jsondoc_->>'EntityId' entityid,
	jsondoc_->>'FirmType' firmtype,
	jsondoc_->>'IdNumber' idnumber,
	jsondoc_->>'IsLocked' islocked,
	jsondoc_->>'LockedBy' lockedby,
	jsondoc_->>'LongName' longname,
	jsondoc_->>'Prospect' prospect,
	jsondoc_->>'SystemId' systemid,
	jsondoc_->>'ShortName' shortname,
	jsondoc_->>'StockCode' stockcode,
	jsondoc_->>'moduleId_' moduleid_,
	jsondoc_->>'CurrentEmp' currentemp,
	jsondoc_->>'EntityType' entitytype,
	jsondoc_->>'LockedDate' lockeddate,
	jsondoc_->>'Occupation' occupation,
	jsondoc_->>'ReviewType' reviewtype,
	jsondoc_->>'Salutation' salutation,
	jsondoc_->>'DateOfBirth' dateofbirth,
	jsondoc_->>'Designation' designation,
	jsondoc_->>'GuarantorId' guarantorid,
	jsondoc_->>'LegalEntity' legalentity,
	jsondoc_->>'ListingDate' listingdate,
	jsondoc_->>'YearStarted' yearstarted,
	jsondoc_->>'BusinessType' businesstype,
	jsondoc_->>'CountryOfInc' countryofinc,
	jsondoc_->>'CountryOfRes' countryofres,
	jsondoc_->>'CreditNumber' creditnumber,
	jsondoc_->>'Descriptions' descriptions,
	jsondoc_->>'IndustryCode' industrycode,
	jsondoc_->>'IssueCountry' issuecountry,
	jsondoc_->>'MonthStarted' monthstarted,
	jsondoc_->>'Nationality1' nationality1,
	jsondoc_->>'Nationality2' nationality2,
	jsondoc_->>'Nationality3' nationality3,
	jsondoc_->>'CountryOfRisk' countryofrisk,
	jsondoc_->>'CustomerSince' customersince,
	jsondoc_->>'StockExchange' stockexchange,
	jsondoc_->>'YearInService' yearinservice,
	jsondoc_->>'ConnectedParty' connectedparty,
	jsondoc_->>'LastReviewDate' lastreviewdate,
	jsondoc_->>'NextReviewDate' nextreviewdate,
	jsondoc_->>'PlaceOfListing' placeoflisting,
	jsondoc_->>'CorporationType' corporationtype,
	jsondoc_->>'CreditCommittee' creditcommittee,
	jsondoc_->>'CreditPortfolio' creditportfolio,
	jsondoc_->>'NameOfGuarantor' nameofguarantor,
	jsondoc_->>'RestrictedUsers' restrictedusers,
	jsondoc_->>'SchedReviewDate' schedreviewdate,
	jsondoc_->>'CountryOfListing' countryoflisting,
	jsondoc_->>'RelationShipType' relationshiptype,
	jsondoc_->>'RestrictedEntity' restrictedentity,
	jsondoc_->>'BusinessPortfolio' businessportfolio,
	jsondoc_->>'EstablishmentDate' establishmentdate,
	jsondoc_->>'IndClassification' indclassification,
	jsondoc_->>'PermanentResident' permanentresident,
	jsondoc_->>'ResponsibleOffice' responsibleoffice,
	jsondoc_->>'ReviewedFrequency' reviewedfrequency,
	jsondoc_->>'RegistrationNumber' registrationnumber,
	jsondoc_->>'ResponsibleOfficer' responsibleofficer,
	jsondoc_->>'ExternalDataSources' externaldatasources,
	jsondoc_->>'PlaceOfIncorporation' placeofincorporation,
	jsondoc_->>'PrimaryCreditOfficer' primarycreditofficer,
	jsondoc_->>'MultipleNationalities' multiplenationalities,
	jsondoc_->>'PrimaryBankingOfficer' primarybankingofficer,
	jsondoc_->>'SourceSystemIdentifier' sourcesystemidentifier,
	jsondoc_->>'ConsolidatedBalanceSheet' consolidatedbalancesheet,
	jsondoc_->>'ProvinceStateOfIncorporation' provincestateofincorporation,
	jsondoc_->>'EnterpriseValueToTotalDebt' enterprisevaluetototaldebt,
	jsondoc_->>'ExpiryDate' expirydate,
	jsondoc_->>'ValuationMethodology' valuationmethodology,
	jsondoc_->>'Jurisdiction' jurisdiction,																								
	--not in tenant
	jsondoc_->>'Bankruptcy' bankruptcy,
	jsondoc_->>'GovernmentBailoutOffirm' governmentbailoutoffirm,
	jsondoc_->>'Industrysector' industrysector,
	wfid_,
	taskid_,
	snapshotid_,
	contextuserid_,
	createdby_,
	createddate_,
	updatedby_,
	updateddate_,
	isvalid_::boolean,
	baseversionid_,
	versionid_,
	isdeleted_::boolean,
	isvisible_::boolean,
	islatestversion_::boolean,
	t_ t_ ,
	(case when e.updateddate_>e.createddate_ then e.updatedby_ else e.createdby_ end) as sourcepopulatedby_,
	GREATEST(e.updateddate_,e.createddate_) as sourcepopulateddate_
	from madata.entity e
	where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABFACTENTITY')
	and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
	;
	
	raise notice '% - Step abfactentity - part a end', clock_timestamp(); 
ELSE
	raise notice '% - Step abfactentity - part b start', clock_timestamp(); 
	DROP TABLE IF EXISTS olapts.ABFACTENTITY;
	CREATE TABLE olapts.ABFACTENTITY AS
	select 
	id_ factentityid_,
	pkid_,
	jsondoc_->>'Gc12' gc12,
	jsondoc_->>'Gc13' gc13,
	jsondoc_->>'Gc16' gc16,
	jsondoc_->>'Gc17' gc17,
	jsondoc_->>'Gc18' gc18,
	jsondoc_->>'Gc19' gc19,
	jsondoc_->>'Gc22' gc22,
	jsondoc_->>'Gc23' gc23,
	jsondoc_->>'Gc100' gc100,
	jsondoc_->>'Gc108' gc108,
	jsondoc_->>'Gc109' gc109,
	jsondoc_->>'Gc110' gc110,
	jsondoc_->>'Gc111' gc111,
	jsondoc_->>'Gc112' gc112,
	jsondoc_->>'Gc113' gc113,
	jsondoc_->>'Gc114' gc114,
	jsondoc_->>'Gc115' gc115,
	jsondoc_->>'Gc116' gc116,
	jsondoc_->>'Gc117' gc117,
	jsondoc_->>'TaxId' taxid,
	jsondoc_->>'Gender' gender,
	jsondoc_->>'IdType' idtype,
	jsondoc_->>'OnList' onlist,
	jsondoc_->>'CdiCode' cdicode,
	jsondoc_->>'GroupId' groupid,
	jsondoc_->>'Currency' currency,
	jsondoc_->>'Division' division,
	jsondoc_->>'EntityId' entityid,
	jsondoc_->>'FirmType' firmtype,
	jsondoc_->>'IdNumber' idnumber,
	jsondoc_->>'IsLocked' islocked,
	jsondoc_->>'LockedBy' lockedby,
	jsondoc_->>'LongName' longname,
	jsondoc_->>'Prospect' prospect,
	jsondoc_->>'SystemId' systemid,
	jsondoc_->>'ShortName' shortname,
	jsondoc_->>'StockCode' stockcode,
	jsondoc_->>'moduleId_' moduleid_,
	jsondoc_->>'CurrentEmp' currentemp,
	jsondoc_->>'EntityType' entitytype,
	jsondoc_->>'LockedDate' lockeddate,
	jsondoc_->>'Occupation' occupation,
	jsondoc_->>'ReviewType' reviewtype,
	jsondoc_->>'Salutation' salutation,
	jsondoc_->>'DateOfBirth' dateofbirth,
	jsondoc_->>'Designation' designation,
	jsondoc_->>'GuarantorId' guarantorid,
	jsondoc_->>'LegalEntity' legalentity,
	jsondoc_->>'ListingDate' listingdate,
	jsondoc_->>'YearStarted' yearstarted,
	jsondoc_->>'BusinessType' businesstype,
	jsondoc_->>'CountryOfInc' countryofinc,
	jsondoc_->>'CountryOfRes' countryofres,
	jsondoc_->>'CreditNumber' creditnumber,
	jsondoc_->>'Descriptions' descriptions,
	jsondoc_->>'IndustryCode' industrycode,
	jsondoc_->>'IssueCountry' issuecountry,
	jsondoc_->>'MonthStarted' monthstarted,
	jsondoc_->>'Nationality1' nationality1,
	jsondoc_->>'Nationality2' nationality2,
	jsondoc_->>'Nationality3' nationality3,
	jsondoc_->>'CountryOfRisk' countryofrisk,
	jsondoc_->>'CustomerSince' customersince,
	jsondoc_->>'StockExchange' stockexchange,
	jsondoc_->>'YearInService' yearinservice,
	jsondoc_->>'ConnectedParty' connectedparty,
	jsondoc_->>'LastReviewDate' lastreviewdate,
	jsondoc_->>'NextReviewDate' nextreviewdate,
	jsondoc_->>'PlaceOfListing' placeoflisting,
	jsondoc_->>'CorporationType' corporationtype,
	jsondoc_->>'CreditCommittee' creditcommittee,
	jsondoc_->>'CreditPortfolio' creditportfolio,
	jsondoc_->>'NameOfGuarantor' nameofguarantor,
	jsondoc_->>'RestrictedUsers' restrictedusers,
	jsondoc_->>'SchedReviewDate' schedreviewdate,
	jsondoc_->>'CountryOfListing' countryoflisting,
	jsondoc_->>'RelationShipType' relationshiptype,
	jsondoc_->>'RestrictedEntity' restrictedentity,
	jsondoc_->>'BusinessPortfolio' businessportfolio,
	jsondoc_->>'EstablishmentDate' establishmentdate,
	jsondoc_->>'IndClassification' indclassification,
	jsondoc_->>'PermanentResident' permanentresident,
	jsondoc_->>'ResponsibleOffice' responsibleoffice,
	jsondoc_->>'ReviewedFrequency' reviewedfrequency,
	jsondoc_->>'RegistrationNumber' registrationnumber,
	jsondoc_->>'ResponsibleOfficer' responsibleofficer,
	jsondoc_->>'ExternalDataSources' externaldatasources,
	jsondoc_->>'PlaceOfIncorporation' placeofincorporation,
	jsondoc_->>'PrimaryCreditOfficer' primarycreditofficer,
	jsondoc_->>'MultipleNationalities' multiplenationalities,
	jsondoc_->>'PrimaryBankingOfficer' primarybankingofficer,
	jsondoc_->>'SourceSystemIdentifier' sourcesystemidentifier,
	jsondoc_->>'ConsolidatedBalanceSheet' consolidatedbalancesheet,
	jsondoc_->>'ProvinceStateOfIncorporation' provincestateofincorporation,
	jsondoc_->>'EnterpriseValueToTotalDebt' enterprisevaluetototaldebt,
	jsondoc_->>'ExpiryDate' expirydate,
	jsondoc_->>'ValuationMethodology' valuationmethodology,
	jsondoc_->>'Jurisdiction' jurisdiction,																								 
	--not in tenant
	jsondoc_->>'Bankruptcy' bankruptcy,
	jsondoc_->>'GovernmentBailoutOffirm' governmentbailoutoffirm,
	jsondoc_->>'Industrysector' industrysector,
	wfid_,
	taskid_,
	snapshotid_,
	contextuserid_,
	createdby_,
	createddate_,
	updatedby_,
	updateddate_,
	isvalid_::boolean,
	baseversionid_,
	versionid_,
	isdeleted_::boolean,
	isvisible_::boolean,
	islatestversion_::boolean,
	t_ t_ ,
	(case when e.updateddate_>e.createddate_ then e.updatedby_ else e.createdby_ end) as sourcepopulatedby_,
	GREATEST(e.updateddate_,e.createddate_) as sourcepopulateddate_
	from madata.entity e
	where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABFACTENTITY')
	and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
	;
	raise notice '% - Step abfactentity - part b end', clock_timestamp(); 
	
	--abfactentity
	raise notice '% - Step abfactentity_idx - part a start', clock_timestamp(); 
	DROP INDEX if exists olapts.abfactentity_idx;
	DROP INDEX if exists olapts.abfactentity_idx2;
	DROP INDEX if exists olapts.abfactentity_idx3;
	DROP INDEX if exists olapts.abfactentity_idx4;

	CREATE INDEX IF NOT EXISTS abfactentity_idxdate_brin ON olapts.abfactentity USING BRIN (sourcepopulateddate_);
	CREATE INDEX IF NOT EXISTS abfactentity_idx_entity_hash ON olapts.abfactentity USING hash (entityid);
	CREATE INDEX IF NOT EXISTS abfactentity_idx_pkid_hash ON olapts.abfactentity USING hash (pkid_);
	CREATE INDEX IF NOT EXISTS abfactentity_idx_versionid_hash ON olapts.abfactentity USING hash (versionid_);
	CREATE INDEX IF NOT EXISTS abfactentity_idx_btree ON olapts.abfactentity (pkid_,entityid,versionid_,sourcepopulateddate_) include(gc18,cdicode,systemid,isvalid_,isdeleted_,isvisible_,islatestversion_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);


	raise notice '% - Step abfactentity_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abfactentity - part c start', clock_timestamp(); 
DROP TABLE IF EXISTS olapts.ABFACTENTITYFLAG;
CREATE TABLE olapts.ABFACTENTITYFLAG AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.entity e
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFACTENTITY';
delete from olapts.refreshhistory where tablename = 'ABFACTENTITY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFACTENTITY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFACTENTITYFLAG';
delete from olapts.refreshhistory where tablename = 'ABFACTENTITYFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFACTENTITYFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abfactentity - part c end', clock_timestamp(); 

raise notice '% - Step abfactentityflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abfactentityflag_idx;
DROP INDEX if exists olapts.abfactentityflag_idx2;
DROP INDEX if exists olapts.abfactentityflag_idx3;
DROP INDEX if exists olapts.abfactentityflag_idx4;
DROP INDEX if exists olapts.abfactentityflag_idx5;
DROP INDEX if exists olapts.abfactentityflag_idx6;

CREATE INDEX IF NOT EXISTS abfactentityflag_idxdate_brin ON olapts.abfactentityflag USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abfactentityflag_idx_entity_hash ON olapts.abfactentityflag USING hash (pkid_);
CREATE INDEX IF NOT EXISTS abfactentityflag_idx_id_hash ON olapts.abfactentityflag USING hash (id_);
CREATE INDEX IF NOT EXISTS abfactentityflag_idx_versionid_hash ON olapts.abfactentityflag USING hash (versionid_);
CREATE INDEX IF NOT EXISTS abfactentityflag_idx_btree ON olapts.abfactentityflag (pkid_,versionid_,sourcepopulateddate_) include(isvalid_,isdeleted_,isvisible_,islatestversion_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);

raise notice '% - Step abfactentityflag_idx - part a end', clock_timestamp(); 

END $$;

-- End Entity Related Tables

-- Financial Related Tables

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABADDEDACCOUNT') THEN
raise notice '% - Step abaddedaccount - part a start', clock_timestamp();
insert into olapts.abaddedaccount
select
pkid_::varchar as dimaddedaccountlatestid_,
id_::varchar as addedaccountid_,
(jsondoc_ ->>'AccountId')::int as accountid ,
(jsondoc_ ->>'ClassId')::int as classid ,
(jsondoc_ ->>'FinancialId')::int as financialid ,
(jsondoc_ ->>'FinTemplateId')::int as fintemplateid ,
(jsondoc_ ->>'FlowId')::int as flowid ,
(jsondoc_ ->>'Id')::bigint as id ,
(jsondoc_ ->>'Label')::varchar as label ,
(jsondoc_ ->>'OnlyInProjection')::int as onlyinprojection ,
(jsondoc_ ->>'PrecedingAccountId')::int as precedingaccountid ,
(jsondoc_ ->>'TypeId')::int as typeid ,
(jsondoc_ ->>'UnitMixId')::bigint as unitmixid ,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
t_::varchar t_,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.addedaccount
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABADDEDACCOUNT')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abaddedaccount - part a end', clock_timestamp();
ELSE
raise notice '% - Step abaddedaccount - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abaddedaccount;
CREATE TABLE olapts.abaddedaccount AS
select
pkid_::varchar as dimaddedaccountlatestid_,
id_::varchar as addedaccountid_,
(jsondoc_ ->>'AccountId')::int as accountid ,
(jsondoc_ ->>'ClassId')::int as classid ,
(jsondoc_ ->>'FinancialId')::int as financialid ,
(jsondoc_ ->>'FinTemplateId')::int as fintemplateid ,
(jsondoc_ ->>'FlowId')::int as flowid ,
(jsondoc_ ->>'Id')::bigint as id ,
(jsondoc_ ->>'Label')::varchar as label ,
(jsondoc_ ->>'OnlyInProjection')::int as onlyinprojection ,
(jsondoc_ ->>'PrecedingAccountId')::int as precedingaccountid ,
(jsondoc_ ->>'TypeId')::int as typeid ,
(jsondoc_ ->>'UnitMixId')::bigint as unitmixid ,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
t_::varchar t_,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.addedaccount
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABADDEDACCOUNT')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abaddedaccount - part b end', clock_timestamp();

--abaddedaccount
raise notice '% - Step abaddedaccount_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abaddedaccount_idx;
DROP INDEX if exists olapts.abaddedaccount_idx2;

CREATE INDEX IF NOT EXISTS abaddedaccount_idx ON olapts.abaddedaccount (addedaccountid_);
CREATE INDEX IF NOT EXISTS abaddedaccount_idx2 ON olapts.abaddedaccount (dimaddedaccountlatestid_);

raise notice '% - Step abaddedaccount_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abaddedaccount - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abaddedaccountflag;
CREATE TABLE olapts.abaddedaccountflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.addedaccount
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABADDEDACCOUNT';
delete from olapts.refreshhistory where tablename = 'ABADDEDACCOUNT';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABADDEDACCOUNT' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABADDEDACCOUNTFLAG';
delete from olapts.refreshhistory where tablename = 'ABADDEDACCOUNTFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABADDEDACCOUNTFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abaddedaccount - part c end', clock_timestamp();

raise notice '% - Step abaddedaccountflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abaddedaccountflag_idx;
DROP INDEX if exists olapts.abaddedaccountflag_idx2;

CREATE INDEX IF NOT EXISTS abaddedaccountflag_idx ON olapts.abaddedaccountflag (id_);
CREATE INDEX IF NOT EXISTS abaddedaccountflag_idx2 ON olapts.abaddedaccountflag (pkid_,versionid_);

raise notice '% - Step abaddedaccountflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABFINANCIAL') THEN
	raise notice '% - Step abfinancial - part a start', clock_timestamp();
	insert into olapts.abfinancial
	select 
	id_,
	pkid_ financialid,
	f.jsondoc_ ->> 'AllowCombinedStmt' allowcombinedstatement,
	f.jsondoc_ ->> 'BatchType' as batchtype,
	f.jsondoc_ ->> 'DisplayRounding' as displayrounding,
	f.jsondoc_ ->> 'EntityId' as entityid,
	f.jsondoc_ ->> 'FinancialTemplate' as financialtemplate,
	l.description modelname_en,
	lg.description modelname_el,
	f.jsondoc_ ->> 'Gc4' as targetcurrency,
	f.jsondoc_ ->> 'Gc7' as invesntoryaccountingmethod,
	f.jsondoc_ ->> 'Gc8' as depreciationmethod,
	f.jsondoc_ ->> 'HiddenAccounts' as hiddenaccounts,
	f.jsondoc_ ->> 'HiddenClasses' as hiddenclasses,
	(f.jsondoc_ ->> 'Primary')::boolean "primary",
	f.jsondoc_ ->> 'ReportRounding' as reportrounding,
	f.jsondoc_ ->> 'RmaSubmissionDate' as rmasubmissiondate,
	f.jsondoc_ ->> 'ShowAccountsWithValue' as showaccountswithvalue,
	f.jsondoc_ ->> 'SubType' as subtype,
	f.jsondoc_ ->> 'IftTc6' as iftfisalyearendmonth,
	f.jsondoc_ ->> 'GiftTc6' as giftfisalyearendmonth,
	f.jsondoc_ ->> 'GiftTc11' industrysector,
	f.jsondoc_ ->> 'GiftTc7' entitytypelegalform,
	f.jsondoc_ ->> 'GiftTc8' listedinase,
	wfid_::varchar as wfid_,
	taskid_::varchar as taskid_,
	versionid_::integer as versionid_, 
	isdeleted_::boolean as isdeleted_,
	islatestversion_::boolean as islatestversion_,
	isvisible_::boolean as isvisible_,
	isvalid_::boolean as isvalid_,
	snapshotid_::integer as snapshotid_,
	contextuserid_::varchar as contextuserid_ ,
	(createdby_)::varchar as createdby_ , 
	(createddate_)::timestamp as createddate_ , 
	(updatedby_)::varchar as updatedby_ , 
	(updateddate_)::timestamp as updateddate_ , 
	t_ t_ ,
	(case when f.updateddate_>f.createddate_ then f.updatedby_ else f.createdby_ end) as sourcepopulatedby_,
	GREATEST(f.updateddate_,f.createddate_) as sourcepopulateddate_
	from madata.financial f 
	LEFT JOIN madata.languagemodels l  
	on ((f.jsondoc_ ->> 'FinancialTemplate')::int = l.modelid and l.languageid = 1 )
	LEFT JOIN madata.languagemodels lg  
	on ((f.jsondoc_ ->> 'FinancialTemplate')::int = lg.modelid and lg.languageid = 15 )
	where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABFINANCIAL')
	and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
	;

	raise notice '% - Step abfinancial - part a end', clock_timestamp();
ELSE
	raise notice '% - Step abfinancial - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abfinancial;
	CREATE TABLE olapts.abfinancial AS
	select 
	id_,
	pkid_ financialid,
	f.jsondoc_ ->> 'AllowCombinedStmt' allowcombinedstatement,
	f.jsondoc_ ->> 'BatchType' as batchtype,
	f.jsondoc_ ->> 'DisplayRounding' as displayrounding,
	f.jsondoc_ ->> 'EntityId' as entityid,
	f.jsondoc_ ->> 'FinancialTemplate' as financialtemplate,
	l.description modelname_en,
	lg.description modelname_el,
	f.jsondoc_ ->> 'Gc4' as targetcurrency,
	f.jsondoc_ ->> 'Gc7' as invesntoryaccountingmethod,
	f.jsondoc_ ->> 'Gc8' as depreciationmethod,
	f.jsondoc_ ->> 'HiddenAccounts' as hiddenaccounts,
	f.jsondoc_ ->> 'HiddenClasses' as hiddenclasses,
	(f.jsondoc_ ->> 'Primary')::boolean "primary",
	f.jsondoc_ ->> 'ReportRounding' as reportrounding,
	f.jsondoc_ ->> 'RmaSubmissionDate' as rmasubmissiondate,
	f.jsondoc_ ->> 'ShowAccountsWithValue' as showaccountswithvalue,
	f.jsondoc_ ->> 'SubType' as subtype,
	f.jsondoc_ ->> 'IftTc6' as iftfisalyearendmonth,
	f.jsondoc_ ->> 'GiftTc6' as giftfisalyearendmonth,
	f.jsondoc_ ->> 'GiftTc11' industrysector,
	f.jsondoc_ ->> 'GiftTc7' entitytypelegalform,
	f.jsondoc_ ->> 'GiftTc8' listedinase,
	wfid_::varchar as wfid_,
	taskid_::varchar as taskid_,
	versionid_::integer as versionid_, 
	isdeleted_::boolean as isdeleted_,
	islatestversion_::boolean as islatestversion_,
	isvisible_::boolean as isvisible_,
	isvalid_::boolean as isvalid_,
	snapshotid_::integer as snapshotid_,
	contextuserid_::varchar as contextuserid_ ,
	(createdby_)::varchar as createdby_ , 
	(createddate_)::timestamp as createddate_ , 
	(updatedby_)::varchar as updatedby_ , 
	(updateddate_)::timestamp as updateddate_ , 
	t_ t_ ,
	(case when f.updateddate_>f.createddate_ then f.updatedby_ else f.createdby_ end) as sourcepopulatedby_,
	GREATEST(f.updateddate_,f.createddate_) as sourcepopulateddate_
	from madata.financial f 
	LEFT JOIN madata.languagemodels l  
	on ((f.jsondoc_ ->> 'FinancialTemplate')::int = l.modelid and l.languageid = 1 )
	LEFT JOIN madata.languagemodels lg  
	on ((f.jsondoc_ ->> 'FinancialTemplate')::int = lg.modelid and lg.languageid = 15 )
	where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABFINANCIAL')
	and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
	;

	raise notice '% - Step abfinancial - part b end', clock_timestamp();
		
		

	--abfinancial
	raise notice '% - Step abfinancial_idx - part a start', clock_timestamp(); 
	DROP INDEX if exists olapts.abfinancial_idx;
	DROP INDEX if exists olapts.abfinancial_idx2;

	CREATE INDEX IF NOT EXISTS abfinancial_idx ON olapts.abfinancial (id_,wfid_);
	CREATE INDEX IF NOT EXISTS abfinancial_idx2 ON olapts.abfinancial (financialid,wfid_,sourcepopulateddate_ DESC NULLS LAST);
		

	raise notice '% - Step abfinancial_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abfinancial - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abfinancialflag;
CREATE TABLE olapts.abfinancialflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.financial
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFINANCIAL';
delete from olapts.refreshhistory where tablename = 'ABFINANCIAL';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFINANCIAL' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFINANCIALFLAG';
delete from olapts.refreshhistory where tablename = 'ABFINANCIALFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFINANCIALFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abfinancial - part c end', clock_timestamp();

raise notice '% - Step abfinancialflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abfinancialflag_idx;
DROP INDEX if exists olapts.abfinancialflag_idx2;

CREATE INDEX IF NOT EXISTS abfinancialflag_idx ON olapts.abfinancialflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abfinancialflag_idx2 ON olapts.abfinancialflag (pkid_,versionid_,wfid_);

raise notice '% - Step abfinancialflag_idx - part a end', clock_timestamp(); 
  
END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABHISTSTMTBALANCE') THEN
	raise notice '% - Step abhiststmtbalance - part a start', clock_timestamp();
	PERFORM olapts.abpopulate_histstmtbalance(false,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABHISTSTMTBALANCE'),false);
	
	raise notice '% - Step abhiststmtbalance - part a end', clock_timestamp();
ELSE
	raise notice '% - Step abhiststmtbalance - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abhiststmtbalance;
	PERFORM olapts.abpopulate_histstmtbalance(true,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABHISTSTMTBALANCE'),true);
	
	raise notice '% - Step abhiststmtbalance - part b end', clock_timestamp();
	
	--abhiststmtbalance
	raise notice '% - Step abhiststmtbalance_idx - part a start', clock_timestamp(); 
	DROP INDEX if exists olapts.abhiststmtbalance_idx;
	DROP INDEX if exists olapts.abhiststmtbalance_idx2;
	DROP INDEX if exists olapts.abhiststmtbalance_idx3;
	DROP INDEX if exists olapts.abhiststmtbalance_idx4;
	DROP INDEX if exists olapts.abhiststmtbalance_idx5;
	DROP INDEX if exists olapts.abhiststmtbalance_idx6;
	DROP INDEX if exists olapts.abhiststmtbalance_idx7;
	
	CREATE INDEX IF NOT EXISTS abhiststmtbalance_idx_pkid_gin ON olapts.abhiststmtbalance USING GIN (id_,pkid_,financialid,statementid,versionid_,wfid_);
	CREATE INDEX IF NOT EXISTS abhiststmtbalance_idx_date_brin ON olapts.abhiststmtbalance USING BRIN (sourcepopulateddate_);
	CREATE INDEX IF NOT EXISTS abhiststmtbalance_idx_pkid_btree_ops ON olapts.abhiststmtbalance ((id_) varchar_pattern_ops,(pkid_),financialid,statementid,accountid,sourcepopulateddate_) include (versionid_,originrounding,historicalstatementid_,v_histstmtbalancelatestid_,isdeleted_,isvalid_,islatestversion_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);


	raise notice '% - Step abhiststmtbalance_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abhiststmtbalance - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abhiststmtbalanceflag;
CREATE TABLE olapts.abhiststmtbalanceflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.historicalstatement
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABHISTSTMTBALANCE';
delete from olapts.refreshhistory where tablename = 'ABHISTSTMTBALANCE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABHISTSTMTBALANCE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABHISTSTMTBALANCEFLAG';
delete from olapts.refreshhistory where tablename = 'ABHISTSTMTBALANCEFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABHISTSTMTBALANCEFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abhiststmtbalance - part c end', clock_timestamp();

raise notice '% - Step abhiststmtbalanceflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abhiststmtbalanceflag_idx;
DROP INDEX if exists olapts.abhiststmtbalanceflag_idx2;
DROP INDEX if exists olapts.abhiststmtbalanceflag_idx3;
DROP INDEX if exists olapts.abhiststmtbalanceflag_idx4;
DROP INDEX if exists olapts.abhiststmtbalanceflag_idx5;

CREATE INDEX IF NOT EXISTS abhiststmtbalanceflag_idx_pkid_gin ON olapts.abhiststmtbalanceflag USING GIN (id_,pkid_,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS abhiststmtbalanceflag_idx_date_brin ON olapts.abhiststmtbalanceflag USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abhiststmtbalanceflag_idx_pkid_btree_ops ON olapts.abhiststmtbalanceflag ((id_) varchar_pattern_ops,(pkid_),sourcepopulateddate_) include (versionid_,isdeleted_,isvalid_,islatestversion_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);

raise notice '% - Step abhiststmtbalanceflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABHISTORICALSTATEMENT') THEN
	raise notice '% - Step abhistoricalstatement - part a start', clock_timestamp();
	PERFORM olapts.abpopulate_historicalstatement(false,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABHISTORICALSTATEMENT'),false);
	
	raise notice '% - Step abhistoricalstatement - part a end', clock_timestamp();
ELSE
	raise notice '% - Step abhistoricalstatement - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abhistoricalstatement;
	PERFORM olapts.abpopulate_historicalstatement(true,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABHISTORICALSTATEMENT'),true);
	
	raise notice '% - Step abhistoricalstatement - part b end', clock_timestamp();
	
	--abhistoricalstatement
	raise notice '% - Step abhistoricalstatement_idx - part a start', clock_timestamp(); 
	DROP INDEX if exists olapts.abhistoricalstatement_idx;
	DROP INDEX if exists olapts.abhistoricalstatement_idx2;
	DROP INDEX if exists olapts.abhistoricalstatement_idx3;
	DROP INDEX if exists olapts.abhistoricalstatement_idx4;
	DROP INDEX if exists olapts.abhistoricalstatement_idx5;
	DROP INDEX if exists olapts.abhistoricalstatement_idx6;
	DROP INDEX if exists olapts.abhistoricalstatement_idx7;
	DROP INDEX if exists olapts.abhistoricalstatement_idx8;
	DROP INDEX if exists olapts.abhistoricalstatement_idx9;
	DROP INDEX if exists olapts.abhistoricalstatement_idx10;
	
	
	CREATE INDEX IF NOT EXISTS abhistoricalstatement_idx_pkid_gin ON olapts.abhistoricalstatement USING GIN (pkid_,financialid,dimhistoricalstatementid_,statementid,versionid_,wfid_);
	CREATE INDEX IF NOT EXISTS abhistoricalstatement_idx_date_brin ON olapts.abhistoricalstatement USING BRIN (sourcepopulateddate_);
	CREATE INDEX IF NOT EXISTS abhistoricalstatement_idx_pkid_btree ON olapts.abhistoricalstatement ((pkid_),financialid,statementid,sourcepopulateddate_) include (versionid_,dimhistoricalstatementid_,isdeleted_,isvalid_,islatestversion_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);

	

	raise notice '% - Step abhistoricalstatement_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abhistoricalstatement - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abhistoricalstatementflag;
CREATE TABLE olapts.abhistoricalstatementflag AS
select 
id_ dimhistoricalstatementid_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.historicalstatement
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABHISTORICALSTATEMENT';
delete from olapts.refreshhistory where tablename = 'ABHISTORICALSTATEMENT';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABHISTORICALSTATEMENT' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABHISTORICALSTATEMENTFLAG';
delete from olapts.refreshhistory where tablename = 'ABHISTORICALSTATEMENTFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABHISTORICALSTATEMENTFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abhistoricalstatement - part c end', clock_timestamp();

raise notice '% - Step abhistoricalstatementflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abhistoricalstatementflag_idx;
DROP INDEX if exists olapts.abhistoricalstatementflag_idx2;

CREATE INDEX IF NOT EXISTS abhistoricalstatementflag_idx_pkid_gin ON olapts.abhistoricalstatementflag USING GIN (pkid_,dimhistoricalstatementid_,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS abhistoricalstatementflag_idx_date_brin ON olapts.abhistoricalstatementflag USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abhistoricalstatementflag_idx_pkid_btree ON olapts.abhistoricalstatementflag ((pkid_),versionid_,sourcepopulateddate_) include (dimhistoricalstatementid_,dimhistoricalstatementid_,isdeleted_,isvalid_,islatestversion_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);

raise notice '% - Step abhistoricalstatementflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABSTATEMENTCONSTANT') THEN
	raise notice '% - Step abstatementconstant - part a start', clock_timestamp();
	PERFORM olapts.abpopulate_statementconstant(false,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABSTATEMENTCONSTANT'),false);
	
	raise notice '% - Step abstatementconstant - part a end', clock_timestamp();
ELSE
	raise notice '% - Step abstatementconstant - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abstatementconstant;
	PERFORM olapts.abpopulate_statementconstant(true,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABSTATEMENTCONSTANT'),true);
	raise notice '% - Step abstatementconstant - part b end', clock_timestamp();

	--abstatementconstant
	raise notice '% - Step abstatementconstant_idx - part a start', clock_timestamp(); 
	DROP INDEX if exists olapts.abstatementconstant_idx;
	DROP INDEX if exists olapts.abstatementconstant_idx2;
	DROP INDEX if exists olapts.abstatementconstant_idx3;
	DROP INDEX if exists olapts.abstatementconstant_idx4;
	DROP INDEX if exists olapts.abstatementconstant_idx5;
	DROP INDEX if exists olapts.abstatementconstant_idx6;

	CREATE INDEX IF NOT EXISTS abstatementconstant_idx_block_gin ON olapts.abstatementconstant USING GIN (financialid,statementid,statementconstid,versionid_,wfid_);
	CREATE INDEX IF NOT EXISTS abstatementconstant_idx_date_brin ON olapts.abstatementconstant USING BRIN (sourcepopulateddate_);
	CREATE INDEX IF NOT EXISTS abstatementconstant_idx_hash ON olapts.abstatementconstant USING hash (financialid);
	CREATE INDEX IF NOT EXISTS abstatementconstant_idx_stmtid_hash ON olapts.abstatementconstant USING hash (statementid);
	CREATE INDEX IF NOT EXISTS abstatementconstant_idx_date_btree ON olapts.abstatementconstant (financialid,statementid,sourcepopulateddate_,statementconstid) INCLUDE (historicalstatementid_,versionid_,wfid_);

	raise notice '% - Step abstatementconstant_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abstatementconstant - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abstatementconstantflag;
CREATE TABLE olapts.abstatementconstantflag AS
select 
id_ historicalstatementid_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.historicalstatement
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSTATEMENTCONSTANT';
delete from olapts.refreshhistory where tablename = 'ABSTATEMENTCONSTANT';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSTATEMENTCONSTANT' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSTATEMENTCONSTANTFLAG';
delete from olapts.refreshhistory where tablename = 'ABSTATEMENTCONSTANTFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSTATEMENTCONSTANTFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abstatementconstant - part c end', clock_timestamp();

raise notice '% - Step abstatementconstantflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abstatementconstantflag_idx;
DROP INDEX if exists olapts.abstatementconstantflag_idx2;

CREATE INDEX IF NOT EXISTS abstatementconstantflag_idx_block_gin ON olapts.abstatementconstantflag USING GIN (pkid_,sourcepopulateddate_,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS abstatementconstantflag_idx_date_brin ON olapts.abstatementconstantflag USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abstatementconstantflag_idx_hash ON olapts.abstatementconstantflag USING hash (pkid_);
CREATE INDEX IF NOT EXISTS abstatementconstantflag_idx_date_btree ON olapts.abstatementconstantflag (pkid_,sourcepopulateddate_) INCLUDE (historicalstatementid_,versionid_,wfid_);

raise notice '% - Step abstatementconstantflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABPROJECTIONSTATEMENT') THEN
raise notice '% - Step abprojectionstatement - part a start', clock_timestamp();
insert into olapts.abprojectionstatement
select 
id_ id_,
pkid_ pkid_,
jsondoc_,
wfid_,
taskid_,
versionid_,
statusid_,
isdeleted_::boolean,
islatestversion_::boolean,
baseversionid_,
contextuserid_,
isvisible_::boolean,
isvalid_::boolean,
snapshotid_,
t_,
createdby_,
createddate_,
updatedby_,
updateddate_,
fkid_projection,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ 
from madata.projectionstatement
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABPROJECTIONSTATEMENT')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abprojectionstatement - part a end', clock_timestamp();
ELSE
raise notice '% - Step abprojectionstatement - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abprojectionstatement;
CREATE TABLE olapts.abprojectionstatement AS
select 
id_ id_,
pkid_ pkid_,
jsondoc_,
wfid_,
taskid_,
versionid_,
statusid_,
isdeleted_::boolean,
islatestversion_::boolean,
baseversionid_,
contextuserid_,
isvisible_::boolean,
isvalid_::boolean,
snapshotid_,
t_,
createdby_,
createddate_,
updatedby_,
updateddate_,
fkid_projection,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_
from madata.projectionstatement
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABPROJECTIONSTATEMENT')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abprojectionstatement - part b end', clock_timestamp();

--abprojectionstatement
raise notice '% - Step abprojectionstatement_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abprojectionstatement_idx;
DROP INDEX if exists olapts.abprojectionstatement_idx2;

CREATE INDEX IF NOT EXISTS abprojectionstatement_idx ON olapts.abprojectionstatement (id_,wfid_);
CREATE INDEX IF NOT EXISTS abprojectionstatement_idx2 ON olapts.abprojectionstatement (pkid_,versionid_,wfid_);	

raise notice '% - Step abprojectionstatement_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abprojectionstatement - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abprojectionstatementflag;
CREATE TABLE olapts.abprojectionstatementflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.projectionstatement
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPROJECTIONSTATEMENT';
delete from olapts.refreshhistory where tablename = 'ABPROJECTIONSTATEMENT';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPROJECTIONSTATEMENT' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPROJECTIONSTATEMENTFLAG';
delete from olapts.refreshhistory where tablename = 'ABPROJECTIONSTATEMENTFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPROJECTIONSTATEMENTFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abprojectionstatement - part c end', clock_timestamp();

raise notice '% - Step abprojectionstatementflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abprojectionstatementflag_idx;
DROP INDEX if exists olapts.abprojectionstatementflag_idx2;

CREATE INDEX IF NOT EXISTS abprojectionstatementflag_idx ON olapts.abprojectionstatementflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abprojectionstatementflag_idx2 ON olapts.abprojectionstatementflag (pkid_,versionid_,wfid_);

raise notice '% - Step abprojectionstatementflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABPEERANALYSIS') THEN
raise notice '% - Step abpeeranalysis - part a start', clock_timestamp();
insert into olapts.abpeeranalysis
select 
id_ dimpeeranalysisid_,
pkid_::varchar as pkid_,
(jsondoc_ ->>'ConsultCategory')::int as consultcategory ,
(jsondoc_ ->>'ConsultDatabaseId')::int as consultdatabaseid ,
(jsondoc_ ->>'ConsultDivisionId')::varchar as consultdivisionid ,
(jsondoc_ ->>'ConsultSIC')::varchar as consultsic ,
(jsondoc_ ->>'ConsultSize')::int as consultsize ,
(jsondoc_ ->>'DefaultCategory')::int as defaultcategory ,
(jsondoc_ ->>'ConsultDatasource')::varchar as consultdatasource ,
(jsondoc_ ->>'DefaultDatabaseId')::int as defaultdatabaseid ,
(jsondoc_ ->>'DefaultDatasource')::varchar as defaultdatasource ,
(jsondoc_ ->>'DefaultDivisionId')::varchar as defaultdivisionid ,
(jsondoc_ ->>'DefaultSIC')::varchar as defaultsic ,
(jsondoc_ ->>'DefaultSize')::int as defaultsize ,
(jsondoc_ ->>'FinancialId')::int as financialid ,
(jsondoc_ ->>'PeerCategory')::int as peercategory ,
(jsondoc_ ->>'PeerDatabaseId')::int as peerdatabaseid ,
(jsondoc_ ->>'PeerDatasource')::varchar as peerdatasource ,
(jsondoc_ ->>'PeerDivisionId')::varchar as peerdivisionid ,
(jsondoc_ ->>'PeerSIC')::varchar as peersic ,
(jsondoc_ ->>'PeerSize')::int as peersize ,
(jsondoc_ ->>'RatioCategory')::int as ratiocategory ,
(jsondoc_ ->>'RatioDatabaseId')::int as ratiodatabaseid ,
(jsondoc_ ->>'RatioDatasource')::varchar as ratiodatasource ,
(jsondoc_ ->>'RatioDivisionId')::varchar as ratiodivisionid ,
(jsondoc_ ->>'RatioSIC')::varchar as ratiosic ,
(jsondoc_ ->>'RatioSize')::int as ratiosize ,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
fkid_financial,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.peeranalysis
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABPEERANALYSIS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abpeeranalysis - part a end', clock_timestamp();
ELSE
raise notice '% - Step abpeeranalysis - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abpeeranalysis;
CREATE TABLE olapts.abpeeranalysis AS
select 
id_ dimpeeranalysisid_,
pkid_::varchar as pkid_,
(jsondoc_ ->>'ConsultCategory')::int as consultcategory ,
(jsondoc_ ->>'ConsultDatabaseId')::int as consultdatabaseid ,
(jsondoc_ ->>'ConsultDivisionId')::varchar as consultdivisionid ,
(jsondoc_ ->>'ConsultSIC')::varchar as consultsic ,
(jsondoc_ ->>'ConsultSize')::int as consultsize ,
(jsondoc_ ->>'DefaultCategory')::int as defaultcategory ,
(jsondoc_ ->>'ConsultDatasource')::varchar as consultdatasource ,
(jsondoc_ ->>'DefaultDatabaseId')::int as defaultdatabaseid ,
(jsondoc_ ->>'DefaultDatasource')::varchar as defaultdatasource ,
(jsondoc_ ->>'DefaultDivisionId')::varchar as defaultdivisionid ,
(jsondoc_ ->>'DefaultSIC')::varchar as defaultsic ,
(jsondoc_ ->>'DefaultSize')::int as defaultsize ,
(jsondoc_ ->>'FinancialId')::int as financialid ,
(jsondoc_ ->>'PeerCategory')::int as peercategory ,
(jsondoc_ ->>'PeerDatabaseId')::int as peerdatabaseid ,
(jsondoc_ ->>'PeerDatasource')::varchar as peerdatasource ,
(jsondoc_ ->>'PeerDivisionId')::varchar as peerdivisionid ,
(jsondoc_ ->>'PeerSIC')::varchar as peersic ,
(jsondoc_ ->>'PeerSize')::int as peersize ,
(jsondoc_ ->>'RatioCategory')::int as ratiocategory ,
(jsondoc_ ->>'RatioDatabaseId')::int as ratiodatabaseid ,
(jsondoc_ ->>'RatioDatasource')::varchar as ratiodatasource ,
(jsondoc_ ->>'RatioDivisionId')::varchar as ratiodivisionid ,
(jsondoc_ ->>'RatioSIC')::varchar as ratiosic ,
(jsondoc_ ->>'RatioSize')::int as ratiosize ,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
fkid_financial,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.peeranalysis
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABPEERANALYSIS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;
raise notice '% - Step abpeeranalysis - part b end', clock_timestamp();

--abpeeranalysis
raise notice '% - Step abpeeranalysis_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abpeeranalysis_idx;
DROP INDEX if exists olapts.abpeeranalysis_idx2;

CREATE INDEX IF NOT EXISTS abpeeranalysis_idx_gin ON olapts.abpeeranalysis USING GIN (dimpeeranalysisid_,pkid_,versionid_,sourcepopulateddate_,wfid_);
CREATE INDEX IF NOT EXISTS abpeeranalysis_idx_dimpeeranalysisid_hash ON olapts.abpeeranalysis USING hash (dimpeeranalysisid_);
CREATE INDEX IF NOT EXISTS abpeeranalysis_idx_date_brin ON olapts.abpeeranalysis USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abpeeranalysis_idx_btree ON olapts.abpeeranalysis (dimpeeranalysisid_,pkid_,versionid_,sourcepopulateddate_) include (financialid,peerdatabaseid,peersic,isdeleted_,isvalid_,isvisible_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);

raise notice '% - Step abpeeranalysis_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abpeeranalysis - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abpeeranalysisflag;
CREATE TABLE olapts.abpeeranalysisflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.peeranalysis
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPEERANALYSIS';
delete from olapts.refreshhistory where tablename = 'ABPEERANALYSIS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPEERANALYSIS' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPEERANALYSISFLAG';
delete from olapts.refreshhistory where tablename = 'ABPEERANALYSISFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPEERANALYSISFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abpeeranalysis - part c end', clock_timestamp();

raise notice '% - Step abpeeranalysisflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abpeeranalysisflag_idx;
DROP INDEX if exists olapts.abpeeranalysisflag_idx2;

CREATE INDEX IF NOT EXISTS abpeeranalysisflag_idx_gin ON olapts.abpeeranalysisflag USING GIN (pkid_,versionid_,sourcepopulateddate_,wfid_);
CREATE INDEX IF NOT EXISTS abpeeranalysisflag_idx_pkid_hash ON olapts.abpeeranalysisflag USING hash (id_);
CREATE INDEX IF NOT EXISTS abpeeranalysisflag_idx_date_brin ON olapts.abpeeranalysisflag USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abpeeranalysisflag_idx_btree ON olapts.abpeeranalysisflag (id_,pkid_,versionid_,sourcepopulateddate_) include (isdeleted_,isvalid_,isvisible_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);

raise notice '% - Step abpeeranalysisflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABUPHISTSTMTFINANCIALS') THEN
raise notice '% - Step abuphiststmtfinancials - part a start', clock_timestamp();
insert into olapts.abuphiststmtfinancials
select 
up.Id_::varchar factuphiststmtfinancialid_,
(up.jsondoc_ ->> 'EntityId')::varchar||':'||(up.jsondoc_ ->> 'FinancialId')::varchar||'|'||(up.jsondoc_ ->> 'StatementId')::varchar as pkid_,
up.jsondoc_->>'EntityId'::varchar entityid,
up.jsondoc_ -> 'Data' ->> 'CustomerName'::varchar customername,
up.jsondoc_ -> 'Data' ->> 'UserId'::varchar userid,
(up.jsondoc_->>'StatementId') statementid,
(up.jsondoc_->>'FinancialId') financialid,
up.jsondoc_->> 'FinancialTemplate'::varchar financialtemplatekey_,
cast(to_char((up.jsondoc_ -> 'Data' ->> 'StatementDate')::date,'YYYYMMDD') as integer) statementdatekey_,
(left(up.jsondoc_ -> 'Data' ->>'StatementYear',4))::numeric statementyear,
up.jsondoc_ ->> 'UserId'::varchar useridkey_,
up.jsondoc_ ->'Data' ->> 'Accountant'::varchar accountant,
up.jsondoc_ ->'Data' ->> 'AccountingStandard'::varchar accountingstandard, 
(up.jsondoc_ -> 'Data' ->>'OfficersComp')::numeric as officerscomp, 
up.jsondoc_ ->'Data' ->> 'Analyst'::varchar analyst, 
up.jsondoc_ ->'Data' ->> 'AuditMethod'::varchar auditmethod, 
((up.jsondoc_ -> 'Data' ->>'ReconcileID')::numeric)::integer as reconcileid,  
up.jsondoc_ ->'Data' ->> 'StatementType'::varchar statementtype,  
up.jsondoc_ ->'Data' ->> 'StatementSource'::varchar statementsource,
(up.jsondoc_ -> 'Data' ->>'StatementMonths')::numeric as statementmonths,
(up.jsondoc_ -> 'Data' ->>'Periods')::numeric as periods,
up.jsondoc_ ->'Data' ->> 'Cash' Cash,
up.jsondoc_ ->'Data' ->> 'EBIT' EBIT,
up.jsondoc_ ->'Data' ->> 'Land' Land,
up.jsondoc_ ->'Data' ->> 'CPLTD' CPLTD,
up.jsondoc_ ->'Data' ->> 'EBITDA' EBITDA,
up.jsondoc_ ->'Data' ->> 'LTDBank' LTDBank,
up.jsondoc_ ->'Data' ->> 'Patents' Patents,
up.jsondoc_ ->'Data' ->> 'CashToTA' CashToTA,
up.jsondoc_ ->'Data' ->> 'Goodwill' Goodwill,
up.jsondoc_ ->'Data' ->> 'LTDOther' LTDOther,
up.jsondoc_ ->'Data' ->> 'PBTToTNW' PBTToTNW,
up.jsondoc_ ->'Data' ->> 'Buildings' Buildings,
up.jsondoc_ ->'Data' ->> 'CPLTDBank' CPLTDBank,
up.jsondoc_ ->'Data' ->> 'DebtToTNW' DebtToTNW,
up.jsondoc_ ->'Data' ->> 'NetProfit' NetProfit,
up.jsondoc_ ->'Data' ->> 'AllOtherCL' AllOtherCL,
up.jsondoc_ ->'Data' ->> 'CPLTDOther' CPLTDOther,
up.jsondoc_ ->'Data' ->> 'MaxInvDays' MaxInvDays,
up.jsondoc_ ->'Data' ->> 'NWAToSales' NWAToSales,
up.jsondoc_ ->'Data' ->> 'OtherTaxes' OtherTaxes,
up.jsondoc_ ->'Data' ->> 'Overdrafts' Overdrafts,
up.jsondoc_ ->'Data' ->> 'QuickRatio' QuickRatio,
up.jsondoc_ ->'Data' ->> 'SalesToTNW' SalesToTNW,
up.jsondoc_ ->'Data' ->> 'CostOfSales' CostOfSales,
up.jsondoc_ ->'Data' ->> 'GrossProfit' GrossProfit,
up.jsondoc_ ->'Data' ->> 'Inventories' Inventories,
up.jsondoc_ ->'Data' ->> 'NetOpProfit' NetOpProfit,
up.jsondoc_ ->'Data' ->> 'OffBSAssets' OffBSAssets,
up.jsondoc_ ->'Data' ->> 'OtherEquity' OtherEquity,
up.jsondoc_ ->'Data' ->> 'ReturnOnTNW' ReturnOnTNW,
up.jsondoc_ ->'Data' ->> 'SalesGrowth' SalesGrowth,
up.jsondoc_ ->'Data' ->> 'TotalAssets' TotalAssets,
up.jsondoc_ ->'Data' ->> 'AuditOpinion' AuditOpinion,
up.jsondoc_ ->'Data' ->> 'CashAfterOps' CashAfterOps,
up.jsondoc_ ->'Data' ->> 'CurrentRatio' CurrentRatio,
up.jsondoc_ ->'Data' ->> 'DebtToEquity' DebtToEquity,
up.jsondoc_ ->'Data' ->> 'EBITDAGrowth' EBITDAGrowth,
up.jsondoc_ ->'Data' ->> 'EBITDAMargin' EBITDAMargin,
up.jsondoc_ ->'Data' ->> 'LongTermDebt' LongTermDebt,
up.jsondoc_ ->'Data' ->> 'NumEmployees' NumEmployees,
up.jsondoc_ ->'Data' ->> 'RDCapitalExp' RDCapitalExp,
up.jsondoc_ ->'Data' ->> 'SharePremium' SharePremium,
up.jsondoc_ ->'Data' ->> 'CashDividends' CashDividends,
up.jsondoc_ ->'Data' ->> 'InventoryDays' InventoryDays,
up.jsondoc_ ->'Data' ->> 'MaxSubordDebt' MaxSubordDebt,
up.jsondoc_ ->'Data' ->> 'MinQuickRatio' MinQuickRatio,
up.jsondoc_ ->'Data' ->> 'NetCashIncome' NetCashIncome,
up.jsondoc_ ->'Data' ->> 'OffBSLeverage' OffBSLeverage,
up.jsondoc_ ->'Data' ->> 'OtherOpIncome' OtherOpIncome,
up.jsondoc_ ->'Data' ->> 'OtherReserves' OtherReserves,
up.jsondoc_ ->'Data' ->> 'PrepaymentsCP' PrepaymentsCP,
up.jsondoc_ ->'Data' ->> 'RestructCosts' RestructCosts,
up.jsondoc_ ->'Data' ->> 'SalesRevenues' SalesRevenues,
up.jsondoc_ ->'Data' ->> 'TradePayables' TradePayables,
up.jsondoc_ ->'Data' ->> 'BadDebtExpense' BadDebtExpense,
up.jsondoc_ ->'Data' ->> 'BorFunToEquity' BorFunToEquity,
up.jsondoc_ ->'Data' ->> 'COGSToTradePay' COGSToTradePay,
up.jsondoc_ ->'Data' ->> 'DebtToNetWorth' DebtToNetWorth,
up.jsondoc_ ->'Data' ->> 'DeferredIntExp' DeferredIntExp,
up.jsondoc_ ->'Data' ->> 'DueToSholderCP' DueToSholderCP,
up.jsondoc_ ->'Data' ->> 'ExtraordIncExp' ExtraordIncExp,
up.jsondoc_ ->'Data' ->> 'GainInvestProp' GainInvestProp,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmFinAct' ICFCFFrmFinAct,
up.jsondoc_ ->'Data' ->> 'ICFDivsPaidFin' ICFDivsPaidFin,
up.jsondoc_ ->'Data' ->> 'ICFDivsRecOper' ICFDivsRecOper,
up.jsondoc_ ->'Data' ->> 'ICFPurchasePPE' ICFPurchasePPE,
up.jsondoc_ ->'Data' ->> 'InterestIncome' InterestIncome,
up.jsondoc_ ->'Data' ->> 'LTDConvertible' LTDConvertible,
up.jsondoc_ ->'Data' ->> 'MaxSalesGrowth' MaxSalesGrowth,
up.jsondoc_ ->'Data' ->> 'MinCashBalance' MinCashBalance,
up.jsondoc_ ->'Data' ->> 'MinCashFlowCov' MinCashFlowCov,
up.jsondoc_ ->'Data' ->> 'MinEarningsCov' MinEarningsCov,
up.jsondoc_ ->'Data' ->> 'MinInterestCov' MinInterestCov,
up.jsondoc_ ->'Data' ->> 'NetFixedAssets' NetFixedAssets,
up.jsondoc_ ->'Data' ->> 'NetIntangibles' NetIntangibles,
up.jsondoc_ ->'Data' ->> 'NetTradeReceiv' NetTradeReceiv,
up.jsondoc_ ->'Data' ->> 'NotesPayableCP' NotesPayableCP,
up.jsondoc_ ->'Data' ->> 'OpLeaseRentExp' OpLeaseRentExp,
up.jsondoc_ ->'Data' ->> 'OthNonOpIncExp' OthNonOpIncExp,
up.jsondoc_ ->'Data' ->> 'OthOperExpProv' OthOperExpProv,
up.jsondoc_ ->'Data' ->> 'PrepaymentsLTP' PrepaymentsLTP,
up.jsondoc_ ->'Data' ->> 'RestrictedCash' RestrictedCash,
up.jsondoc_ ->'Data' ->> 'RestructProvCP' RestructProvCP,
up.jsondoc_ ->'Data' ->> 'ReturnOnAssets' ReturnOnAssets,
up.jsondoc_ ->'Data' ->> 'STLoansPayable' STLoansPayable,
up.jsondoc_ ->'Data' ->> 'TaxProvisionCP' TaxProvisionCP,
up.jsondoc_ ->'Data' ->> 'TotalIncomeTax' TotalIncomeTax,
up.jsondoc_ ->'Data' ->> 'TreasuryShares' TreasuryShares,
up.jsondoc_ ->'Data' ->> 'WorkingCapital' WorkingCapital,
up.jsondoc_ ->'Data' ->> 'AllOthCurrLiabs' AllOthCurrLiabs,
up.jsondoc_ ->'Data' ->> 'AllOtherNCLiabs' AllOtherNCLiabs,
up.jsondoc_ ->'Data' ->> 'COGSToInventory' COGSToInventory,
up.jsondoc_ ->'Data' ->> 'CashEquivalents' CashEquivalents,
up.jsondoc_ ->'Data' ->> 'CashFrTradActiv' CashFrTradActiv,
up.jsondoc_ ->'Data' ->> 'CashGrossMargin' CashGrossMargin,
up.jsondoc_ ->'Data' ->> 'CashPdDivAndInt' CashPdDivAndInt,
up.jsondoc_ ->'Data' ->> 'CashPdToSupplrs' CashPdToSupplrs,
up.jsondoc_ ->'Data' ->> 'DCFCFFrmOperAct' DCFCFFrmOperAct,
up.jsondoc_ ->'Data' ->> 'DCFCFFromFinAct' DCFCFFromFinAct,
up.jsondoc_ ->'Data' ->> 'DCFChgNetIntang' DCFChgNetIntang,
up.jsondoc_ ->'Data' ->> 'DefIncomeTaxPay' DefIncomeTaxPay,
up.jsondoc_ ->'Data' ->> 'DeferredTaxAsts' DeferredTaxAsts,
up.jsondoc_ ->'Data' ->> 'DepAmortToSales' DepAmortToSales,
up.jsondoc_ ->'Data' ->> 'DeprecImpairCOS' DeprecImpairCOS,
up.jsondoc_ ->'Data' ->> 'DerivHedgAstsCP' DerivHedgAstsCP,
up.jsondoc_ ->'Data' ->> 'DerivHedgLiabCP' DerivHedgLiabCP,
up.jsondoc_ ->'Data' ->> 'DueFrmJVPartner' DueFrmJVPartner,
up.jsondoc_ ->'Data' ->> 'DueFrmSholderCP' DueFrmSholderCP,
up.jsondoc_ ->'Data' ->> 'DueToRelPartyCP' DueToRelPartyCP,
up.jsondoc_ ->'Data' ->> 'DueToSholderLTP' DueToSholderLTP,
up.jsondoc_ ->'Data' ->> 'EffTangNetWorth' EffTangNetWorth,
up.jsondoc_ ->'Data' ->> 'FinanceLeasesCP' FinanceLeasesCP,
up.jsondoc_ ->'Data' ->> 'GainDispDiscOps' GainDispDiscOps,
up.jsondoc_ ->'Data' ->> 'GeneralAdminExp' GeneralAdminExp,
up.jsondoc_ ->'Data' ->> 'HedgingReserves' HedgingReserves,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmOperAct' ICFCFFrmOperAct,
up.jsondoc_ ->'Data' ->> 'ICFDepreciation' ICFDepreciation,
up.jsondoc_ ->'Data' ->> 'ICFDivsPaidOper' ICFDivsPaidOper,
up.jsondoc_ ->'Data' ->> 'ICFIncTaxesPaid' ICFIncTaxesPaid,
up.jsondoc_ ->'Data' ->> 'ICFIncomeTaxExp' ICFIncomeTaxExp,
up.jsondoc_ ->'Data' ->> 'ICFProcNCBorrow' ICFProcNCBorrow,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleSubs' ICFProcSaleSubs,
up.jsondoc_ ->'Data' ->> 'InterestExpense' InterestExpense,
up.jsondoc_ ->'Data' ->> 'InterestPayable' InterestPayable,
up.jsondoc_ ->'Data' ->> 'LTDSubordinated' LTDSubordinated,
up.jsondoc_ ->'Data' ->> 'MaxTradePayDays' MaxTradePayDays,
up.jsondoc_ ->'Data' ->> 'MinCurrentRatio' MinCurrentRatio,
up.jsondoc_ ->'Data' ->> 'MinTangNetWorth' MinTangNetWorth,
up.jsondoc_ ->'Data' ->> 'NetCashAfterOps' NetCashAfterOps,
up.jsondoc_ ->'Data' ->> 'NetOpProfGrowth' NetOpProfGrowth,
up.jsondoc_ ->'Data' ->> 'NetOpProfMargin' NetOpProfMargin,
up.jsondoc_ ->'Data' ->> 'NetProfitGrowth' NetProfitGrowth,
up.jsondoc_ ->'Data' ->> 'NetProfitMargin' NetProfitMargin,
up.jsondoc_ ->'Data' ->> 'NetTradeRecDays' NetTradeRecDays,
up.jsondoc_ ->'Data' ->> 'OffrCompToSales' OffrCompToSales,
up.jsondoc_ ->'Data' ->> 'OthEquityResAdj' OthEquityResAdj,
up.jsondoc_ ->'Data' ->> 'OtherOpExpenses' OtherOpExpenses,
up.jsondoc_ ->'Data' ->> 'PermanentEquity' PermanentEquity,
up.jsondoc_ ->'Data' ->> 'PriorPeriodAdjs' PriorPeriodAdjs,
up.jsondoc_ ->'Data' ->> 'PriorYearTaxAdj' PriorYearTaxAdj,
up.jsondoc_ ->'Data' ->> 'ProfitBeforeTax' ProfitBeforeTax,
up.jsondoc_ ->'Data' ->> 'ResearchDevelop' ResearchDevelop,
up.jsondoc_ ->'Data' ->> 'RestructProvLTP' RestructProvLTP,
up.jsondoc_ ->'Data' ->> 'RetainedProfits' RetainedProfits,
up.jsondoc_ ->'Data' ->> 'SalesToTradeRec' SalesToTradeRec,
up.jsondoc_ ->'Data' ->> 'ShareOptionCost' ShareOptionCost,
up.jsondoc_ ->'Data' ->> 'TaxProvisionLTP' TaxProvisionLTP,
up.jsondoc_ ->'Data' ->> 'TotalInventory' totalinventory,
up.jsondoc_ ->'Data' ->> 'TotNonCurrLiabs' TotNonCurrLiabs,
up.jsondoc_ ->'Data' ->> 'TotalCurrAssets' TotalCurrAssets,
up.jsondoc_ ->'Data' ->> 'TotalIntExpense' TotalIntExpense,
up.jsondoc_ ->'Data' ->> 'TotalOpExpenses' TotalOpExpenses,
up.jsondoc_ ->'Data' ->> 'AdjstsScrtznProg' AdjstsScrtznProg,
up.jsondoc_ ->'Data' ->> 'AllOthCurrAssets' AllOthCurrAssets,
up.jsondoc_ ->'Data' ->> 'AllOtherExpenses' AllOtherExpenses,
up.jsondoc_ ->'Data' ->> 'AstsPlgedScrtztn' AstsPlgedScrtztn,
up.jsondoc_ ->'Data' ->> 'AstsRelToDiscOps' AstsRelToDiscOps,
up.jsondoc_ ->'Data' ->> 'BorrFundtoEBITDA' BorrFundtoEBITDA,
up.jsondoc_ ->'Data' ->> 'CPLTDConvertible' CPLTDConvertible,
up.jsondoc_ ->'Data' ->> 'CapitalizedCosts' CapitalizedCosts,
up.jsondoc_ ->'Data' ->> 'CashAftDebtAmort' CashAftDebtAmort,
up.jsondoc_ ->'Data' ->> 'CashClctdFrSales' CashClctdFrSales,
up.jsondoc_ ->'Data' ->> 'CashDivComShares' CashDivComShares,
up.jsondoc_ ->'Data' ->> 'CashFlowCoverage' CashFlowCoverage,
up.jsondoc_ ->'Data' ->> 'CashPdForOpCosts' CashPdForOpCosts,
up.jsondoc_ ->'Data' ->> 'ConstrInProgress' ConstrInProgress,
up.jsondoc_ ->'Data' ->> 'CurrentTaxReceiv' CurrentTaxReceiv,
up.jsondoc_ ->'Data' ->> 'DbtLessSubEffTNW' DbtLessSubEffTNW,
up.jsondoc_ ->'Data' ->> 'DeferredHedgGain' DeferredHedgGain,
up.jsondoc_ ->'Data' ->> 'DeferredIncomeCP' DeferredIncomeCP,
up.jsondoc_ ->'Data' ->> 'DerivHedgAstsLTP' DerivHedgAstsLTP,
up.jsondoc_ ->'Data' ->> 'DerivHedgLiabLTP' DerivHedgLiabLTP,
up.jsondoc_ ->'Data' ->> 'DividendsPayable' DividendsPayable,
up.jsondoc_ ->'Data' ->> 'DueFrmRelPartyCP' DueFrmRelPartyCP,
up.jsondoc_ ->'Data' ->> 'DueFrmSholderLTP' DueFrmSholderLTP,
up.jsondoc_ ->'Data' ->> 'DueToRelPartyLTP' DueToRelPartyLTP,
up.jsondoc_ ->'Data' ->> 'EarningsCoverage' EarningsCoverage,
up.jsondoc_ ->'Data' ->> 'EffectiveTaxRate' EffectiveTaxRate,
up.jsondoc_ ->'Data' ->> 'ExtraordIncExpNC' ExtraordIncExpNC,
up.jsondoc_ ->'Data' ->> 'FMVAdjFnclAssets' FMVAdjFnclAssets,
up.jsondoc_ ->'Data' ->> 'FinanceLeasesLTP' FinanceLeasesLTP,
up.jsondoc_ ->'Data' ->> 'GainLossFnclAsts' GainLossFnclAsts,
up.jsondoc_ ->'Data' ->> 'GoodsAndServices' GoodsAndServices,
up.jsondoc_ ->'Data' ->> 'GrossFixedAssets' GrossFixedAssets,
up.jsondoc_ ->'Data' ->> 'ICFBorrowCostsPd' ICFBorrowCostsPd,
up.jsondoc_ ->'Data' ->> 'ICFChgInDefTaxes' ICFChgInDefTaxes,
up.jsondoc_ ->'Data' ->> 'ICFChgInOtherRec' ICFChgInOtherRec,
up.jsondoc_ ->'Data' ->> 'ICFChgInTaxesPay' ICFChgInTaxesPay,
up.jsondoc_ ->'Data' ->> 'ICFChgInTradePay' ICFChgInTradePay,
up.jsondoc_ ->'Data' ->> 'ICFChgInTradeRec' ICFChgInTradeRec,
up.jsondoc_ ->'Data' ->> 'ICFChgOthWrkgCap' ICFChgOthWrkgCap,
up.jsondoc_ ->'Data' ->> 'ICFImpactChgExch' ICFImpactChgExch,
up.jsondoc_ ->'Data' ->> 'ICFIncomeTaxPaid' ICFIncomeTaxPaid,
up.jsondoc_ ->'Data' ->> 'ICFIntIncRecdInv' ICFIntIncRecdInv,
up.jsondoc_ ->'Data' ->> 'ICFNetProcIssCap' ICFNetProcIssCap,
up.jsondoc_ ->'Data' ->> 'ICFNetProfitLoss' ICFNetProfitLoss,
up.jsondoc_ ->'Data' ->> 'ICFOthNonCashAdj' ICFOthNonCashAdj,
up.jsondoc_ ->'Data' ->> 'ICFOtherInvestCF' ICFOtherInvestCF,
up.jsondoc_ ->'Data' ->> 'ICFRepayNCBorrow' ICFRepayNCBorrow,
up.jsondoc_ ->'Data' ->> 'ICFSalesAssocAff' ICFSalesAssocAff,
up.jsondoc_ ->'Data' ->> 'InterestCoverage' InterestCoverage,
up.jsondoc_ ->'Data' ->> 'MaterialExpenses' MaterialExpenses,
up.jsondoc_ ->'Data' ->> 'MaxCapitalExpend' MaxCapitalExpend,
up.jsondoc_ ->'Data' ->> 'MaxCashDividends' MaxCashDividends,
up.jsondoc_ ->'Data' ->> 'MaxOffBSLeverage' MaxOffBSLeverage,
up.jsondoc_ ->'Data' ->> 'NumOutPrefShares' NumOutPrefShares,
up.jsondoc_ ->'Data' ->> 'OffBSLiabilities' OffBSLiabilities,
up.jsondoc_ ->'Data' ->> 'OthIncAndTaxesPd' OthIncAndTaxesPd,
up.jsondoc_ ->'Data' ->> 'OthNonOpIncExpNC' OthNonOpIncExpNC,
up.jsondoc_ ->'Data' ->> 'OtherFixedAssets' OtherFixedAssets,
up.jsondoc_ ->'Data' ->> 'PBTToTotalAssets' PBTToTotalAssets,
up.jsondoc_ ->'Data' ->> 'PrefShareCapital' PrefShareCapital,
up.jsondoc_ ->'Data' ->> 'ProfitB4ExtItems' ProfitB4ExtItems,
up.jsondoc_ ->'Data' ->> 'RetireBenefitsCP' RetireBenefitsCP,
up.jsondoc_ ->'Data' ->> 'ReturnOnTotEqRes' ReturnOnTotEqRes,
up.jsondoc_ ->'Data' ->> 'STDebtToCurLiabs' STDebtToCurLiabs,
up.jsondoc_ ->'Data' ->> 'SalesToTotalAsts' SalesToTotalAsts,
up.jsondoc_ ->'Data' ->> 'SellMarketingExp' SellMarketingExp,
up.jsondoc_ ->'Data' ->> 'SocSecOthTaxesCP' SocSecOthTaxesCP,
up.jsondoc_ ->'Data' ->> 'StockDividendsNC' StockDividendsNC,
up.jsondoc_ ->'Data' ->> 'SubordDebtEquity' SubordDebtEquity,
up.jsondoc_ ->'Data' ->> 'SubordinatedDebt' SubordinatedDebt,
up.jsondoc_ ->'Data' ->> 'TangibleNetWorth' TangibleNetWorth,
up.jsondoc_ ->'Data' ->> 'TotalAssetGrowth' TotalAssetGrowth,
up.jsondoc_ ->'Data' ->> 'TotalCostofSales' TotalCostofSales,
up.jsondoc_ ->'Data' ->> 'TotalLiabilities' TotalLiabilities,
up.jsondoc_ ->'Data' ->> 'TradePayableDays' TradePayableDays,
up.jsondoc_ ->'Data' ->> 'TradePayablesLTP' TradePayablesLTP,
up.jsondoc_ ->'Data' ->> 'TradeReceivGross' TradeReceivGross,
up.jsondoc_ ->'Data' ->> 'AccumDeprecImpair' AccumDeprecImpair,
up.jsondoc_ ->'Data' ->> 'AmortImpairIntgbl' AmortImpairIntgbl,
up.jsondoc_ ->'Data' ->> 'BorrFundtoTotLiab' BorrFundtoTotLiab,
up.jsondoc_ ->'Data' ->> 'CPLTDSubordinated' CPLTDSubordinated,
up.jsondoc_ ->'Data' ->> 'CashDivPrefShares' CashDivPrefShares,
up.jsondoc_ ->'Data' ->> 'CgsInventoriesWIP' CgsInventoriesWIP,
up.jsondoc_ ->'Data' ->> 'DCFTotAdjustments' DCFTotAdjustments,
up.jsondoc_ ->'Data' ->> 'DecommEnvirCostCP' DecommEnvirCostCP,
up.jsondoc_ ->'Data' ->> 'DeferredIncomeLTP' DeferredIncomeLTP,
up.jsondoc_ ->'Data' ->> 'DeprecImpairOpExp' DeprecImpairOpExp,
up.jsondoc_ ->'Data' ->> 'DerivativesFMVAdj' DerivativesFMVAdj,
up.jsondoc_ ->'Data' ->> 'DueFrmRelPartyLTP' DueFrmRelPartyLTP,
up.jsondoc_ ->'Data' ->> 'EquityAndReserves' EquityAndReserves,
up.jsondoc_ ->'Data' ->> 'ExchGainSaleOfBus' ExchGainSaleOfBus,
up.jsondoc_ ->'Data' ->> 'ExchRatePeriodAvg' ExchRatePeriodAvg,
up.jsondoc_ ->'Data' ->> 'ExchRatePeriodEnd' ExchRatePeriodEnd,
up.jsondoc_ ->'Data' ->> 'ForexTranslEquity' ForexTranslEquity,
up.jsondoc_ ->'Data' ->> 'FurnitureFixtures' FurnitureFixtures,
up.jsondoc_ ->'Data' ->> 'GainDispFixedAsts' GainDispFixedAsts,
up.jsondoc_ ->'Data' ->> 'GrossProfitMargin' GrossProfitMargin,
up.jsondoc_ ->'Data' ->> 'ICFAmortAstImpair' ICFAmortAstImpair,
up.jsondoc_ ->'Data' ->> 'ICFAssocProfShare' ICFAssocProfShare,
up.jsondoc_ ->'Data' ->> 'ICFCFDispFixAsset' ICFCFDispFixAsset,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmInvestAct' ICFCFFrmInvestAct,
up.jsondoc_ ->'Data' ->> 'ICFChgInInventory' ICFChgInInventory,
up.jsondoc_ ->'Data' ->> 'ICFChgOthCurrAsts' ICFChgOthCurrAsts,
up.jsondoc_ ->'Data' ->> 'ICFDivsRecdInvest' ICFDivsRecdInvest,
up.jsondoc_ ->'Data' ->> 'ICFIntIncRecdOper' ICFIntIncRecdOper,
up.jsondoc_ ->'Data' ->> 'ICFInvestAssocAff' ICFInvestAssocAff,
up.jsondoc_ ->'Data' ->> 'ICFNetProfitB4Tax' ICFNetProfitB4Tax,
up.jsondoc_ ->'Data' ->> 'ICFOthFinancingCF' ICFOthFinancingCF,
up.jsondoc_ ->'Data' ->> 'ICFPurchOtherAsts' ICFPurchOtherAsts,
up.jsondoc_ ->'Data' ->> 'ICFPurchaseSaleTS' ICFPurchaseSaleTS,
up.jsondoc_ ->'Data' ->> 'InvestInJVPartner' InvestInJVPartner,
up.jsondoc_ ->'Data' ->> 'InvestmentInAssoc' InvestmentInAssoc,
up.jsondoc_ ->'Data' ->> 'InvestmentPropNet' InvestmentPropNet,
up.jsondoc_ ->'Data' ->> 'MinPensionLiabAdj' MinPensionLiabAdj,
up.jsondoc_ ->'Data' ->> 'MinReturnOnAssets' MinReturnOnAssets,
up.jsondoc_ ->'Data' ->> 'MinReturnOnEquity' MinReturnOnEquity,
up.jsondoc_ ->'Data' ->> 'MinWorkingCapital' MinWorkingCapital,
up.jsondoc_ ->'Data' ->> 'MinorityIntEquity' MinorityIntEquity,
up.jsondoc_ ->'Data' ->> 'MinorityInterests' MinorityInterests,
up.jsondoc_ ->'Data' ->> 'NetInterestIncExp' NetInterestIncExp,
up.jsondoc_ ->'Data' ->> 'NetOthFinanIncExp' NetOthFinanIncExp,
up.jsondoc_ ->'Data' ->> 'OthDebtServiceExp' OthDebtServiceExp,
up.jsondoc_ ->'Data' ->> 'OthFnclAstIncDvds' OthFnclAstIncDvds,
up.jsondoc_ ->'Data' ->> 'OthNonCurrentAsts' OthNonCurrentAsts,
up.jsondoc_ ->'Data' ->> 'OthNonIncRelTaxes' OthNonIncRelTaxes,
up.jsondoc_ ->'Data' ->> 'OthReceivablesLTP' OthReceivablesLTP,
up.jsondoc_ ->'Data' ->> 'OtherProvisionsCP' OtherProvisionsCP,
up.jsondoc_ ->'Data' ->> 'PBTToTotEquityRes' PBTToTotEquityRes,
up.jsondoc_ ->'Data' ->> 'PersonnelBenefExp' PersonnelBenefExp,
up.jsondoc_ ->'Data' ->> 'PlantAndEquipment' PlantAndEquipment,
up.jsondoc_ ->'Data' ->> 'ProfitB4TaxMargin' ProfitB4TaxMargin,
up.jsondoc_ ->'Data' ->> 'RetireBenefitsLTP' RetireBenefitsLTP,
up.jsondoc_ ->'Data' ->> 'RlzdForexTranslGL' RlzdForexTranslGL,
up.jsondoc_ ->'Data' ->> 'STOthLoansPayable' STOthLoansPayable,
up.jsondoc_ ->'Data' ->> 'SalestoWorkingCap' SalestoWorkingCap,
up.jsondoc_ ->'Data' ->> 'SecurOthFinAstsCP' SecurOthFinAstsCP,
up.jsondoc_ ->'Data' ->> 'ShareOptionCostCP' ShareOptionCostCP,
up.jsondoc_ ->'Data' ->> 'SocSecOthTaxesLTP' SocSecOthTaxesLTP,
up.jsondoc_ ->'Data' ->> 'StartUpCapitalExp' StartUpCapitalExp,
up.jsondoc_ ->'Data' ->> 'SustainableGrowth' SustainableGrowth,
up.jsondoc_ ->'Data' ->> 'TotEquityResLiabs' TotEquityResLiabs,
up.jsondoc_ ->'Data' ->> 'TotEquityReserves' TotEquityReserves,
up.jsondoc_ ->'Data' ->> 'TotLiabtoTotAsset' TotLiabtoTotAsset,
up.jsondoc_ ->'Data' ->> 'TotOthEqResIncExp' TotOthEqResIncExp,
up.jsondoc_ ->'Data' ->> 'TotalCurrentLiabs' TotalCurrentLiabs,
up.jsondoc_ ->'Data' ->> 'UnrealGainFixAsts' UnrealGainFixAsts,
up.jsondoc_ ->'Data' ->> 'UnrealGainInvests' UnrealGainInvests,
up.jsondoc_ ->'Data' ->> 'AccIntgbleAstAmort' AccIntgbleAstAmort,
up.jsondoc_ ->'Data' ->> 'AccuOthEqtyRsrvInc' AccuOthEqtyRsrvInc,
up.jsondoc_ ->'Data' ->> 'AccumGoodwilImpair' AccumGoodwilImpair,
up.jsondoc_ ->'Data' ->> 'AllOthNonCurrAssts' AllOthNonCurrAssts,
up.jsondoc_ ->'Data' ->> 'AllowForDoubtAccts' AllowForDoubtAccts,
up.jsondoc_ ->'Data' ->> 'BillingsInExcCosts' BillingsInExcCosts,
up.jsondoc_ ->'Data' ->> 'BorrFundtoEffTgWth' BorrFundtoEffTgWth,
up.jsondoc_ ->'Data' ->> 'CapAndRestrReserve' CapAndRestrReserve,
up.jsondoc_ ->'Data' ->> 'CapitalExpenditure' CapitalExpenditure,
up.jsondoc_ ->'Data' ->> 'CashAndEquivalents' CashAndEquivalents,
up.jsondoc_ ->'Data' ->> 'CommonShareCapital' CommonShareCapital,
up.jsondoc_ ->'Data' ->> 'CurrentIncomeTaxes' CurrentIncomeTaxes,
up.jsondoc_ ->'Data' ->> 'CustomerAdvancesCP' CustomerAdvancesCP,
up.jsondoc_ ->'Data' ->> 'DCFBegOfPeriodCash' DCFBegOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'DCFCFFromInvestAct' DCFCFFromInvestAct,
up.jsondoc_ ->'Data' ->> 'DCFChgNetFixAssets' DCFChgNetFixAssets,
up.jsondoc_ ->'Data' ->> 'DCFEndOfPeriodCash' DCFEndOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'DCFTotMovementCash' DCFTotMovementCash,
up.jsondoc_ ->'Data' ->> 'DecommEnvirCostLTP' DecommEnvirCostLTP,
up.jsondoc_ ->'Data' ->> 'DeferredIncTaxesNC' DeferredIncTaxesNC,
up.jsondoc_ ->'Data' ->> 'DividendPayoutRate' DividendPayoutRate,
up.jsondoc_ ->'Data' ->> 'ExpensesOwnWorkCap' ExpensesOwnWorkCap,
up.jsondoc_ ->'Data' ->> 'GrantsAndSubsidies' GrantsAndSubsidies,
up.jsondoc_ ->'Data' ->> 'ICFAcqSubNetCshAcq' ICFAcqSubNetCshAcq,
up.jsondoc_ ->'Data' ->> 'ICFBegOfPeriodCash' ICFBegOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'ICFCFDispFnclAsset' ICFCFDispFnclAsset,
up.jsondoc_ ->'Data' ->> 'ICFCFExtItemInvest' ICFCFExtItemInvest,
up.jsondoc_ ->'Data' ->> 'ICFCFExtraItemsFin' ICFCFExtraItemsFin,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmExtrItemOp' ICFCFFrmExtrItemOp,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmHedgingFin' ICFCFFrmHedgingFin,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmHedingOper' ICFCFFrmHedingOper,
up.jsondoc_ ->'Data' ->> 'ICFCFHedgingInvest' ICFCFHedgingInvest,
up.jsondoc_ ->'Data' ->> 'ICFChgFnclInstrTrd' ICFChgFnclInstrTrd,
up.jsondoc_ ->'Data' ->> 'ICFChgInCurrBorrow' ICFChgInCurrBorrow,
up.jsondoc_ ->'Data' ->> 'ICFChgInOthCurLiab' ICFChgInOthCurLiab,
up.jsondoc_ ->'Data' ->> 'ICFChgInProvisions' ICFChgInProvisions,
up.jsondoc_ ->'Data' ->> 'ICFChgPostEmplBnft' ICFChgPostEmplBnft,
up.jsondoc_ ->'Data' ->> 'ICFChgPrepayDefAst' ICFChgPrepayDefAst,
up.jsondoc_ ->'Data' ->> 'ICFDvdsPdMinShlder' ICFDvdsPdMinShlder,
up.jsondoc_ ->'Data' ->> 'ICFEndOfPeriodCash' ICFEndOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'ICFInterestExpense' ICFInterestExpense,
up.jsondoc_ ->'Data' ->> 'ICFInterestPaidFin' ICFInterestPaidFin,
up.jsondoc_ ->'Data' ->> 'ICFIssueCostNCBorr' ICFIssueCostNCBorr,
up.jsondoc_ ->'Data' ->> 'ICFNetFrgnExchDiff' ICFNetFrgnExchDiff,
up.jsondoc_ ->'Data' ->> 'ICFOthAftTaxIncExp' ICFOthAftTaxIncExp,
up.jsondoc_ ->'Data' ->> 'ICFPayFinLeaseLiab' ICFPayFinLeaseLiab,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleInvProp' ICFProcSaleInvProp,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleOthAsts' ICFProcSaleOthAsts,
up.jsondoc_ ->'Data' ->> 'ICFProcSlIntblAsts' ICFProcSlIntblAsts,
up.jsondoc_ ->'Data' ->> 'ICFProceedsSalePPE' ICFProceedsSalePPE,
up.jsondoc_ ->'Data' ->> 'ICFPurchFnclAssets' ICFPurchFnclAssets,
up.jsondoc_ ->'Data' ->> 'ICFPurchIntgblAsts' ICFPurchIntgblAsts,
up.jsondoc_ ->'Data' ->> 'ICFPurchInvestProp' ICFPurchInvestProp,
up.jsondoc_ ->'Data' ->> 'ICFTotMovementCash' ICFTotMovementCash,
up.jsondoc_ ->'Data' ->> 'ICFTranslAdjRelCsh' ICFTranslAdjRelCsh,
up.jsondoc_ ->'Data' ->> 'ICFUnexplAdjToCash' ICFUnexplAdjToCash,
up.jsondoc_ ->'Data' ->> 'IncFrmJointVenture' IncFrmJointVenture,
up.jsondoc_ ->'Data' ->> 'IncLossFrmRelParty' IncLossFrmRelParty,
up.jsondoc_ ->'Data' ->> 'IncomeTaxesPayable' IncomeTaxesPayable,
up.jsondoc_ ->'Data' ->> 'InvDaysExclCOSDepr' InvDaysExclCOSDepr,
up.jsondoc_ ->'Data' ->> 'LeaseholdImprvmnts' LeaseholdImprvmnts,
up.jsondoc_ ->'Data' ->> 'LiabsToEnterprises' LiabsToEnterprises,
up.jsondoc_ ->'Data' ->> 'MaxDebtToTangWorth' MaxDebtToTangWorth,
up.jsondoc_ ->'Data' ->> 'MaxNetTradeRecDays' MaxNetTradeRecDays,
up.jsondoc_ ->'Data' ->> 'MinNetProfitMargin' MinNetProfitMargin,
up.jsondoc_ ->'Data' ->> 'NetFixedAssetToTNW' NetFixedAssetToTNW,
up.jsondoc_ ->'Data' ->> 'NetProfDeprtoCPLTD' NetProfDeprtoCPLTD,
up.jsondoc_ ->'Data' ->> 'NumOutCommonShares' NumOutCommonShares,
up.jsondoc_ ->'Data' ->> 'OpLeaseCommitments' OpLeaseCommitments,
up.jsondoc_ ->'Data' ->> 'OpLeaseReceivables' OpLeaseReceivables,
up.jsondoc_ ->'Data' ->> 'OperExpExclDepAmor' OperExpExclDepAmor,
up.jsondoc_ ->'Data' ->> 'OthAdjRetainedProf' OthAdjRetainedProf,
up.jsondoc_ ->'Data' ->> 'OthIntangibleAsset' OthIntangibleAsset,
up.jsondoc_ ->'Data' ->> 'OthNonCurrentLiabs' OthNonCurrentLiabs,
up.jsondoc_ ->'Data' ->> 'OtherCurrentAssets' OtherCurrentAssets,
up.jsondoc_ ->'Data' ->> 'OtherIncomeExpense' OtherIncomeExpense,
up.jsondoc_ ->'Data' ->> 'OtherProvisionsLTP' OtherProvisionsLTP,
up.jsondoc_ ->'Data' ->> 'OtherReceivablesCP' OtherReceivablesCP,
up.jsondoc_ ->'Data' ->> 'ProvRetirementCost' ProvRetirementCost,
up.jsondoc_ ->'Data' ->> 'RentsRoyaltyIncome' RentsRoyaltyIncome,
up.jsondoc_ ->'Data' ->> 'RevaluationReserve' RevaluationReserve,
up.jsondoc_ ->'Data' ->> 'RtmntPlanActuarial' RtmntPlanActuarial,
up.jsondoc_ ->'Data' ->> 'STBankLoansPayable' STBankLoansPayable,
up.jsondoc_ ->'Data' ->> 'SecurOthFinAstsLTP' SecurOthFinAstsLTP,
up.jsondoc_ ->'Data' ->> 'ShareOptionCostLTP' ShareOptionCostLTP,
up.jsondoc_ ->'Data' ->> 'TotAdjRetainProfit' TotAdjRetainProfit,
up.jsondoc_ ->'Data' ->> 'TotalNonCurrAssets' TotalNonCurrAssets,
up.jsondoc_ ->'Data' ->> 'TransferToReserves' TransferToReserves,
up.jsondoc_ ->'Data' ->> 'UnrealGainInvestPr' UnrealGainInvestPr,
up.jsondoc_ ->'Data' ->> 'VehAndTranspoEquip' VehAndTranspoEquip,
up.jsondoc_ ->'Data' ->> 'CustomerAdvancesLTP' CustomerAdvancesLTP,
up.jsondoc_ ->'Data' ->> 'ICFInterestPaidOper' ICFInterestPaidOper,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleFnclAsts' ICFProcSaleFnclAsts,
up.jsondoc_ ->'Data' ->> 'SalesToNetFixedAsts' SalesToNetFixedAsts,
up.jsondoc_ ->'Data' ->> 'ShortTermSecurities' ShortTermSecurities,
up.jsondoc_ ->'Data' ->> 'UCAChgInFixedAssets' UCAChgInFixedAssets,
up.jsondoc_ ->'Data' ->> 'CashinHandandAtBanks' CashinHandandAtBanks,
up.jsondoc_ ->'Data' ->> 'GoodwillAmortization' GoodwillAmortization,
up.jsondoc_ ->'Data' ->> 'ICFCashPdtoEmployees' ICFCashPdtoEmployees,
up.jsondoc_ ->'Data' ->> 'ICFCashPdtoSuppliers' ICFCashPdtoSuppliers,
up.jsondoc_ ->'Data' ->> 'ICFMinorityInterests' ICFMinorityInterests,
up.jsondoc_ ->'Data' ->> 'SpecItemswEquityChar' SpecItemswEquityChar,
up.jsondoc_ ->'Data' ->> 'ConcessLicenseTrdmrks' ConcessLicenseTrdmrks,
up.jsondoc_ ->'Data' ->> 'ICFCashRecfrmCustomers' ICFCashRecfrmCustomers,
up.jsondoc_ ->'Data' ->> 'SubscribedCapCallNotPd' SubscribedCapCallNotPd,
up.jsondoc_ ->'Data' ->> 'TradePayDaysExclCOSDepr' TradePayDaysExclCOSDepr,
up.jsondoc_ ->'Data' ->> 'ICFCashFlowFromOpActDirect' ICFCashFlowFromOpActDirect,
up.jsondoc_ ->'Data' ->> 'Sales' Sales,
up.jsondoc_ ->'Data' ->> 'Deposits' Deposits,
up.jsondoc_ ->'Data' ->> 'CashAndCashEquivalents' CashAndCashEquivalents,
up.jsondoc_ ->'Data' ->> 'Uni_AccountReceivablesDays' Uni_AccountReceivablesDays,
up.jsondoc_ ->'Data' ->> 'Uni_AccountsPayableDays' Uni_AccountsPayableDays,
up.jsondoc_ ->'Data' ->> 'Uni_BorrowedFunds' Uni_BorrowedFunds,
up.jsondoc_ ->'Data' ->> 'Uni_CashInHandAndAtBanks' Uni_CashInHandAndAtBanks,
up.jsondoc_ ->'Data' ->> 'Uni_CashLiquidity' Uni_CashLiquidity,
up.jsondoc_ ->'Data' ->> 'Uni_CashToCurrentLiabilities' Uni_CashToCurrentLiabilities,
up.jsondoc_ ->'Data' ->> 'Uni_ConstructionContracts' Uni_ConstructionContracts,
up.jsondoc_ ->'Data' ->> 'Uni_CostsΙnΕxcellΟfΒillings' Uni_CostsΙnΕxcellΟfΒillings,
up.jsondoc_ ->'Data' ->> 'Uni_CurrentRatio' Uni_CurrentRatio,
up.jsondoc_ ->'Data' ->> 'Uni_DebtCoverageRatio' Uni_DebtCoverageRatio,
up.jsondoc_ ->'Data' ->> 'Uni_DebtToEbitda' Uni_DebtToEbitda,
up.jsondoc_ ->'Data' ->> 'Uni_DebtToSales' Uni_DebtToSales,
up.jsondoc_ ->'Data' ->> 'Uni_DeprecAndImpairment' Uni_DeprecAndImpairment,
up.jsondoc_ ->'Data' ->> 'Uni_DeprecImpairment' Uni_DeprecImpairment,
up.jsondoc_ ->'Data' ->> 'Uni_DividendsPayable' Uni_DividendsPayable,
up.jsondoc_ ->'Data' ->> 'Uni_EBITDA' Uni_EBITDA,
up.jsondoc_ ->'Data' ->> 'Uni_EBTDA' Uni_EBTDA,
up.jsondoc_ ->'Data' ->> 'Uni_EbtdaToDebt' Uni_EbtdaToDebt,
up.jsondoc_ ->'Data' ->> 'Uni_EbtdaToSales' Uni_EbtdaToSales,
up.jsondoc_ ->'Data' ->> 'Uni_FinancialLeverage' Uni_FinancialLeverage,
up.jsondoc_ ->'Data' ->> 'Uni_FreeFlowsToSales' Uni_FreeFlowsToSales,
up.jsondoc_ ->'Data' ->> 'Uni_GrossProfit' Uni_GrossProfit,
up.jsondoc_ ->'Data' ->> 'Uni_GrossProfitMargin' Uni_GrossProfitMargin,
up.jsondoc_ ->'Data' ->> 'Uni_InterestCoverage' Uni_InterestCoverage,
up.jsondoc_ ->'Data' ->> 'Uni_InterestExpense' Uni_InterestExpense,
up.jsondoc_ ->'Data' ->> 'Uni_InterestIncome' Uni_InterestIncome,
up.jsondoc_ ->'Data' ->> 'Uni_Inventories' Uni_Inventories,
up.jsondoc_ ->'Data' ->> 'Uni_InventoryDays' Uni_InventoryDays,
up.jsondoc_ ->'Data' ->> 'Uni_InvestementsInPropertyCp' Uni_InvestementsInPropertyCp,
up.jsondoc_ ->'Data' ->> 'Uni_NetMarginToReserves' Uni_NetMarginToReserves,
up.jsondoc_ ->'Data' ->> 'Uni_NetMarginToTotalAssets' Uni_NetMarginToTotalAssets,
up.jsondoc_ ->'Data' ->> 'Uni_NetTradeReceivables' Uni_NetTradeReceivables,
up.jsondoc_ ->'Data' ->> 'Uni_OperatingFlows' Uni_OperatingFlows,
up.jsondoc_ ->'Data' ->> 'Uni_OtherCurrentAssets' Uni_OtherCurrentAssets,
up.jsondoc_ ->'Data' ->> 'Uni_OtherCurrentLiabilities' Uni_OtherCurrentLiabilities,
up.jsondoc_ ->'Data' ->> 'Uni_OtherDebtors' Uni_OtherDebtors,
up.jsondoc_ ->'Data' ->> 'Uni_PrepaymentsByCustomers' Uni_PrepaymentsByCustomers,
up.jsondoc_ ->'Data' ->> 'Uni_PrepaymentsCp' Uni_PrepaymentsCp,
up.jsondoc_ ->'Data' ->> 'Uni_Profit' Uni_Profit,
up.jsondoc_ ->'Data' ->> 'Uni_ProfitLossBeforeExtraordinaryItems' Uni_ProfitLossBeforeExtraordinaryItems,
up.jsondoc_ ->'Data' ->> 'Uni_ProfitLossBeforeTax' Uni_ProfitLossBeforeTax,
up.jsondoc_ ->'Data' ->> 'Uni_QuickRatio' Uni_QuickRatio,
up.jsondoc_ ->'Data' ->> 'Uni_Receivables' Uni_Receivables,
up.jsondoc_ ->'Data' ->> 'Uni_ReceivablesToSales' Uni_ReceivablesToSales,
up.jsondoc_ ->'Data' ->> 'Uni_Sales' Uni_Sales,
up.jsondoc_ ->'Data' ->> 'Uni_SalesGrowth' Uni_SalesGrowth,
up.jsondoc_ ->'Data' ->> 'Uni_SalesRevenue' Uni_SalesRevenue,
up.jsondoc_ ->'Data' ->> 'Uni_SalesToAssets' Uni_SalesToAssets,
up.jsondoc_ ->'Data' ->> 'Uni_Taxes' Uni_Taxes,
up.jsondoc_ ->'Data' ->> 'Uni_TotalCostOfSales' Uni_TotalCostOfSales,
up.jsondoc_ ->'Data' ->> 'Uni_TotalCurrentAssets' Uni_TotalCurrentAssets,
up.jsondoc_ ->'Data' ->> 'Uni_TotalCurrentLiabilities ' Uni_TotalCurrentLiabilities ,
up.jsondoc_ ->'Data' ->> 'Uni_TotalEquityAndReserves' Uni_TotalEquityAndReserves,
up.jsondoc_ ->'Data' ->> 'Uni_TotalFixedAssets' Uni_TotalFixedAssets,
up.jsondoc_ ->'Data' ->> 'Uni_TotalLiabilitiesToNetWorth' Uni_TotalLiabilitiesToNetWorth,
up.jsondoc_ ->'Data' ->> 'Uni_TotalNonCurrentAssets' Uni_TotalNonCurrentAssets,
up.jsondoc_ ->'Data' ->> 'Uni_TotalNonCurrentLiabilities' Uni_TotalNonCurrentLiabilities,
up.jsondoc_ ->'Data' ->> 'Uni_TradePayablesCP' Uni_TradePayablesCP,
up.jsondoc_ ->'Data' ->> 'Uni_WorkingCapital' Uni_WorkingCapital,
up.jsondoc_ ->'Data' ->> 'Uni_WorkingCapitalToSales' Uni_WorkingCapitalToSales,
(up.jsondoc_ ->'Data' ->> 'StatementDate')::varchar(10)::date StatementDate,
--up.jsondoc_ ->'Data' ->> 'HistStmtVersionId' HistStmtVersionId,
up.wfid_::varchar wfid_,
up.taskid_::varchar taskid_,
up.versionid_::integer versionid_, 
up.statusid_::integer statusid_,
up.isdeleted_::boolean isdeleted_,
up.islatestversion_::boolean islatestversion_,
up.isvisible_::boolean isvisible_,
up.isvalid_::boolean isvalid_,
up.snapshotid_::integer snapshotid_,
up.contextuserid_::varchar contextuserid_ ,
up.createdby_::varchar createdby_ , 
up.createddate_::timestamp createddate_ , 
up.updatedby_::varchar updatedby_ , 
up.updateddate_::timestamp updateddate_ , 
t_ t_ ,
(case when up.updateddate_>up.createddate_ then up.updatedby_ else up.createdby_ end) as sourcepopulatedby_,
GREATEST(up.updateddate_,up.createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.uphiststmtfinancials up
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPHISTSTMTFINANCIALS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abuphiststmtfinancials - part a end', clock_timestamp();
ELSE
raise notice '% - Step abuphiststmtfinancials - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abuphiststmtfinancials;
CREATE TABLE olapts.abuphiststmtfinancials AS
select 
up.Id_::varchar factuphiststmtfinancialid_,
(up.jsondoc_ ->> 'EntityId')::varchar||':'||(up.jsondoc_ ->> 'FinancialId')::varchar||'|'||(up.jsondoc_ ->> 'StatementId')::varchar as pkid_,
up.jsondoc_->>'EntityId'::varchar entityid,
up.jsondoc_ -> 'Data' ->> 'CustomerName'::varchar customername,
up.jsondoc_ -> 'Data' ->> 'UserId'::varchar userid,
(up.jsondoc_->>'StatementId') statementid,
(up.jsondoc_->>'FinancialId') financialid,
up.jsondoc_->> 'FinancialTemplate'::varchar financialtemplatekey_,
cast(to_char((up.jsondoc_ -> 'Data' ->> 'StatementDate')::date,'YYYYMMDD') as integer) statementdatekey_,
(left(up.jsondoc_ -> 'Data' ->>'StatementYear',4))::numeric statementyear,
up.jsondoc_ ->> 'UserId'::varchar useridkey_,
up.jsondoc_ ->'Data' ->> 'Accountant'::varchar accountant,
up.jsondoc_ ->'Data' ->> 'AccountingStandard'::varchar accountingstandard, 
(up.jsondoc_ -> 'Data' ->>'OfficersComp')::numeric as officerscomp, 
up.jsondoc_ ->'Data' ->> 'Analyst'::varchar analyst, 
up.jsondoc_ ->'Data' ->> 'AuditMethod'::varchar auditmethod, 
((up.jsondoc_ -> 'Data' ->>'ReconcileID')::numeric)::integer as reconcileid,  
up.jsondoc_ ->'Data' ->> 'StatementType'::varchar statementtype,  
up.jsondoc_ ->'Data' ->> 'StatementSource'::varchar statementsource,
(up.jsondoc_ -> 'Data' ->>'StatementMonths')::numeric as statementmonths,
(up.jsondoc_ -> 'Data' ->>'Periods')::numeric as periods,
up.jsondoc_ ->'Data' ->> 'Cash' Cash,
up.jsondoc_ ->'Data' ->> 'EBIT' EBIT,
up.jsondoc_ ->'Data' ->> 'Land' Land,
up.jsondoc_ ->'Data' ->> 'CPLTD' CPLTD,
up.jsondoc_ ->'Data' ->> 'EBITDA' EBITDA,
up.jsondoc_ ->'Data' ->> 'LTDBank' LTDBank,
up.jsondoc_ ->'Data' ->> 'Patents' Patents,
up.jsondoc_ ->'Data' ->> 'CashToTA' CashToTA,
up.jsondoc_ ->'Data' ->> 'Goodwill' Goodwill,
up.jsondoc_ ->'Data' ->> 'LTDOther' LTDOther,
up.jsondoc_ ->'Data' ->> 'PBTToTNW' PBTToTNW,
up.jsondoc_ ->'Data' ->> 'Buildings' Buildings,
up.jsondoc_ ->'Data' ->> 'CPLTDBank' CPLTDBank,
up.jsondoc_ ->'Data' ->> 'DebtToTNW' DebtToTNW,
up.jsondoc_ ->'Data' ->> 'NetProfit' NetProfit,
up.jsondoc_ ->'Data' ->> 'AllOtherCL' AllOtherCL,
up.jsondoc_ ->'Data' ->> 'CPLTDOther' CPLTDOther,
up.jsondoc_ ->'Data' ->> 'MaxInvDays' MaxInvDays,
up.jsondoc_ ->'Data' ->> 'NWAToSales' NWAToSales,
up.jsondoc_ ->'Data' ->> 'OtherTaxes' OtherTaxes,
up.jsondoc_ ->'Data' ->> 'Overdrafts' Overdrafts,
up.jsondoc_ ->'Data' ->> 'QuickRatio' QuickRatio,
up.jsondoc_ ->'Data' ->> 'SalesToTNW' SalesToTNW,
up.jsondoc_ ->'Data' ->> 'CostOfSales' CostOfSales,
up.jsondoc_ ->'Data' ->> 'GrossProfit' GrossProfit,
up.jsondoc_ ->'Data' ->> 'Inventories' Inventories,
up.jsondoc_ ->'Data' ->> 'NetOpProfit' NetOpProfit,
up.jsondoc_ ->'Data' ->> 'OffBSAssets' OffBSAssets,
up.jsondoc_ ->'Data' ->> 'OtherEquity' OtherEquity,
up.jsondoc_ ->'Data' ->> 'ReturnOnTNW' ReturnOnTNW,
up.jsondoc_ ->'Data' ->> 'SalesGrowth' SalesGrowth,
up.jsondoc_ ->'Data' ->> 'TotalAssets' TotalAssets,
up.jsondoc_ ->'Data' ->> 'AuditOpinion' AuditOpinion,
up.jsondoc_ ->'Data' ->> 'CashAfterOps' CashAfterOps,
up.jsondoc_ ->'Data' ->> 'CurrentRatio' CurrentRatio,
up.jsondoc_ ->'Data' ->> 'DebtToEquity' DebtToEquity,
up.jsondoc_ ->'Data' ->> 'EBITDAGrowth' EBITDAGrowth,
up.jsondoc_ ->'Data' ->> 'EBITDAMargin' EBITDAMargin,
up.jsondoc_ ->'Data' ->> 'LongTermDebt' LongTermDebt,
up.jsondoc_ ->'Data' ->> 'NumEmployees' NumEmployees,
up.jsondoc_ ->'Data' ->> 'RDCapitalExp' RDCapitalExp,
up.jsondoc_ ->'Data' ->> 'SharePremium' SharePremium,
up.jsondoc_ ->'Data' ->> 'CashDividends' CashDividends,
up.jsondoc_ ->'Data' ->> 'InventoryDays' InventoryDays,
up.jsondoc_ ->'Data' ->> 'MaxSubordDebt' MaxSubordDebt,
up.jsondoc_ ->'Data' ->> 'MinQuickRatio' MinQuickRatio,
up.jsondoc_ ->'Data' ->> 'NetCashIncome' NetCashIncome,
up.jsondoc_ ->'Data' ->> 'OffBSLeverage' OffBSLeverage,
up.jsondoc_ ->'Data' ->> 'OtherOpIncome' OtherOpIncome,
up.jsondoc_ ->'Data' ->> 'OtherReserves' OtherReserves,
up.jsondoc_ ->'Data' ->> 'PrepaymentsCP' PrepaymentsCP,
up.jsondoc_ ->'Data' ->> 'RestructCosts' RestructCosts,
up.jsondoc_ ->'Data' ->> 'SalesRevenues' SalesRevenues,
up.jsondoc_ ->'Data' ->> 'TradePayables' TradePayables,
up.jsondoc_ ->'Data' ->> 'BadDebtExpense' BadDebtExpense,
up.jsondoc_ ->'Data' ->> 'BorFunToEquity' BorFunToEquity,
up.jsondoc_ ->'Data' ->> 'COGSToTradePay' COGSToTradePay,
up.jsondoc_ ->'Data' ->> 'DebtToNetWorth' DebtToNetWorth,
up.jsondoc_ ->'Data' ->> 'DeferredIntExp' DeferredIntExp,
up.jsondoc_ ->'Data' ->> 'DueToSholderCP' DueToSholderCP,
up.jsondoc_ ->'Data' ->> 'ExtraordIncExp' ExtraordIncExp,
up.jsondoc_ ->'Data' ->> 'GainInvestProp' GainInvestProp,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmFinAct' ICFCFFrmFinAct,
up.jsondoc_ ->'Data' ->> 'ICFDivsPaidFin' ICFDivsPaidFin,
up.jsondoc_ ->'Data' ->> 'ICFDivsRecOper' ICFDivsRecOper,
up.jsondoc_ ->'Data' ->> 'ICFPurchasePPE' ICFPurchasePPE,
up.jsondoc_ ->'Data' ->> 'InterestIncome' InterestIncome,
up.jsondoc_ ->'Data' ->> 'LTDConvertible' LTDConvertible,
up.jsondoc_ ->'Data' ->> 'MaxSalesGrowth' MaxSalesGrowth,
up.jsondoc_ ->'Data' ->> 'MinCashBalance' MinCashBalance,
up.jsondoc_ ->'Data' ->> 'MinCashFlowCov' MinCashFlowCov,
up.jsondoc_ ->'Data' ->> 'MinEarningsCov' MinEarningsCov,
up.jsondoc_ ->'Data' ->> 'MinInterestCov' MinInterestCov,
up.jsondoc_ ->'Data' ->> 'NetFixedAssets' NetFixedAssets,
up.jsondoc_ ->'Data' ->> 'NetIntangibles' NetIntangibles,
up.jsondoc_ ->'Data' ->> 'NetTradeReceiv' NetTradeReceiv,
up.jsondoc_ ->'Data' ->> 'NotesPayableCP' NotesPayableCP,
up.jsondoc_ ->'Data' ->> 'OpLeaseRentExp' OpLeaseRentExp,
up.jsondoc_ ->'Data' ->> 'OthNonOpIncExp' OthNonOpIncExp,
up.jsondoc_ ->'Data' ->> 'OthOperExpProv' OthOperExpProv,
up.jsondoc_ ->'Data' ->> 'PrepaymentsLTP' PrepaymentsLTP,
up.jsondoc_ ->'Data' ->> 'RestrictedCash' RestrictedCash,
up.jsondoc_ ->'Data' ->> 'RestructProvCP' RestructProvCP,
up.jsondoc_ ->'Data' ->> 'ReturnOnAssets' ReturnOnAssets,
up.jsondoc_ ->'Data' ->> 'STLoansPayable' STLoansPayable,
up.jsondoc_ ->'Data' ->> 'TaxProvisionCP' TaxProvisionCP,
up.jsondoc_ ->'Data' ->> 'TotalIncomeTax' TotalIncomeTax,
up.jsondoc_ ->'Data' ->> 'TreasuryShares' TreasuryShares,
up.jsondoc_ ->'Data' ->> 'WorkingCapital' WorkingCapital,
up.jsondoc_ ->'Data' ->> 'AllOthCurrLiabs' AllOthCurrLiabs,
up.jsondoc_ ->'Data' ->> 'AllOtherNCLiabs' AllOtherNCLiabs,
up.jsondoc_ ->'Data' ->> 'COGSToInventory' COGSToInventory,
up.jsondoc_ ->'Data' ->> 'CashEquivalents' CashEquivalents,
up.jsondoc_ ->'Data' ->> 'CashFrTradActiv' CashFrTradActiv,
up.jsondoc_ ->'Data' ->> 'CashGrossMargin' CashGrossMargin,
up.jsondoc_ ->'Data' ->> 'CashPdDivAndInt' CashPdDivAndInt,
up.jsondoc_ ->'Data' ->> 'CashPdToSupplrs' CashPdToSupplrs,
up.jsondoc_ ->'Data' ->> 'DCFCFFrmOperAct' DCFCFFrmOperAct,
up.jsondoc_ ->'Data' ->> 'DCFCFFromFinAct' DCFCFFromFinAct,
up.jsondoc_ ->'Data' ->> 'DCFChgNetIntang' DCFChgNetIntang,
up.jsondoc_ ->'Data' ->> 'DefIncomeTaxPay' DefIncomeTaxPay,
up.jsondoc_ ->'Data' ->> 'DeferredTaxAsts' DeferredTaxAsts,
up.jsondoc_ ->'Data' ->> 'DepAmortToSales' DepAmortToSales,
up.jsondoc_ ->'Data' ->> 'DeprecImpairCOS' DeprecImpairCOS,
up.jsondoc_ ->'Data' ->> 'DerivHedgAstsCP' DerivHedgAstsCP,
up.jsondoc_ ->'Data' ->> 'DerivHedgLiabCP' DerivHedgLiabCP,
up.jsondoc_ ->'Data' ->> 'DueFrmJVPartner' DueFrmJVPartner,
up.jsondoc_ ->'Data' ->> 'DueFrmSholderCP' DueFrmSholderCP,
up.jsondoc_ ->'Data' ->> 'DueToRelPartyCP' DueToRelPartyCP,
up.jsondoc_ ->'Data' ->> 'DueToSholderLTP' DueToSholderLTP,
up.jsondoc_ ->'Data' ->> 'EffTangNetWorth' EffTangNetWorth,
up.jsondoc_ ->'Data' ->> 'FinanceLeasesCP' FinanceLeasesCP,
up.jsondoc_ ->'Data' ->> 'GainDispDiscOps' GainDispDiscOps,
up.jsondoc_ ->'Data' ->> 'GeneralAdminExp' GeneralAdminExp,
up.jsondoc_ ->'Data' ->> 'HedgingReserves' HedgingReserves,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmOperAct' ICFCFFrmOperAct,
up.jsondoc_ ->'Data' ->> 'ICFDepreciation' ICFDepreciation,
up.jsondoc_ ->'Data' ->> 'ICFDivsPaidOper' ICFDivsPaidOper,
up.jsondoc_ ->'Data' ->> 'ICFIncTaxesPaid' ICFIncTaxesPaid,
up.jsondoc_ ->'Data' ->> 'ICFIncomeTaxExp' ICFIncomeTaxExp,
up.jsondoc_ ->'Data' ->> 'ICFProcNCBorrow' ICFProcNCBorrow,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleSubs' ICFProcSaleSubs,
up.jsondoc_ ->'Data' ->> 'InterestExpense' InterestExpense,
up.jsondoc_ ->'Data' ->> 'InterestPayable' InterestPayable,
up.jsondoc_ ->'Data' ->> 'LTDSubordinated' LTDSubordinated,
up.jsondoc_ ->'Data' ->> 'MaxTradePayDays' MaxTradePayDays,
up.jsondoc_ ->'Data' ->> 'MinCurrentRatio' MinCurrentRatio,
up.jsondoc_ ->'Data' ->> 'MinTangNetWorth' MinTangNetWorth,
up.jsondoc_ ->'Data' ->> 'NetCashAfterOps' NetCashAfterOps,
up.jsondoc_ ->'Data' ->> 'NetOpProfGrowth' NetOpProfGrowth,
up.jsondoc_ ->'Data' ->> 'NetOpProfMargin' NetOpProfMargin,
up.jsondoc_ ->'Data' ->> 'NetProfitGrowth' NetProfitGrowth,
up.jsondoc_ ->'Data' ->> 'NetProfitMargin' NetProfitMargin,
up.jsondoc_ ->'Data' ->> 'NetTradeRecDays' NetTradeRecDays,
up.jsondoc_ ->'Data' ->> 'OffrCompToSales' OffrCompToSales,
up.jsondoc_ ->'Data' ->> 'OthEquityResAdj' OthEquityResAdj,
up.jsondoc_ ->'Data' ->> 'OtherOpExpenses' OtherOpExpenses,
up.jsondoc_ ->'Data' ->> 'PermanentEquity' PermanentEquity,
up.jsondoc_ ->'Data' ->> 'PriorPeriodAdjs' PriorPeriodAdjs,
up.jsondoc_ ->'Data' ->> 'PriorYearTaxAdj' PriorYearTaxAdj,
up.jsondoc_ ->'Data' ->> 'ProfitBeforeTax' ProfitBeforeTax,
up.jsondoc_ ->'Data' ->> 'ResearchDevelop' ResearchDevelop,
up.jsondoc_ ->'Data' ->> 'RestructProvLTP' RestructProvLTP,
up.jsondoc_ ->'Data' ->> 'RetainedProfits' RetainedProfits,
up.jsondoc_ ->'Data' ->> 'SalesToTradeRec' SalesToTradeRec,
up.jsondoc_ ->'Data' ->> 'ShareOptionCost' ShareOptionCost,
up.jsondoc_ ->'Data' ->> 'TaxProvisionLTP' TaxProvisionLTP,
up.jsondoc_ ->'Data' ->> 'TotalInventory' totalinventory,
up.jsondoc_ ->'Data' ->> 'TotNonCurrLiabs' TotNonCurrLiabs,
up.jsondoc_ ->'Data' ->> 'TotalCurrAssets' TotalCurrAssets,
up.jsondoc_ ->'Data' ->> 'TotalIntExpense' TotalIntExpense,
up.jsondoc_ ->'Data' ->> 'TotalOpExpenses' TotalOpExpenses,
up.jsondoc_ ->'Data' ->> 'AdjstsScrtznProg' AdjstsScrtznProg,
up.jsondoc_ ->'Data' ->> 'AllOthCurrAssets' AllOthCurrAssets,
up.jsondoc_ ->'Data' ->> 'AllOtherExpenses' AllOtherExpenses,
up.jsondoc_ ->'Data' ->> 'AstsPlgedScrtztn' AstsPlgedScrtztn,
up.jsondoc_ ->'Data' ->> 'AstsRelToDiscOps' AstsRelToDiscOps,
up.jsondoc_ ->'Data' ->> 'BorrFundtoEBITDA' BorrFundtoEBITDA,
up.jsondoc_ ->'Data' ->> 'CPLTDConvertible' CPLTDConvertible,
up.jsondoc_ ->'Data' ->> 'CapitalizedCosts' CapitalizedCosts,
up.jsondoc_ ->'Data' ->> 'CashAftDebtAmort' CashAftDebtAmort,
up.jsondoc_ ->'Data' ->> 'CashClctdFrSales' CashClctdFrSales,
up.jsondoc_ ->'Data' ->> 'CashDivComShares' CashDivComShares,
up.jsondoc_ ->'Data' ->> 'CashFlowCoverage' CashFlowCoverage,
up.jsondoc_ ->'Data' ->> 'CashPdForOpCosts' CashPdForOpCosts,
up.jsondoc_ ->'Data' ->> 'ConstrInProgress' ConstrInProgress,
up.jsondoc_ ->'Data' ->> 'CurrentTaxReceiv' CurrentTaxReceiv,
up.jsondoc_ ->'Data' ->> 'DbtLessSubEffTNW' DbtLessSubEffTNW,
up.jsondoc_ ->'Data' ->> 'DeferredHedgGain' DeferredHedgGain,
up.jsondoc_ ->'Data' ->> 'DeferredIncomeCP' DeferredIncomeCP,
up.jsondoc_ ->'Data' ->> 'DerivHedgAstsLTP' DerivHedgAstsLTP,
up.jsondoc_ ->'Data' ->> 'DerivHedgLiabLTP' DerivHedgLiabLTP,
up.jsondoc_ ->'Data' ->> 'DividendsPayable' DividendsPayable,
up.jsondoc_ ->'Data' ->> 'DueFrmRelPartyCP' DueFrmRelPartyCP,
up.jsondoc_ ->'Data' ->> 'DueFrmSholderLTP' DueFrmSholderLTP,
up.jsondoc_ ->'Data' ->> 'DueToRelPartyLTP' DueToRelPartyLTP,
up.jsondoc_ ->'Data' ->> 'EarningsCoverage' EarningsCoverage,
up.jsondoc_ ->'Data' ->> 'EffectiveTaxRate' EffectiveTaxRate,
up.jsondoc_ ->'Data' ->> 'ExtraordIncExpNC' ExtraordIncExpNC,
up.jsondoc_ ->'Data' ->> 'FMVAdjFnclAssets' FMVAdjFnclAssets,
up.jsondoc_ ->'Data' ->> 'FinanceLeasesLTP' FinanceLeasesLTP,
up.jsondoc_ ->'Data' ->> 'GainLossFnclAsts' GainLossFnclAsts,
up.jsondoc_ ->'Data' ->> 'GoodsAndServices' GoodsAndServices,
up.jsondoc_ ->'Data' ->> 'GrossFixedAssets' GrossFixedAssets,
up.jsondoc_ ->'Data' ->> 'ICFBorrowCostsPd' ICFBorrowCostsPd,
up.jsondoc_ ->'Data' ->> 'ICFChgInDefTaxes' ICFChgInDefTaxes,
up.jsondoc_ ->'Data' ->> 'ICFChgInOtherRec' ICFChgInOtherRec,
up.jsondoc_ ->'Data' ->> 'ICFChgInTaxesPay' ICFChgInTaxesPay,
up.jsondoc_ ->'Data' ->> 'ICFChgInTradePay' ICFChgInTradePay,
up.jsondoc_ ->'Data' ->> 'ICFChgInTradeRec' ICFChgInTradeRec,
up.jsondoc_ ->'Data' ->> 'ICFChgOthWrkgCap' ICFChgOthWrkgCap,
up.jsondoc_ ->'Data' ->> 'ICFImpactChgExch' ICFImpactChgExch,
up.jsondoc_ ->'Data' ->> 'ICFIncomeTaxPaid' ICFIncomeTaxPaid,
up.jsondoc_ ->'Data' ->> 'ICFIntIncRecdInv' ICFIntIncRecdInv,
up.jsondoc_ ->'Data' ->> 'ICFNetProcIssCap' ICFNetProcIssCap,
up.jsondoc_ ->'Data' ->> 'ICFNetProfitLoss' ICFNetProfitLoss,
up.jsondoc_ ->'Data' ->> 'ICFOthNonCashAdj' ICFOthNonCashAdj,
up.jsondoc_ ->'Data' ->> 'ICFOtherInvestCF' ICFOtherInvestCF,
up.jsondoc_ ->'Data' ->> 'ICFRepayNCBorrow' ICFRepayNCBorrow,
up.jsondoc_ ->'Data' ->> 'ICFSalesAssocAff' ICFSalesAssocAff,
up.jsondoc_ ->'Data' ->> 'InterestCoverage' InterestCoverage,
up.jsondoc_ ->'Data' ->> 'MaterialExpenses' MaterialExpenses,
up.jsondoc_ ->'Data' ->> 'MaxCapitalExpend' MaxCapitalExpend,
up.jsondoc_ ->'Data' ->> 'MaxCashDividends' MaxCashDividends,
up.jsondoc_ ->'Data' ->> 'MaxOffBSLeverage' MaxOffBSLeverage,
up.jsondoc_ ->'Data' ->> 'NumOutPrefShares' NumOutPrefShares,
up.jsondoc_ ->'Data' ->> 'OffBSLiabilities' OffBSLiabilities,
up.jsondoc_ ->'Data' ->> 'OthIncAndTaxesPd' OthIncAndTaxesPd,
up.jsondoc_ ->'Data' ->> 'OthNonOpIncExpNC' OthNonOpIncExpNC,
up.jsondoc_ ->'Data' ->> 'OtherFixedAssets' OtherFixedAssets,
up.jsondoc_ ->'Data' ->> 'PBTToTotalAssets' PBTToTotalAssets,
up.jsondoc_ ->'Data' ->> 'PrefShareCapital' PrefShareCapital,
up.jsondoc_ ->'Data' ->> 'ProfitB4ExtItems' ProfitB4ExtItems,
up.jsondoc_ ->'Data' ->> 'RetireBenefitsCP' RetireBenefitsCP,
up.jsondoc_ ->'Data' ->> 'ReturnOnTotEqRes' ReturnOnTotEqRes,
up.jsondoc_ ->'Data' ->> 'STDebtToCurLiabs' STDebtToCurLiabs,
up.jsondoc_ ->'Data' ->> 'SalesToTotalAsts' SalesToTotalAsts,
up.jsondoc_ ->'Data' ->> 'SellMarketingExp' SellMarketingExp,
up.jsondoc_ ->'Data' ->> 'SocSecOthTaxesCP' SocSecOthTaxesCP,
up.jsondoc_ ->'Data' ->> 'StockDividendsNC' StockDividendsNC,
up.jsondoc_ ->'Data' ->> 'SubordDebtEquity' SubordDebtEquity,
up.jsondoc_ ->'Data' ->> 'SubordinatedDebt' SubordinatedDebt,
up.jsondoc_ ->'Data' ->> 'TangibleNetWorth' TangibleNetWorth,
up.jsondoc_ ->'Data' ->> 'TotalAssetGrowth' TotalAssetGrowth,
up.jsondoc_ ->'Data' ->> 'TotalCostofSales' TotalCostofSales,
up.jsondoc_ ->'Data' ->> 'TotalLiabilities' TotalLiabilities,
up.jsondoc_ ->'Data' ->> 'TradePayableDays' TradePayableDays,
up.jsondoc_ ->'Data' ->> 'TradePayablesLTP' TradePayablesLTP,
up.jsondoc_ ->'Data' ->> 'TradeReceivGross' TradeReceivGross,
up.jsondoc_ ->'Data' ->> 'AccumDeprecImpair' AccumDeprecImpair,
up.jsondoc_ ->'Data' ->> 'AmortImpairIntgbl' AmortImpairIntgbl,
up.jsondoc_ ->'Data' ->> 'BorrFundtoTotLiab' BorrFundtoTotLiab,
up.jsondoc_ ->'Data' ->> 'CPLTDSubordinated' CPLTDSubordinated,
up.jsondoc_ ->'Data' ->> 'CashDivPrefShares' CashDivPrefShares,
up.jsondoc_ ->'Data' ->> 'CgsInventoriesWIP' CgsInventoriesWIP,
up.jsondoc_ ->'Data' ->> 'DCFTotAdjustments' DCFTotAdjustments,
up.jsondoc_ ->'Data' ->> 'DecommEnvirCostCP' DecommEnvirCostCP,
up.jsondoc_ ->'Data' ->> 'DeferredIncomeLTP' DeferredIncomeLTP,
up.jsondoc_ ->'Data' ->> 'DeprecImpairOpExp' DeprecImpairOpExp,
up.jsondoc_ ->'Data' ->> 'DerivativesFMVAdj' DerivativesFMVAdj,
up.jsondoc_ ->'Data' ->> 'DueFrmRelPartyLTP' DueFrmRelPartyLTP,
up.jsondoc_ ->'Data' ->> 'EquityAndReserves' EquityAndReserves,
up.jsondoc_ ->'Data' ->> 'ExchGainSaleOfBus' ExchGainSaleOfBus,
up.jsondoc_ ->'Data' ->> 'ExchRatePeriodAvg' ExchRatePeriodAvg,
up.jsondoc_ ->'Data' ->> 'ExchRatePeriodEnd' ExchRatePeriodEnd,
up.jsondoc_ ->'Data' ->> 'ForexTranslEquity' ForexTranslEquity,
up.jsondoc_ ->'Data' ->> 'FurnitureFixtures' FurnitureFixtures,
up.jsondoc_ ->'Data' ->> 'GainDispFixedAsts' GainDispFixedAsts,
up.jsondoc_ ->'Data' ->> 'GrossProfitMargin' GrossProfitMargin,
up.jsondoc_ ->'Data' ->> 'ICFAmortAstImpair' ICFAmortAstImpair,
up.jsondoc_ ->'Data' ->> 'ICFAssocProfShare' ICFAssocProfShare,
up.jsondoc_ ->'Data' ->> 'ICFCFDispFixAsset' ICFCFDispFixAsset,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmInvestAct' ICFCFFrmInvestAct,
up.jsondoc_ ->'Data' ->> 'ICFChgInInventory' ICFChgInInventory,
up.jsondoc_ ->'Data' ->> 'ICFChgOthCurrAsts' ICFChgOthCurrAsts,
up.jsondoc_ ->'Data' ->> 'ICFDivsRecdInvest' ICFDivsRecdInvest,
up.jsondoc_ ->'Data' ->> 'ICFIntIncRecdOper' ICFIntIncRecdOper,
up.jsondoc_ ->'Data' ->> 'ICFInvestAssocAff' ICFInvestAssocAff,
up.jsondoc_ ->'Data' ->> 'ICFNetProfitB4Tax' ICFNetProfitB4Tax,
up.jsondoc_ ->'Data' ->> 'ICFOthFinancingCF' ICFOthFinancingCF,
up.jsondoc_ ->'Data' ->> 'ICFPurchOtherAsts' ICFPurchOtherAsts,
up.jsondoc_ ->'Data' ->> 'ICFPurchaseSaleTS' ICFPurchaseSaleTS,
up.jsondoc_ ->'Data' ->> 'InvestInJVPartner' InvestInJVPartner,
up.jsondoc_ ->'Data' ->> 'InvestmentInAssoc' InvestmentInAssoc,
up.jsondoc_ ->'Data' ->> 'InvestmentPropNet' InvestmentPropNet,
up.jsondoc_ ->'Data' ->> 'MinPensionLiabAdj' MinPensionLiabAdj,
up.jsondoc_ ->'Data' ->> 'MinReturnOnAssets' MinReturnOnAssets,
up.jsondoc_ ->'Data' ->> 'MinReturnOnEquity' MinReturnOnEquity,
up.jsondoc_ ->'Data' ->> 'MinWorkingCapital' MinWorkingCapital,
up.jsondoc_ ->'Data' ->> 'MinorityIntEquity' MinorityIntEquity,
up.jsondoc_ ->'Data' ->> 'MinorityInterests' MinorityInterests,
up.jsondoc_ ->'Data' ->> 'NetInterestIncExp' NetInterestIncExp,
up.jsondoc_ ->'Data' ->> 'NetOthFinanIncExp' NetOthFinanIncExp,
up.jsondoc_ ->'Data' ->> 'OthDebtServiceExp' OthDebtServiceExp,
up.jsondoc_ ->'Data' ->> 'OthFnclAstIncDvds' OthFnclAstIncDvds,
up.jsondoc_ ->'Data' ->> 'OthNonCurrentAsts' OthNonCurrentAsts,
up.jsondoc_ ->'Data' ->> 'OthNonIncRelTaxes' OthNonIncRelTaxes,
up.jsondoc_ ->'Data' ->> 'OthReceivablesLTP' OthReceivablesLTP,
up.jsondoc_ ->'Data' ->> 'OtherProvisionsCP' OtherProvisionsCP,
up.jsondoc_ ->'Data' ->> 'PBTToTotEquityRes' PBTToTotEquityRes,
up.jsondoc_ ->'Data' ->> 'PersonnelBenefExp' PersonnelBenefExp,
up.jsondoc_ ->'Data' ->> 'PlantAndEquipment' PlantAndEquipment,
up.jsondoc_ ->'Data' ->> 'ProfitB4TaxMargin' ProfitB4TaxMargin,
up.jsondoc_ ->'Data' ->> 'RetireBenefitsLTP' RetireBenefitsLTP,
up.jsondoc_ ->'Data' ->> 'RlzdForexTranslGL' RlzdForexTranslGL,
up.jsondoc_ ->'Data' ->> 'STOthLoansPayable' STOthLoansPayable,
up.jsondoc_ ->'Data' ->> 'SalestoWorkingCap' SalestoWorkingCap,
up.jsondoc_ ->'Data' ->> 'SecurOthFinAstsCP' SecurOthFinAstsCP,
up.jsondoc_ ->'Data' ->> 'ShareOptionCostCP' ShareOptionCostCP,
up.jsondoc_ ->'Data' ->> 'SocSecOthTaxesLTP' SocSecOthTaxesLTP,
up.jsondoc_ ->'Data' ->> 'StartUpCapitalExp' StartUpCapitalExp,
up.jsondoc_ ->'Data' ->> 'SustainableGrowth' SustainableGrowth,
up.jsondoc_ ->'Data' ->> 'TotEquityResLiabs' TotEquityResLiabs,
up.jsondoc_ ->'Data' ->> 'TotEquityReserves' TotEquityReserves,
up.jsondoc_ ->'Data' ->> 'TotLiabtoTotAsset' TotLiabtoTotAsset,
up.jsondoc_ ->'Data' ->> 'TotOthEqResIncExp' TotOthEqResIncExp,
up.jsondoc_ ->'Data' ->> 'TotalCurrentLiabs' TotalCurrentLiabs,
up.jsondoc_ ->'Data' ->> 'UnrealGainFixAsts' UnrealGainFixAsts,
up.jsondoc_ ->'Data' ->> 'UnrealGainInvests' UnrealGainInvests,
up.jsondoc_ ->'Data' ->> 'AccIntgbleAstAmort' AccIntgbleAstAmort,
up.jsondoc_ ->'Data' ->> 'AccuOthEqtyRsrvInc' AccuOthEqtyRsrvInc,
up.jsondoc_ ->'Data' ->> 'AccumGoodwilImpair' AccumGoodwilImpair,
up.jsondoc_ ->'Data' ->> 'AllOthNonCurrAssts' AllOthNonCurrAssts,
up.jsondoc_ ->'Data' ->> 'AllowForDoubtAccts' AllowForDoubtAccts,
up.jsondoc_ ->'Data' ->> 'BillingsInExcCosts' BillingsInExcCosts,
up.jsondoc_ ->'Data' ->> 'BorrFundtoEffTgWth' BorrFundtoEffTgWth,
up.jsondoc_ ->'Data' ->> 'CapAndRestrReserve' CapAndRestrReserve,
up.jsondoc_ ->'Data' ->> 'CapitalExpenditure' CapitalExpenditure,
up.jsondoc_ ->'Data' ->> 'CashAndEquivalents' CashAndEquivalents,
up.jsondoc_ ->'Data' ->> 'CommonShareCapital' CommonShareCapital,
up.jsondoc_ ->'Data' ->> 'CurrentIncomeTaxes' CurrentIncomeTaxes,
up.jsondoc_ ->'Data' ->> 'CustomerAdvancesCP' CustomerAdvancesCP,
up.jsondoc_ ->'Data' ->> 'DCFBegOfPeriodCash' DCFBegOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'DCFCFFromInvestAct' DCFCFFromInvestAct,
up.jsondoc_ ->'Data' ->> 'DCFChgNetFixAssets' DCFChgNetFixAssets,
up.jsondoc_ ->'Data' ->> 'DCFEndOfPeriodCash' DCFEndOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'DCFTotMovementCash' DCFTotMovementCash,
up.jsondoc_ ->'Data' ->> 'DecommEnvirCostLTP' DecommEnvirCostLTP,
up.jsondoc_ ->'Data' ->> 'DeferredIncTaxesNC' DeferredIncTaxesNC,
up.jsondoc_ ->'Data' ->> 'DividendPayoutRate' DividendPayoutRate,
up.jsondoc_ ->'Data' ->> 'ExpensesOwnWorkCap' ExpensesOwnWorkCap,
up.jsondoc_ ->'Data' ->> 'GrantsAndSubsidies' GrantsAndSubsidies,
up.jsondoc_ ->'Data' ->> 'ICFAcqSubNetCshAcq' ICFAcqSubNetCshAcq,
up.jsondoc_ ->'Data' ->> 'ICFBegOfPeriodCash' ICFBegOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'ICFCFDispFnclAsset' ICFCFDispFnclAsset,
up.jsondoc_ ->'Data' ->> 'ICFCFExtItemInvest' ICFCFExtItemInvest,
up.jsondoc_ ->'Data' ->> 'ICFCFExtraItemsFin' ICFCFExtraItemsFin,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmExtrItemOp' ICFCFFrmExtrItemOp,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmHedgingFin' ICFCFFrmHedgingFin,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmHedingOper' ICFCFFrmHedingOper,
up.jsondoc_ ->'Data' ->> 'ICFCFHedgingInvest' ICFCFHedgingInvest,
up.jsondoc_ ->'Data' ->> 'ICFChgFnclInstrTrd' ICFChgFnclInstrTrd,
up.jsondoc_ ->'Data' ->> 'ICFChgInCurrBorrow' ICFChgInCurrBorrow,
up.jsondoc_ ->'Data' ->> 'ICFChgInOthCurLiab' ICFChgInOthCurLiab,
up.jsondoc_ ->'Data' ->> 'ICFChgInProvisions' ICFChgInProvisions,
up.jsondoc_ ->'Data' ->> 'ICFChgPostEmplBnft' ICFChgPostEmplBnft,
up.jsondoc_ ->'Data' ->> 'ICFChgPrepayDefAst' ICFChgPrepayDefAst,
up.jsondoc_ ->'Data' ->> 'ICFDvdsPdMinShlder' ICFDvdsPdMinShlder,
up.jsondoc_ ->'Data' ->> 'ICFEndOfPeriodCash' ICFEndOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'ICFInterestExpense' ICFInterestExpense,
up.jsondoc_ ->'Data' ->> 'ICFInterestPaidFin' ICFInterestPaidFin,
up.jsondoc_ ->'Data' ->> 'ICFIssueCostNCBorr' ICFIssueCostNCBorr,
up.jsondoc_ ->'Data' ->> 'ICFNetFrgnExchDiff' ICFNetFrgnExchDiff,
up.jsondoc_ ->'Data' ->> 'ICFOthAftTaxIncExp' ICFOthAftTaxIncExp,
up.jsondoc_ ->'Data' ->> 'ICFPayFinLeaseLiab' ICFPayFinLeaseLiab,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleInvProp' ICFProcSaleInvProp,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleOthAsts' ICFProcSaleOthAsts,
up.jsondoc_ ->'Data' ->> 'ICFProcSlIntblAsts' ICFProcSlIntblAsts,
up.jsondoc_ ->'Data' ->> 'ICFProceedsSalePPE' ICFProceedsSalePPE,
up.jsondoc_ ->'Data' ->> 'ICFPurchFnclAssets' ICFPurchFnclAssets,
up.jsondoc_ ->'Data' ->> 'ICFPurchIntgblAsts' ICFPurchIntgblAsts,
up.jsondoc_ ->'Data' ->> 'ICFPurchInvestProp' ICFPurchInvestProp,
up.jsondoc_ ->'Data' ->> 'ICFTotMovementCash' ICFTotMovementCash,
up.jsondoc_ ->'Data' ->> 'ICFTranslAdjRelCsh' ICFTranslAdjRelCsh,
up.jsondoc_ ->'Data' ->> 'ICFUnexplAdjToCash' ICFUnexplAdjToCash,
up.jsondoc_ ->'Data' ->> 'IncFrmJointVenture' IncFrmJointVenture,
up.jsondoc_ ->'Data' ->> 'IncLossFrmRelParty' IncLossFrmRelParty,
up.jsondoc_ ->'Data' ->> 'IncomeTaxesPayable' IncomeTaxesPayable,
up.jsondoc_ ->'Data' ->> 'InvDaysExclCOSDepr' InvDaysExclCOSDepr,
up.jsondoc_ ->'Data' ->> 'LeaseholdImprvmnts' LeaseholdImprvmnts,
up.jsondoc_ ->'Data' ->> 'LiabsToEnterprises' LiabsToEnterprises,
up.jsondoc_ ->'Data' ->> 'MaxDebtToTangWorth' MaxDebtToTangWorth,
up.jsondoc_ ->'Data' ->> 'MaxNetTradeRecDays' MaxNetTradeRecDays,
up.jsondoc_ ->'Data' ->> 'MinNetProfitMargin' MinNetProfitMargin,
up.jsondoc_ ->'Data' ->> 'NetFixedAssetToTNW' NetFixedAssetToTNW,
up.jsondoc_ ->'Data' ->> 'NetProfDeprtoCPLTD' NetProfDeprtoCPLTD,
up.jsondoc_ ->'Data' ->> 'NumOutCommonShares' NumOutCommonShares,
up.jsondoc_ ->'Data' ->> 'OpLeaseCommitments' OpLeaseCommitments,
up.jsondoc_ ->'Data' ->> 'OpLeaseReceivables' OpLeaseReceivables,
up.jsondoc_ ->'Data' ->> 'OperExpExclDepAmor' OperExpExclDepAmor,
up.jsondoc_ ->'Data' ->> 'OthAdjRetainedProf' OthAdjRetainedProf,
up.jsondoc_ ->'Data' ->> 'OthIntangibleAsset' OthIntangibleAsset,
up.jsondoc_ ->'Data' ->> 'OthNonCurrentLiabs' OthNonCurrentLiabs,
up.jsondoc_ ->'Data' ->> 'OtherCurrentAssets' OtherCurrentAssets,
up.jsondoc_ ->'Data' ->> 'OtherIncomeExpense' OtherIncomeExpense,
up.jsondoc_ ->'Data' ->> 'OtherProvisionsLTP' OtherProvisionsLTP,
up.jsondoc_ ->'Data' ->> 'OtherReceivablesCP' OtherReceivablesCP,
up.jsondoc_ ->'Data' ->> 'ProvRetirementCost' ProvRetirementCost,
up.jsondoc_ ->'Data' ->> 'RentsRoyaltyIncome' RentsRoyaltyIncome,
up.jsondoc_ ->'Data' ->> 'RevaluationReserve' RevaluationReserve,
up.jsondoc_ ->'Data' ->> 'RtmntPlanActuarial' RtmntPlanActuarial,
up.jsondoc_ ->'Data' ->> 'STBankLoansPayable' STBankLoansPayable,
up.jsondoc_ ->'Data' ->> 'SecurOthFinAstsLTP' SecurOthFinAstsLTP,
up.jsondoc_ ->'Data' ->> 'ShareOptionCostLTP' ShareOptionCostLTP,
up.jsondoc_ ->'Data' ->> 'TotAdjRetainProfit' TotAdjRetainProfit,
up.jsondoc_ ->'Data' ->> 'TotalNonCurrAssets' TotalNonCurrAssets,
up.jsondoc_ ->'Data' ->> 'TransferToReserves' TransferToReserves,
up.jsondoc_ ->'Data' ->> 'UnrealGainInvestPr' UnrealGainInvestPr,
up.jsondoc_ ->'Data' ->> 'VehAndTranspoEquip' VehAndTranspoEquip,
up.jsondoc_ ->'Data' ->> 'CustomerAdvancesLTP' CustomerAdvancesLTP,
up.jsondoc_ ->'Data' ->> 'ICFInterestPaidOper' ICFInterestPaidOper,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleFnclAsts' ICFProcSaleFnclAsts,
up.jsondoc_ ->'Data' ->> 'SalesToNetFixedAsts' SalesToNetFixedAsts,
up.jsondoc_ ->'Data' ->> 'ShortTermSecurities' ShortTermSecurities,
up.jsondoc_ ->'Data' ->> 'UCAChgInFixedAssets' UCAChgInFixedAssets,
up.jsondoc_ ->'Data' ->> 'CashinHandandAtBanks' CashinHandandAtBanks,
up.jsondoc_ ->'Data' ->> 'GoodwillAmortization' GoodwillAmortization,
up.jsondoc_ ->'Data' ->> 'ICFCashPdtoEmployees' ICFCashPdtoEmployees,
up.jsondoc_ ->'Data' ->> 'ICFCashPdtoSuppliers' ICFCashPdtoSuppliers,
up.jsondoc_ ->'Data' ->> 'ICFMinorityInterests' ICFMinorityInterests,
up.jsondoc_ ->'Data' ->> 'SpecItemswEquityChar' SpecItemswEquityChar,
up.jsondoc_ ->'Data' ->> 'ConcessLicenseTrdmrks' ConcessLicenseTrdmrks,
up.jsondoc_ ->'Data' ->> 'ICFCashRecfrmCustomers' ICFCashRecfrmCustomers,
up.jsondoc_ ->'Data' ->> 'SubscribedCapCallNotPd' SubscribedCapCallNotPd,
up.jsondoc_ ->'Data' ->> 'TradePayDaysExclCOSDepr' TradePayDaysExclCOSDepr,
up.jsondoc_ ->'Data' ->> 'ICFCashFlowFromOpActDirect' ICFCashFlowFromOpActDirect,
up.jsondoc_ ->'Data' ->> 'Sales' Sales,
up.jsondoc_ ->'Data' ->> 'Deposits' Deposits,
up.jsondoc_ ->'Data' ->> 'CashAndCashEquivalents' CashAndCashEquivalents,
up.jsondoc_ ->'Data' ->> 'Uni_AccountReceivablesDays' Uni_AccountReceivablesDays,
up.jsondoc_ ->'Data' ->> 'Uni_AccountsPayableDays' Uni_AccountsPayableDays,
up.jsondoc_ ->'Data' ->> 'Uni_BorrowedFunds' Uni_BorrowedFunds,
up.jsondoc_ ->'Data' ->> 'Uni_CashInHandAndAtBanks' Uni_CashInHandAndAtBanks,
up.jsondoc_ ->'Data' ->> 'Uni_CashLiquidity' Uni_CashLiquidity,
up.jsondoc_ ->'Data' ->> 'Uni_CashToCurrentLiabilities' Uni_CashToCurrentLiabilities,
up.jsondoc_ ->'Data' ->> 'Uni_ConstructionContracts' Uni_ConstructionContracts,
up.jsondoc_ ->'Data' ->> 'Uni_CostsΙnΕxcellΟfΒillings' Uni_CostsΙnΕxcellΟfΒillings,
up.jsondoc_ ->'Data' ->> 'Uni_CurrentRatio' Uni_CurrentRatio,
up.jsondoc_ ->'Data' ->> 'Uni_DebtCoverageRatio' Uni_DebtCoverageRatio,
up.jsondoc_ ->'Data' ->> 'Uni_DebtToEbitda' Uni_DebtToEbitda,
up.jsondoc_ ->'Data' ->> 'Uni_DebtToSales' Uni_DebtToSales,
up.jsondoc_ ->'Data' ->> 'Uni_DeprecAndImpairment' Uni_DeprecAndImpairment,
up.jsondoc_ ->'Data' ->> 'Uni_DeprecImpairment' Uni_DeprecImpairment,
up.jsondoc_ ->'Data' ->> 'Uni_DividendsPayable' Uni_DividendsPayable,
up.jsondoc_ ->'Data' ->> 'Uni_EBITDA' Uni_EBITDA,
up.jsondoc_ ->'Data' ->> 'Uni_EBTDA' Uni_EBTDA,
up.jsondoc_ ->'Data' ->> 'Uni_EbtdaToDebt' Uni_EbtdaToDebt,
up.jsondoc_ ->'Data' ->> 'Uni_EbtdaToSales' Uni_EbtdaToSales,
up.jsondoc_ ->'Data' ->> 'Uni_FinancialLeverage' Uni_FinancialLeverage,
up.jsondoc_ ->'Data' ->> 'Uni_FreeFlowsToSales' Uni_FreeFlowsToSales,
up.jsondoc_ ->'Data' ->> 'Uni_GrossProfit' Uni_GrossProfit,
up.jsondoc_ ->'Data' ->> 'Uni_GrossProfitMargin' Uni_GrossProfitMargin,
up.jsondoc_ ->'Data' ->> 'Uni_InterestCoverage' Uni_InterestCoverage,
up.jsondoc_ ->'Data' ->> 'Uni_InterestExpense' Uni_InterestExpense,
up.jsondoc_ ->'Data' ->> 'Uni_InterestIncome' Uni_InterestIncome,
up.jsondoc_ ->'Data' ->> 'Uni_Inventories' Uni_Inventories,
up.jsondoc_ ->'Data' ->> 'Uni_InventoryDays' Uni_InventoryDays,
up.jsondoc_ ->'Data' ->> 'Uni_InvestementsInPropertyCp' Uni_InvestementsInPropertyCp,
up.jsondoc_ ->'Data' ->> 'Uni_NetMarginToReserves' Uni_NetMarginToReserves,
up.jsondoc_ ->'Data' ->> 'Uni_NetMarginToTotalAssets' Uni_NetMarginToTotalAssets,
up.jsondoc_ ->'Data' ->> 'Uni_NetTradeReceivables' Uni_NetTradeReceivables,
up.jsondoc_ ->'Data' ->> 'Uni_OperatingFlows' Uni_OperatingFlows,
up.jsondoc_ ->'Data' ->> 'Uni_OtherCurrentAssets' Uni_OtherCurrentAssets,
up.jsondoc_ ->'Data' ->> 'Uni_OtherCurrentLiabilities' Uni_OtherCurrentLiabilities,
up.jsondoc_ ->'Data' ->> 'Uni_OtherDebtors' Uni_OtherDebtors,
up.jsondoc_ ->'Data' ->> 'Uni_PrepaymentsByCustomers' Uni_PrepaymentsByCustomers,
up.jsondoc_ ->'Data' ->> 'Uni_PrepaymentsCp' Uni_PrepaymentsCp,
up.jsondoc_ ->'Data' ->> 'Uni_Profit' Uni_Profit,
up.jsondoc_ ->'Data' ->> 'Uni_ProfitLossBeforeExtraordinaryItems' Uni_ProfitLossBeforeExtraordinaryItems,
up.jsondoc_ ->'Data' ->> 'Uni_ProfitLossBeforeTax' Uni_ProfitLossBeforeTax,
up.jsondoc_ ->'Data' ->> 'Uni_QuickRatio' Uni_QuickRatio,
up.jsondoc_ ->'Data' ->> 'Uni_Receivables' Uni_Receivables,
up.jsondoc_ ->'Data' ->> 'Uni_ReceivablesToSales' Uni_ReceivablesToSales,
up.jsondoc_ ->'Data' ->> 'Uni_Sales' Uni_Sales,
up.jsondoc_ ->'Data' ->> 'Uni_SalesGrowth' Uni_SalesGrowth,
up.jsondoc_ ->'Data' ->> 'Uni_SalesRevenue' Uni_SalesRevenue,
up.jsondoc_ ->'Data' ->> 'Uni_SalesToAssets' Uni_SalesToAssets,
up.jsondoc_ ->'Data' ->> 'Uni_Taxes' Uni_Taxes,
up.jsondoc_ ->'Data' ->> 'Uni_TotalCostOfSales' Uni_TotalCostOfSales,
up.jsondoc_ ->'Data' ->> 'Uni_TotalCurrentAssets' Uni_TotalCurrentAssets,
up.jsondoc_ ->'Data' ->> 'Uni_TotalCurrentLiabilities ' Uni_TotalCurrentLiabilities ,
up.jsondoc_ ->'Data' ->> 'Uni_TotalEquityAndReserves' Uni_TotalEquityAndReserves,
up.jsondoc_ ->'Data' ->> 'Uni_TotalFixedAssets' Uni_TotalFixedAssets,
up.jsondoc_ ->'Data' ->> 'Uni_TotalLiabilitiesToNetWorth' Uni_TotalLiabilitiesToNetWorth,
up.jsondoc_ ->'Data' ->> 'Uni_TotalNonCurrentAssets' Uni_TotalNonCurrentAssets,
up.jsondoc_ ->'Data' ->> 'Uni_TotalNonCurrentLiabilities' Uni_TotalNonCurrentLiabilities,
up.jsondoc_ ->'Data' ->> 'Uni_TradePayablesCP' Uni_TradePayablesCP,
up.jsondoc_ ->'Data' ->> 'Uni_WorkingCapital' Uni_WorkingCapital,
up.jsondoc_ ->'Data' ->> 'Uni_WorkingCapitalToSales' Uni_WorkingCapitalToSales,
(up.jsondoc_ ->'Data' ->> 'StatementDate')::varchar(10)::date StatementDate,
--up.jsondoc_ ->'Data' ->> 'HistStmtVersionId' HistStmtVersionId,
up.wfid_::varchar wfid_,
up.taskid_::varchar taskid_,
up.versionid_::integer versionid_, 
up.statusid_::integer statusid_,
up.isdeleted_::boolean isdeleted_,
up.islatestversion_::boolean islatestversion_,
up.isvisible_::boolean isvisible_,
up.isvalid_::boolean isvalid_,
up.snapshotid_::integer snapshotid_,
up.contextuserid_::varchar contextuserid_ ,
up.createdby_::varchar createdby_ , 
up.createddate_::timestamp createddate_ , 
up.updatedby_::varchar updatedby_ , 
up.updateddate_::timestamp updateddate_ , 
t_ t_ ,
(case when up.updateddate_>up.createddate_ then up.updatedby_ else up.createdby_ end) as sourcepopulatedby_,
GREATEST(up.updateddate_,up.createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.uphiststmtfinancials up
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPHISTSTMTFINANCIALS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abuphiststmtfinancials - part b end', clock_timestamp();

--abuphiststmtfinancials
raise notice '% - Step abuphiststmtfinancials_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abuphiststmtfinancials_idx;
DROP INDEX if exists olapts.abuphiststmtfinancials_idx2;
DROP INDEX if exists olapts.abuphiststmtfinancials_idx3;
DROP INDEX if exists olapts.abuphiststmtfinancials_idx4;
DROP INDEX if exists olapts.abuphiststmtfinancials_idx5;

CREATE INDEX IF NOT EXISTS abuphiststmtfinancials_idx_pkid_gin ON olapts.abuphiststmtfinancials USING GIN (factuphiststmtfinancialid_,pkid_,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS abuphiststmtfinancials_idx_date_brin ON olapts.abuphiststmtfinancials USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abuphiststmtfinancials_idx_pkid_btree_ops ON olapts.abuphiststmtfinancials ((factuphiststmtfinancialid_) varchar_pattern_ops,(pkid_) text_pattern_ops,entityid,statementid,sourcepopulateddate_) include (versionid_,statusid_,isdeleted_,isvalid_,islatestversion_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);	

CREATE STATISTICS if not exists abuphiststmtfinancials_stat ON factuphiststmtfinancialid_,pkid_,versionid_,sourcepopulateddate_,wfid_ FROM olapts.abuphiststmtfinancials;

raise notice '% - Step abuphiststmtfinancials_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abuphiststmtfinancials - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abuphiststmtfinancialsflag;
CREATE TABLE olapts.abuphiststmtfinancialsflag AS
select 
id_,
(jsondoc_ ->> 'EntityId')::varchar||':'||(jsondoc_ ->> 'FinancialId')::varchar||'|'||(jsondoc_ ->> 'StatementId')::varchar as pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.uphiststmtfinancials
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPHISTSTMTFINANCIALS';
delete from olapts.refreshhistory where tablename = 'ABUPHISTSTMTFINANCIALS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPHISTSTMTFINANCIALS' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPHISTSTMTFINANCIALSFLAG';
delete from olapts.refreshhistory where tablename = 'ABUPHISTSTMTFINANCIALSFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPHISTSTMTFINANCIALSFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abuphiststmtfinancials - part c end', clock_timestamp();

raise notice '% - Step abuphiststmtfinancialsflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abuphiststmtfinancialsflag_idx;
DROP INDEX if exists olapts.abuphiststmtfinancialsflag_idx2;
DROP INDEX if exists olapts.abuphiststmtfinancialsflag_idx3;
DROP INDEX if exists olapts.abuphiststmtfinancialsflag_idx4;
DROP INDEX if exists olapts.abuphiststmtfinancialsflag_idx5;

CREATE INDEX IF NOT EXISTS abuphiststmtfinancialsflag_idx_pkid_gin ON olapts.abuphiststmtfinancialsflag USING GIN (id_,pkid_,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS abuphiststmtfinancialsflag_idx_date_brin ON olapts.abuphiststmtfinancialsflag USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abuphiststmtfinancialsflag_idx_pkid_btree_ops ON olapts.abuphiststmtfinancialsflag ((id_) varchar_pattern_ops,(pkid_) text_pattern_ops,sourcepopulateddate_) include (versionid_,isdeleted_,isvalid_,islatestversion_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);

raise notice '% - Step abuphiststmtfinancialsflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABUPPROJECTION') THEN
raise notice '% - Step abupprojection - part a start', clock_timestamp();
insert into olapts.abupprojection
select 
id_ factupprojectionid_,
pkid_::varchar as pkid_,
(jsondoc_->>'EntityId') entityid,
(jsondoc_->>'ProjectionId') projectionid,
(jsondoc_->>'FinancialId')::int financialid,
(jsondoc_->>'UserId') useridkey_,
(jsondoc_->'Data'->>'LongTermType') longtermtype,
(jsondoc_->'Data'->>'ScenarioType') scenariotype,
(jsondoc_->'Data'->>'CustomerName') customername,
(jsondoc_->'Data'->>'ProjectionName') projectionname,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
statusid_::int4,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.upprojection
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPPROJECTION')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abupprojection - part a end', clock_timestamp();
ELSE
raise notice '% - Step abupprojection - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abupprojection;
CREATE TABLE olapts.abupprojection AS
select 
id_ factupprojectionid_,
pkid_::varchar as pkid_,
(jsondoc_->>'EntityId') entityid,
(jsondoc_->>'ProjectionId') projectionid,
(jsondoc_->>'FinancialId')::int financialid,
(jsondoc_->>'UserId') useridkey_,
(jsondoc_->'Data'->>'LongTermType') longtermtype,
(jsondoc_->'Data'->>'ScenarioType') scenariotype,
(jsondoc_->'Data'->>'CustomerName') customername,
(jsondoc_->'Data'->>'ProjectionName') projectionname,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
statusid_::int4,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.upprojection
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPPROJECTION')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abupprojection - part b end', clock_timestamp();

--abupprojection
raise notice '% - Step abupprojection_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abupprojection_idx;
DROP INDEX if exists olapts.abupprojection_idx2;
CREATE INDEX IF NOT EXISTS abupprojection_idx ON olapts.abupprojection (factupprojectionid_,wfid_);
CREATE INDEX IF NOT EXISTS abupprojection_idx2 ON olapts.abupprojection (pkid_,versionid_,wfid_);	

raise notice '% - Step abupprojection_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abupprojection - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abupprojectionflag;
CREATE TABLE olapts.abupprojectionflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.upprojection
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPPROJECTION';
delete from olapts.refreshhistory where tablename = 'ABUPPROJECTION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPPROJECTION' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPPROJECTIONFLAG';
delete from olapts.refreshhistory where tablename = 'ABUPPROJECTIONFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPPROJECTIONFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abupprojection - part c end', clock_timestamp();

raise notice '% - Step abupprojectionflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abupprojectionflag_idx;
DROP INDEX if exists olapts.abupprojectionflag_idx2;

CREATE INDEX IF NOT EXISTS abupprojectionflag_idx ON olapts.abupprojectionflag (id_);
CREATE INDEX IF NOT EXISTS abupprojectionflag_idx2 ON olapts.abupprojectionflag (pkid_,versionid_);

raise notice '% - Step abupprojectionflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALS') THEN
raise notice '% - Step abupprojstmtfinancials - part a start', clock_timestamp();
insert into olapts.abupprojstmtfinancials
select
id_ factupprojstmtfinancialid_,
pkid_::varchar as pkid_,
(jsondoc_->>'EntityId') entityid,
(jsondoc_->>'ProjectionId') projectionid,
(jsondoc_->>'StatementId')::int statementid,
(jsondoc_->>'FinancialId')::int financialid,
(jsondoc_->>'FinancialTemplate') financialtemplatekey_,
cast(to_char((up.jsondoc_ -> 'Data' ->> 'StatementDate')::date,'YYYYMMDD') as integer) as statementdatekey_,
(jsondoc_->'Data'->>'UserId') useridkey_,
(jsondoc_->'Data'->>'Accountant') accountant,
(jsondoc_->'Data'->>'AccountingStandard') accountingstandard,
(jsondoc_->'Data'->>'OfficersComp')::numeric officerscomp,
(jsondoc_->'Data'->>'Analyst') analyst,
(jsondoc_->'Data'->>'AuditMethod') auditmethod,
(jsondoc_->'Data'->>'GrossFixedAssets')::numeric grossfixedassets,
(jsondoc_->'Data'->>'ReconcileID')::int reconcileid,
(jsondoc_->'Data'->>'StatementType') statementtype,
(jsondoc_->'Data'->>'TotalInventory')::numeric totalinventory,
(jsondoc_->'Data'->>'Periods')::numeric periods,
(jsondoc_->'Data'->>'StatementSource') statementsource,
(jsondoc_->'Data'->>'StatementMonths')::numeric statementmonths,
(left(up.jsondoc_ -> 'Data' ->>'StatementYear',4))::numeric as statementyear,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
statusid_::int4,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
(up.jsondoc_ -> 'Data' ->> 'CustomerName')::varchar as customername, 
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.upprojstmtfinancials up
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abupprojstmtfinancials - part a end', clock_timestamp();
ELSE
raise notice '% - Step abupprojstmtfinancials - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abupprojstmtfinancials;
CREATE TABLE olapts.abupprojstmtfinancials AS
select
id_ factupprojstmtfinancialid_,
pkid_::varchar as pkid_,
(jsondoc_->>'EntityId') entityid,
(jsondoc_->>'ProjectionId') projectionid,
(jsondoc_->>'StatementId')::int statementid,
(jsondoc_->>'FinancialId')::int financialid,
(jsondoc_->>'FinancialTemplate') financialtemplatekey_,
cast(to_char((up.jsondoc_ -> 'Data' ->> 'StatementDate')::date,'YYYYMMDD') as integer) as statementdatekey_,
(jsondoc_->'Data'->>'UserId') useridkey_,
(jsondoc_->'Data'->>'Accountant') accountant,
(jsondoc_->'Data'->>'AccountingStandard') accountingstandard,
(jsondoc_->'Data'->>'OfficersComp')::numeric officerscomp,
(jsondoc_->'Data'->>'Analyst') analyst,
(jsondoc_->'Data'->>'AuditMethod') auditmethod,
(jsondoc_->'Data'->>'GrossFixedAssets')::numeric grossfixedassets,
(jsondoc_->'Data'->>'ReconcileID')::int reconcileid,
(jsondoc_->'Data'->>'StatementType') statementtype,
(jsondoc_->'Data'->>'TotalInventory')::numeric totalinventory,
(jsondoc_->'Data'->>'Periods')::numeric periods,
(jsondoc_->'Data'->>'StatementSource') statementsource,
(jsondoc_->'Data'->>'StatementMonths')::numeric statementmonths,
(left(up.jsondoc_ -> 'Data' ->>'StatementYear',4))::numeric as statementyear,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
statusid_::int4,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
(up.jsondoc_ -> 'Data' ->> 'CustomerName')::varchar as customername, 
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.upprojstmtfinancials up
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALS')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abupprojstmtfinancials - part b end', clock_timestamp();

--abupprojstmtfinancials
raise notice '% - Step abupprojstmtfinancials_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abupprojstmtfinancials_idx;
DROP INDEX if exists olapts.abupprojstmtfinancials_idx2;

CREATE INDEX IF NOT EXISTS abupprojstmtfinancials_idx ON olapts.abupprojstmtfinancials (factupprojstmtfinancialid_,wfid_);
CREATE INDEX IF NOT EXISTS abupprojstmtfinancials_idx2 ON olapts.abupprojstmtfinancials (pkid_,versionid_,wfid_);	

raise notice '% - Step abupprojstmtfinancials_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abupprojstmtfinancials - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abupprojstmtfinancialsflag;
CREATE TABLE olapts.abupprojstmtfinancialsflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.upprojstmtfinancials
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALS';
delete from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPPROJSTMTFINANCIALS' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSFLAG';
delete from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPPROJSTMTFINANCIALSFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abupprojstmtfinancials - part c end', clock_timestamp();

raise notice '% - Step abupprojstmtfinancialsflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abupprojstmtfinancialsflag_idx;
DROP INDEX if exists olapts.abupprojstmtfinancialsflag_idx2;

CREATE INDEX IF NOT EXISTS abupprojstmtfinancialsflag_idx ON olapts.abupprojstmtfinancialsflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abupprojstmtfinancialsflag_idx2 ON olapts.abupprojstmtfinancialsflag (pkid_,versionid_,wfid_);

raise notice '% - Step abupprojstmtfinancialsflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSDETAIL') THEN
raise notice '% - Step abupprojstmtfinancialsdetail - part a start', clock_timestamp();
insert into olapts.abupprojstmtfinancialsdetail
select 
up.Id_::varchar projstmtfinancialid_,
up.pkid_,
up.jsondoc_->>'EntityId'::varchar entityid,
up.jsondoc_ -> 'Data' ->> 'CustomerName'::varchar customername,
up.jsondoc_ -> 'Data' ->> 'UserId'::varchar userid,
(up.jsondoc_->>'StatementId') statementid,
(up.jsondoc_->>'FinancialId') financialid,
up.jsondoc_->> 'FinancialTemplate'::varchar financialtemplatekey_,
cast(to_char((up.jsondoc_ -> 'Data' ->> 'StatementDate')::date,'YYYYMMDD') as integer) statementdatekey_,
(left(up.jsondoc_ -> 'Data' ->>'StatementYear',4))::numeric statementyear,
up.jsondoc_ ->> 'UserId'::varchar useridkey_,
up.jsondoc_ ->'Data' ->> 'Accountant'::varchar accountant,
up.jsondoc_ ->'Data' ->> 'AccountingStandard'::varchar accountingstandard, 
(up.jsondoc_ -> 'Data' ->>'OfficersComp')::numeric as officerscomp, 
up.jsondoc_ ->'Data' ->> 'Analyst'::varchar analyst, 
up.jsondoc_ ->'Data' ->> 'AuditMethod'::varchar auditmethod, 
((up.jsondoc_ -> 'Data' ->>'ReconcileID')::numeric)::integer as reconcileid,  
up.jsondoc_ ->'Data' ->> 'StatementType'::varchar statementtype,  
up.jsondoc_ ->'Data' ->> 'StatementSource'::varchar statementsource,
(up.jsondoc_ -> 'Data' ->>'StatementMonths')::numeric as statementmonths,
(up.jsondoc_ -> 'Data' ->>'Periods')::numeric as periods,
up.jsondoc_ ->'Data' ->> 'Cash' Cash,
up.jsondoc_ ->'Data' ->> 'EBIT' EBIT,
up.jsondoc_ ->'Data' ->> 'Land' Land,
up.jsondoc_ ->'Data' ->> 'CPLTD' CPLTD,
up.jsondoc_ ->'Data' ->> 'EBITDA' EBITDA,
up.jsondoc_ ->'Data' ->> 'LTDBank' LTDBank,
up.jsondoc_ ->'Data' ->> 'Patents' Patents,
up.jsondoc_ ->'Data' ->> 'CashToTA' CashToTA,
up.jsondoc_ ->'Data' ->> 'Goodwill' Goodwill,
up.jsondoc_ ->'Data' ->> 'LTDOther' LTDOther,
up.jsondoc_ ->'Data' ->> 'PBTToTNW' PBTToTNW,
up.jsondoc_ ->'Data' ->> 'Buildings' Buildings,
up.jsondoc_ ->'Data' ->> 'CPLTDBank' CPLTDBank,
up.jsondoc_ ->'Data' ->> 'DebtToTNW' DebtToTNW,
up.jsondoc_ ->'Data' ->> 'NetProfit' NetProfit,
up.jsondoc_ ->'Data' ->> 'AllOtherCL' AllOtherCL,
up.jsondoc_ ->'Data' ->> 'CPLTDOther' CPLTDOther,
up.jsondoc_ ->'Data' ->> 'MaxInvDays' MaxInvDays,
up.jsondoc_ ->'Data' ->> 'NWAToSales' NWAToSales,
up.jsondoc_ ->'Data' ->> 'OtherTaxes' OtherTaxes,
up.jsondoc_ ->'Data' ->> 'Overdrafts' Overdrafts,
up.jsondoc_ ->'Data' ->> 'QuickRatio' QuickRatio,
up.jsondoc_ ->'Data' ->> 'SalesToTNW' SalesToTNW,
up.jsondoc_ ->'Data' ->> 'CostOfSales' CostOfSales,
up.jsondoc_ ->'Data' ->> 'GrossProfit' GrossProfit,
up.jsondoc_ ->'Data' ->> 'Inventories' Inventories,
up.jsondoc_ ->'Data' ->> 'NetOpProfit' NetOpProfit,
up.jsondoc_ ->'Data' ->> 'OffBSAssets' OffBSAssets,
up.jsondoc_ ->'Data' ->> 'OtherEquity' OtherEquity,
up.jsondoc_ ->'Data' ->> 'ReturnOnTNW' ReturnOnTNW,
up.jsondoc_ ->'Data' ->> 'SalesGrowth' SalesGrowth,
up.jsondoc_ ->'Data' ->> 'TotalAssets' TotalAssets,
up.jsondoc_ ->'Data' ->> 'AuditOpinion' AuditOpinion,
up.jsondoc_ ->'Data' ->> 'CashAfterOps' CashAfterOps,
up.jsondoc_ ->'Data' ->> 'CurrentRatio' CurrentRatio,
up.jsondoc_ ->'Data' ->> 'DebtToEquity' DebtToEquity,
up.jsondoc_ ->'Data' ->> 'EBITDAGrowth' EBITDAGrowth,
up.jsondoc_ ->'Data' ->> 'EBITDAMargin' EBITDAMargin,
up.jsondoc_ ->'Data' ->> 'LongTermDebt' LongTermDebt,
up.jsondoc_ ->'Data' ->> 'NumEmployees' NumEmployees,
up.jsondoc_ ->'Data' ->> 'RDCapitalExp' RDCapitalExp,
up.jsondoc_ ->'Data' ->> 'SharePremium' SharePremium,
up.jsondoc_ ->'Data' ->> 'CashDividends' CashDividends,
up.jsondoc_ ->'Data' ->> 'InventoryDays' InventoryDays,
up.jsondoc_ ->'Data' ->> 'MaxSubordDebt' MaxSubordDebt,
up.jsondoc_ ->'Data' ->> 'MinQuickRatio' MinQuickRatio,
up.jsondoc_ ->'Data' ->> 'NetCashIncome' NetCashIncome,
up.jsondoc_ ->'Data' ->> 'OffBSLeverage' OffBSLeverage,
up.jsondoc_ ->'Data' ->> 'OtherOpIncome' OtherOpIncome,
up.jsondoc_ ->'Data' ->> 'OtherReserves' OtherReserves,
up.jsondoc_ ->'Data' ->> 'PrepaymentsCP' PrepaymentsCP,
up.jsondoc_ ->'Data' ->> 'RestructCosts' RestructCosts,
up.jsondoc_ ->'Data' ->> 'SalesRevenues' SalesRevenues,
up.jsondoc_ ->'Data' ->> 'TradePayables' TradePayables,
up.jsondoc_ ->'Data' ->> 'BadDebtExpense' BadDebtExpense,
up.jsondoc_ ->'Data' ->> 'BorFunToEquity' BorFunToEquity,
up.jsondoc_ ->'Data' ->> 'COGSToTradePay' COGSToTradePay,
up.jsondoc_ ->'Data' ->> 'DebtToNetWorth' DebtToNetWorth,
up.jsondoc_ ->'Data' ->> 'DeferredIntExp' DeferredIntExp,
up.jsondoc_ ->'Data' ->> 'DueToSholderCP' DueToSholderCP,
up.jsondoc_ ->'Data' ->> 'ExtraordIncExp' ExtraordIncExp,
up.jsondoc_ ->'Data' ->> 'GainInvestProp' GainInvestProp,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmFinAct' ICFCFFrmFinAct,
up.jsondoc_ ->'Data' ->> 'ICFDivsPaidFin' ICFDivsPaidFin,
up.jsondoc_ ->'Data' ->> 'ICFDivsRecOper' ICFDivsRecOper,
up.jsondoc_ ->'Data' ->> 'ICFPurchasePPE' ICFPurchasePPE,
up.jsondoc_ ->'Data' ->> 'InterestIncome' InterestIncome,
up.jsondoc_ ->'Data' ->> 'LTDConvertible' LTDConvertible,
up.jsondoc_ ->'Data' ->> 'MaxSalesGrowth' MaxSalesGrowth,
up.jsondoc_ ->'Data' ->> 'MinCashBalance' MinCashBalance,
up.jsondoc_ ->'Data' ->> 'MinCashFlowCov' MinCashFlowCov,
up.jsondoc_ ->'Data' ->> 'MinEarningsCov' MinEarningsCov,
up.jsondoc_ ->'Data' ->> 'MinInterestCov' MinInterestCov,
up.jsondoc_ ->'Data' ->> 'NetFixedAssets' NetFixedAssets,
up.jsondoc_ ->'Data' ->> 'NetIntangibles' NetIntangibles,
up.jsondoc_ ->'Data' ->> 'NetTradeReceiv' NetTradeReceiv,
up.jsondoc_ ->'Data' ->> 'NotesPayableCP' NotesPayableCP,
up.jsondoc_ ->'Data' ->> 'OpLeaseRentExp' OpLeaseRentExp,
up.jsondoc_ ->'Data' ->> 'OthNonOpIncExp' OthNonOpIncExp,
up.jsondoc_ ->'Data' ->> 'OthOperExpProv' OthOperExpProv,
up.jsondoc_ ->'Data' ->> 'PrepaymentsLTP' PrepaymentsLTP,
up.jsondoc_ ->'Data' ->> 'RestrictedCash' RestrictedCash,
up.jsondoc_ ->'Data' ->> 'RestructProvCP' RestructProvCP,
up.jsondoc_ ->'Data' ->> 'ReturnOnAssets' ReturnOnAssets,
up.jsondoc_ ->'Data' ->> 'STLoansPayable' STLoansPayable,
up.jsondoc_ ->'Data' ->> 'TaxProvisionCP' TaxProvisionCP,
up.jsondoc_ ->'Data' ->> 'TotalIncomeTax' TotalIncomeTax,
up.jsondoc_ ->'Data' ->> 'TreasuryShares' TreasuryShares,
up.jsondoc_ ->'Data' ->> 'WorkingCapital' WorkingCapital,
up.jsondoc_ ->'Data' ->> 'AllOthCurrLiabs' AllOthCurrLiabs,
up.jsondoc_ ->'Data' ->> 'AllOtherNCLiabs' AllOtherNCLiabs,
up.jsondoc_ ->'Data' ->> 'COGSToInventory' COGSToInventory,
up.jsondoc_ ->'Data' ->> 'CashEquivalents' CashEquivalents,
up.jsondoc_ ->'Data' ->> 'CashFrTradActiv' CashFrTradActiv,
up.jsondoc_ ->'Data' ->> 'CashGrossMargin' CashGrossMargin,
up.jsondoc_ ->'Data' ->> 'CashPdDivAndInt' CashPdDivAndInt,
up.jsondoc_ ->'Data' ->> 'CashPdToSupplrs' CashPdToSupplrs,
up.jsondoc_ ->'Data' ->> 'DCFCFFrmOperAct' DCFCFFrmOperAct,
up.jsondoc_ ->'Data' ->> 'DCFCFFromFinAct' DCFCFFromFinAct,
up.jsondoc_ ->'Data' ->> 'DCFChgNetIntang' DCFChgNetIntang,
up.jsondoc_ ->'Data' ->> 'DefIncomeTaxPay' DefIncomeTaxPay,
up.jsondoc_ ->'Data' ->> 'DeferredTaxAsts' DeferredTaxAsts,
up.jsondoc_ ->'Data' ->> 'DepAmortToSales' DepAmortToSales,
up.jsondoc_ ->'Data' ->> 'DeprecImpairCOS' DeprecImpairCOS,
up.jsondoc_ ->'Data' ->> 'DerivHedgAstsCP' DerivHedgAstsCP,
up.jsondoc_ ->'Data' ->> 'DerivHedgLiabCP' DerivHedgLiabCP,
up.jsondoc_ ->'Data' ->> 'DueFrmJVPartner' DueFrmJVPartner,
up.jsondoc_ ->'Data' ->> 'DueFrmSholderCP' DueFrmSholderCP,
up.jsondoc_ ->'Data' ->> 'DueToRelPartyCP' DueToRelPartyCP,
up.jsondoc_ ->'Data' ->> 'DueToSholderLTP' DueToSholderLTP,
up.jsondoc_ ->'Data' ->> 'EffTangNetWorth' EffTangNetWorth,
up.jsondoc_ ->'Data' ->> 'FinanceLeasesCP' FinanceLeasesCP,
up.jsondoc_ ->'Data' ->> 'GainDispDiscOps' GainDispDiscOps,
up.jsondoc_ ->'Data' ->> 'GeneralAdminExp' GeneralAdminExp,
up.jsondoc_ ->'Data' ->> 'HedgingReserves' HedgingReserves,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmOperAct' ICFCFFrmOperAct,
up.jsondoc_ ->'Data' ->> 'ICFDepreciation' ICFDepreciation,
up.jsondoc_ ->'Data' ->> 'ICFDivsPaidOper' ICFDivsPaidOper,
up.jsondoc_ ->'Data' ->> 'ICFIncTaxesPaid' ICFIncTaxesPaid,
up.jsondoc_ ->'Data' ->> 'ICFIncomeTaxExp' ICFIncomeTaxExp,
up.jsondoc_ ->'Data' ->> 'ICFProcNCBorrow' ICFProcNCBorrow,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleSubs' ICFProcSaleSubs,
up.jsondoc_ ->'Data' ->> 'InterestExpense' InterestExpense,
up.jsondoc_ ->'Data' ->> 'InterestPayable' InterestPayable,
up.jsondoc_ ->'Data' ->> 'LTDSubordinated' LTDSubordinated,
up.jsondoc_ ->'Data' ->> 'MaxTradePayDays' MaxTradePayDays,
up.jsondoc_ ->'Data' ->> 'MinCurrentRatio' MinCurrentRatio,
up.jsondoc_ ->'Data' ->> 'MinTangNetWorth' MinTangNetWorth,
up.jsondoc_ ->'Data' ->> 'NetCashAfterOps' NetCashAfterOps,
up.jsondoc_ ->'Data' ->> 'NetOpProfGrowth' NetOpProfGrowth,
up.jsondoc_ ->'Data' ->> 'NetOpProfMargin' NetOpProfMargin,
up.jsondoc_ ->'Data' ->> 'NetProfitGrowth' NetProfitGrowth,
up.jsondoc_ ->'Data' ->> 'NetProfitMargin' NetProfitMargin,
up.jsondoc_ ->'Data' ->> 'NetTradeRecDays' NetTradeRecDays,
up.jsondoc_ ->'Data' ->> 'OffrCompToSales' OffrCompToSales,
up.jsondoc_ ->'Data' ->> 'OthEquityResAdj' OthEquityResAdj,
up.jsondoc_ ->'Data' ->> 'OtherOpExpenses' OtherOpExpenses,
up.jsondoc_ ->'Data' ->> 'PermanentEquity' PermanentEquity,
up.jsondoc_ ->'Data' ->> 'PriorPeriodAdjs' PriorPeriodAdjs,
up.jsondoc_ ->'Data' ->> 'PriorYearTaxAdj' PriorYearTaxAdj,
up.jsondoc_ ->'Data' ->> 'ProfitBeforeTax' ProfitBeforeTax,
up.jsondoc_ ->'Data' ->> 'ResearchDevelop' ResearchDevelop,
up.jsondoc_ ->'Data' ->> 'RestructProvLTP' RestructProvLTP,
up.jsondoc_ ->'Data' ->> 'RetainedProfits' RetainedProfits,
up.jsondoc_ ->'Data' ->> 'SalesToTradeRec' SalesToTradeRec,
up.jsondoc_ ->'Data' ->> 'ShareOptionCost' ShareOptionCost,
up.jsondoc_ ->'Data' ->> 'TaxProvisionLTP' TaxProvisionLTP,
up.jsondoc_ ->'Data' ->> 'TotNonCurrLiabs' TotNonCurrLiabs,
up.jsondoc_ ->'Data' ->> 'TotalCurrAssets' TotalCurrAssets,
up.jsondoc_ ->'Data' ->> 'TotalIntExpense' TotalIntExpense,
up.jsondoc_ ->'Data' ->> 'TotalOpExpenses' TotalOpExpenses,
up.jsondoc_ ->'Data' ->> 'AdjstsScrtznProg' AdjstsScrtznProg,
up.jsondoc_ ->'Data' ->> 'AllOthCurrAssets' AllOthCurrAssets,
up.jsondoc_ ->'Data' ->> 'AllOtherExpenses' AllOtherExpenses,
up.jsondoc_ ->'Data' ->> 'AstsPlgedScrtztn' AstsPlgedScrtztn,
up.jsondoc_ ->'Data' ->> 'AstsRelToDiscOps' AstsRelToDiscOps,
up.jsondoc_ ->'Data' ->> 'BorrFundtoEBITDA' BorrFundtoEBITDA,
up.jsondoc_ ->'Data' ->> 'CPLTDConvertible' CPLTDConvertible,
up.jsondoc_ ->'Data' ->> 'CapitalizedCosts' CapitalizedCosts,
up.jsondoc_ ->'Data' ->> 'CashAftDebtAmort' CashAftDebtAmort,
up.jsondoc_ ->'Data' ->> 'CashClctdFrSales' CashClctdFrSales,
up.jsondoc_ ->'Data' ->> 'CashDivComShares' CashDivComShares,
up.jsondoc_ ->'Data' ->> 'CashFlowCoverage' CashFlowCoverage,
up.jsondoc_ ->'Data' ->> 'CashPdForOpCosts' CashPdForOpCosts,
up.jsondoc_ ->'Data' ->> 'ConstrInProgress' ConstrInProgress,
up.jsondoc_ ->'Data' ->> 'CurrentTaxReceiv' CurrentTaxReceiv,
up.jsondoc_ ->'Data' ->> 'DbtLessSubEffTNW' DbtLessSubEffTNW,
up.jsondoc_ ->'Data' ->> 'DeferredHedgGain' DeferredHedgGain,
up.jsondoc_ ->'Data' ->> 'DeferredIncomeCP' DeferredIncomeCP,
up.jsondoc_ ->'Data' ->> 'DerivHedgAstsLTP' DerivHedgAstsLTP,
up.jsondoc_ ->'Data' ->> 'DerivHedgLiabLTP' DerivHedgLiabLTP,
up.jsondoc_ ->'Data' ->> 'DividendsPayable' DividendsPayable,
up.jsondoc_ ->'Data' ->> 'DueFrmRelPartyCP' DueFrmRelPartyCP,
up.jsondoc_ ->'Data' ->> 'DueFrmSholderLTP' DueFrmSholderLTP,
up.jsondoc_ ->'Data' ->> 'DueToRelPartyLTP' DueToRelPartyLTP,
up.jsondoc_ ->'Data' ->> 'EarningsCoverage' EarningsCoverage,
up.jsondoc_ ->'Data' ->> 'EffectiveTaxRate' EffectiveTaxRate,
up.jsondoc_ ->'Data' ->> 'ExtraordIncExpNC' ExtraordIncExpNC,
up.jsondoc_ ->'Data' ->> 'FMVAdjFnclAssets' FMVAdjFnclAssets,
up.jsondoc_ ->'Data' ->> 'FinanceLeasesLTP' FinanceLeasesLTP,
up.jsondoc_ ->'Data' ->> 'GainLossFnclAsts' GainLossFnclAsts,
up.jsondoc_ ->'Data' ->> 'GoodsAndServices' GoodsAndServices,
up.jsondoc_ ->'Data' ->> 'GrossFixedAssets' GrossFixedAssets,
up.jsondoc_ ->'Data' ->> 'ICFBorrowCostsPd' ICFBorrowCostsPd,
up.jsondoc_ ->'Data' ->> 'ICFChgInDefTaxes' ICFChgInDefTaxes,
up.jsondoc_ ->'Data' ->> 'ICFChgInOtherRec' ICFChgInOtherRec,
up.jsondoc_ ->'Data' ->> 'ICFChgInTaxesPay' ICFChgInTaxesPay,
up.jsondoc_ ->'Data' ->> 'ICFChgInTradePay' ICFChgInTradePay,
up.jsondoc_ ->'Data' ->> 'ICFChgInTradeRec' ICFChgInTradeRec,
up.jsondoc_ ->'Data' ->> 'ICFChgOthWrkgCap' ICFChgOthWrkgCap,
up.jsondoc_ ->'Data' ->> 'ICFImpactChgExch' ICFImpactChgExch,
up.jsondoc_ ->'Data' ->> 'ICFIncomeTaxPaid' ICFIncomeTaxPaid,
up.jsondoc_ ->'Data' ->> 'ICFIntIncRecdInv' ICFIntIncRecdInv,
up.jsondoc_ ->'Data' ->> 'ICFNetProcIssCap' ICFNetProcIssCap,
up.jsondoc_ ->'Data' ->> 'ICFNetProfitLoss' ICFNetProfitLoss,
up.jsondoc_ ->'Data' ->> 'ICFOthNonCashAdj' ICFOthNonCashAdj,
up.jsondoc_ ->'Data' ->> 'ICFOtherInvestCF' ICFOtherInvestCF,
up.jsondoc_ ->'Data' ->> 'ICFRepayNCBorrow' ICFRepayNCBorrow,
up.jsondoc_ ->'Data' ->> 'ICFSalesAssocAff' ICFSalesAssocAff,
up.jsondoc_ ->'Data' ->> 'InterestCoverage' InterestCoverage,
up.jsondoc_ ->'Data' ->> 'MaterialExpenses' MaterialExpenses,
up.jsondoc_ ->'Data' ->> 'MaxCapitalExpend' MaxCapitalExpend,
up.jsondoc_ ->'Data' ->> 'MaxCashDividends' MaxCashDividends,
up.jsondoc_ ->'Data' ->> 'MaxOffBSLeverage' MaxOffBSLeverage,
up.jsondoc_ ->'Data' ->> 'NumOutPrefShares' NumOutPrefShares,
up.jsondoc_ ->'Data' ->> 'OffBSLiabilities' OffBSLiabilities,
up.jsondoc_ ->'Data' ->> 'OthIncAndTaxesPd' OthIncAndTaxesPd,
up.jsondoc_ ->'Data' ->> 'OthNonOpIncExpNC' OthNonOpIncExpNC,
up.jsondoc_ ->'Data' ->> 'OtherFixedAssets' OtherFixedAssets,
up.jsondoc_ ->'Data' ->> 'PBTToTotalAssets' PBTToTotalAssets,
up.jsondoc_ ->'Data' ->> 'PrefShareCapital' PrefShareCapital,
up.jsondoc_ ->'Data' ->> 'ProfitB4ExtItems' ProfitB4ExtItems,
up.jsondoc_ ->'Data' ->> 'RetireBenefitsCP' RetireBenefitsCP,
up.jsondoc_ ->'Data' ->> 'ReturnOnTotEqRes' ReturnOnTotEqRes,
up.jsondoc_ ->'Data' ->> 'STDebtToCurLiabs' STDebtToCurLiabs,
up.jsondoc_ ->'Data' ->> 'SalesToTotalAsts' SalesToTotalAsts,
up.jsondoc_ ->'Data' ->> 'SellMarketingExp' SellMarketingExp,
up.jsondoc_ ->'Data' ->> 'SocSecOthTaxesCP' SocSecOthTaxesCP,
up.jsondoc_ ->'Data' ->> 'StockDividendsNC' StockDividendsNC,
up.jsondoc_ ->'Data' ->> 'SubordDebtEquity' SubordDebtEquity,
up.jsondoc_ ->'Data' ->> 'SubordinatedDebt' SubordinatedDebt,
up.jsondoc_ ->'Data' ->> 'TangibleNetWorth' TangibleNetWorth,
up.jsondoc_ ->'Data' ->> 'TotalAssetGrowth' TotalAssetGrowth,
up.jsondoc_ ->'Data' ->> 'TotalCostofSales' TotalCostofSales,
up.jsondoc_ ->'Data' ->> 'TotalLiabilities' TotalLiabilities,
up.jsondoc_ ->'Data' ->> 'TradePayableDays' TradePayableDays,
up.jsondoc_ ->'Data' ->> 'TradePayablesLTP' TradePayablesLTP,
up.jsondoc_ ->'Data' ->> 'TradeReceivGross' TradeReceivGross,
up.jsondoc_ ->'Data' ->> 'AccumDeprecImpair' AccumDeprecImpair,
up.jsondoc_ ->'Data' ->> 'AmortImpairIntgbl' AmortImpairIntgbl,
up.jsondoc_ ->'Data' ->> 'BorrFundtoTotLiab' BorrFundtoTotLiab,
up.jsondoc_ ->'Data' ->> 'CPLTDSubordinated' CPLTDSubordinated,
up.jsondoc_ ->'Data' ->> 'CashDivPrefShares' CashDivPrefShares,
up.jsondoc_ ->'Data' ->> 'CgsInventoriesWIP' CgsInventoriesWIP,
up.jsondoc_ ->'Data' ->> 'DCFTotAdjustments' DCFTotAdjustments,
up.jsondoc_ ->'Data' ->> 'DecommEnvirCostCP' DecommEnvirCostCP,
up.jsondoc_ ->'Data' ->> 'DeferredIncomeLTP' DeferredIncomeLTP,
up.jsondoc_ ->'Data' ->> 'DeprecImpairOpExp' DeprecImpairOpExp,
up.jsondoc_ ->'Data' ->> 'DerivativesFMVAdj' DerivativesFMVAdj,
up.jsondoc_ ->'Data' ->> 'DueFrmRelPartyLTP' DueFrmRelPartyLTP,
up.jsondoc_ ->'Data' ->> 'EquityAndReserves' EquityAndReserves,
up.jsondoc_ ->'Data' ->> 'ExchGainSaleOfBus' ExchGainSaleOfBus,
up.jsondoc_ ->'Data' ->> 'ExchRatePeriodAvg' ExchRatePeriodAvg,
up.jsondoc_ ->'Data' ->> 'ExchRatePeriodEnd' ExchRatePeriodEnd,
up.jsondoc_ ->'Data' ->> 'ForexTranslEquity' ForexTranslEquity,
up.jsondoc_ ->'Data' ->> 'FurnitureFixtures' FurnitureFixtures,
up.jsondoc_ ->'Data' ->> 'GainDispFixedAsts' GainDispFixedAsts,
up.jsondoc_ ->'Data' ->> 'GrossProfitMargin' GrossProfitMargin,
up.jsondoc_ ->'Data' ->> 'ICFAmortAstImpair' ICFAmortAstImpair,
up.jsondoc_ ->'Data' ->> 'ICFAssocProfShare' ICFAssocProfShare,
up.jsondoc_ ->'Data' ->> 'ICFCFDispFixAsset' ICFCFDispFixAsset,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmInvestAct' ICFCFFrmInvestAct,
up.jsondoc_ ->'Data' ->> 'ICFChgInInventory' ICFChgInInventory,
up.jsondoc_ ->'Data' ->> 'ICFChgOthCurrAsts' ICFChgOthCurrAsts,
up.jsondoc_ ->'Data' ->> 'ICFDivsRecdInvest' ICFDivsRecdInvest,
up.jsondoc_ ->'Data' ->> 'ICFIntIncRecdOper' ICFIntIncRecdOper,
up.jsondoc_ ->'Data' ->> 'ICFInvestAssocAff' ICFInvestAssocAff,
up.jsondoc_ ->'Data' ->> 'ICFNetProfitB4Tax' ICFNetProfitB4Tax,
up.jsondoc_ ->'Data' ->> 'ICFOthFinancingCF' ICFOthFinancingCF,
up.jsondoc_ ->'Data' ->> 'ICFPurchOtherAsts' ICFPurchOtherAsts,
up.jsondoc_ ->'Data' ->> 'ICFPurchaseSaleTS' ICFPurchaseSaleTS,
up.jsondoc_ ->'Data' ->> 'InvestInJVPartner' InvestInJVPartner,
up.jsondoc_ ->'Data' ->> 'InvestmentInAssoc' InvestmentInAssoc,
up.jsondoc_ ->'Data' ->> 'InvestmentPropNet' InvestmentPropNet,
up.jsondoc_ ->'Data' ->> 'MinPensionLiabAdj' MinPensionLiabAdj,
up.jsondoc_ ->'Data' ->> 'MinReturnOnAssets' MinReturnOnAssets,
up.jsondoc_ ->'Data' ->> 'MinReturnOnEquity' MinReturnOnEquity,
up.jsondoc_ ->'Data' ->> 'MinWorkingCapital' MinWorkingCapital,
up.jsondoc_ ->'Data' ->> 'MinorityIntEquity' MinorityIntEquity,
up.jsondoc_ ->'Data' ->> 'MinorityInterests' MinorityInterests,
up.jsondoc_ ->'Data' ->> 'NetInterestIncExp' NetInterestIncExp,
up.jsondoc_ ->'Data' ->> 'NetOthFinanIncExp' NetOthFinanIncExp,
up.jsondoc_ ->'Data' ->> 'OthDebtServiceExp' OthDebtServiceExp,
up.jsondoc_ ->'Data' ->> 'OthFnclAstIncDvds' OthFnclAstIncDvds,
up.jsondoc_ ->'Data' ->> 'OthNonCurrentAsts' OthNonCurrentAsts,
up.jsondoc_ ->'Data' ->> 'OthNonIncRelTaxes' OthNonIncRelTaxes,
up.jsondoc_ ->'Data' ->> 'OthReceivablesLTP' OthReceivablesLTP,
up.jsondoc_ ->'Data' ->> 'OtherProvisionsCP' OtherProvisionsCP,
up.jsondoc_ ->'Data' ->> 'PBTToTotEquityRes' PBTToTotEquityRes,
up.jsondoc_ ->'Data' ->> 'PersonnelBenefExp' PersonnelBenefExp,
up.jsondoc_ ->'Data' ->> 'PlantAndEquipment' PlantAndEquipment,
up.jsondoc_ ->'Data' ->> 'ProfitB4TaxMargin' ProfitB4TaxMargin,
up.jsondoc_ ->'Data' ->> 'RetireBenefitsLTP' RetireBenefitsLTP,
up.jsondoc_ ->'Data' ->> 'RlzdForexTranslGL' RlzdForexTranslGL,
up.jsondoc_ ->'Data' ->> 'STOthLoansPayable' STOthLoansPayable,
up.jsondoc_ ->'Data' ->> 'SalestoWorkingCap' SalestoWorkingCap,
up.jsondoc_ ->'Data' ->> 'SecurOthFinAstsCP' SecurOthFinAstsCP,
up.jsondoc_ ->'Data' ->> 'ShareOptionCostCP' ShareOptionCostCP,
up.jsondoc_ ->'Data' ->> 'SocSecOthTaxesLTP' SocSecOthTaxesLTP,
up.jsondoc_ ->'Data' ->> 'StartUpCapitalExp' StartUpCapitalExp,
up.jsondoc_ ->'Data' ->> 'SustainableGrowth' SustainableGrowth,
up.jsondoc_ ->'Data' ->> 'TotEquityResLiabs' TotEquityResLiabs,
up.jsondoc_ ->'Data' ->> 'TotEquityReserves' TotEquityReserves,
up.jsondoc_ ->'Data' ->> 'TotLiabtoTotAsset' TotLiabtoTotAsset,
up.jsondoc_ ->'Data' ->> 'TotOthEqResIncExp' TotOthEqResIncExp,
up.jsondoc_ ->'Data' ->> 'TotalCurrentLiabs' TotalCurrentLiabs,
up.jsondoc_ ->'Data' ->> 'UnrealGainFixAsts' UnrealGainFixAsts,
up.jsondoc_ ->'Data' ->> 'UnrealGainInvests' UnrealGainInvests,
up.jsondoc_ ->'Data' ->> 'AccIntgbleAstAmort' AccIntgbleAstAmort,
up.jsondoc_ ->'Data' ->> 'AccuOthEqtyRsrvInc' AccuOthEqtyRsrvInc,
up.jsondoc_ ->'Data' ->> 'AccumGoodwilImpair' AccumGoodwilImpair,
up.jsondoc_ ->'Data' ->> 'AllOthNonCurrAssts' AllOthNonCurrAssts,
up.jsondoc_ ->'Data' ->> 'AllowForDoubtAccts' AllowForDoubtAccts,
up.jsondoc_ ->'Data' ->> 'BillingsInExcCosts' BillingsInExcCosts,
up.jsondoc_ ->'Data' ->> 'BorrFundtoEffTgWth' BorrFundtoEffTgWth,
up.jsondoc_ ->'Data' ->> 'CapAndRestrReserve' CapAndRestrReserve,
up.jsondoc_ ->'Data' ->> 'CapitalExpenditure' CapitalExpenditure,
up.jsondoc_ ->'Data' ->> 'CashAndEquivalents' CashAndEquivalents,
up.jsondoc_ ->'Data' ->> 'CommonShareCapital' CommonShareCapital,
up.jsondoc_ ->'Data' ->> 'CurrentIncomeTaxes' CurrentIncomeTaxes,
up.jsondoc_ ->'Data' ->> 'CustomerAdvancesCP' CustomerAdvancesCP,
up.jsondoc_ ->'Data' ->> 'DCFBegOfPeriodCash' DCFBegOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'DCFCFFromInvestAct' DCFCFFromInvestAct,
up.jsondoc_ ->'Data' ->> 'DCFChgNetFixAssets' DCFChgNetFixAssets,
up.jsondoc_ ->'Data' ->> 'DCFEndOfPeriodCash' DCFEndOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'DCFTotMovementCash' DCFTotMovementCash,
up.jsondoc_ ->'Data' ->> 'DecommEnvirCostLTP' DecommEnvirCostLTP,
up.jsondoc_ ->'Data' ->> 'DeferredIncTaxesNC' DeferredIncTaxesNC,
up.jsondoc_ ->'Data' ->> 'DividendPayoutRate' DividendPayoutRate,
up.jsondoc_ ->'Data' ->> 'ExpensesOwnWorkCap' ExpensesOwnWorkCap,
up.jsondoc_ ->'Data' ->> 'GrantsAndSubsidies' GrantsAndSubsidies,
up.jsondoc_ ->'Data' ->> 'ICFAcqSubNetCshAcq' ICFAcqSubNetCshAcq,
up.jsondoc_ ->'Data' ->> 'ICFBegOfPeriodCash' ICFBegOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'ICFCFDispFnclAsset' ICFCFDispFnclAsset,
up.jsondoc_ ->'Data' ->> 'ICFCFExtItemInvest' ICFCFExtItemInvest,
up.jsondoc_ ->'Data' ->> 'ICFCFExtraItemsFin' ICFCFExtraItemsFin,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmExtrItemOp' ICFCFFrmExtrItemOp,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmHedgingFin' ICFCFFrmHedgingFin,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmHedingOper' ICFCFFrmHedingOper,
up.jsondoc_ ->'Data' ->> 'ICFCFHedgingInvest' ICFCFHedgingInvest,
up.jsondoc_ ->'Data' ->> 'ICFChgFnclInstrTrd' ICFChgFnclInstrTrd,
up.jsondoc_ ->'Data' ->> 'ICFChgInCurrBorrow' ICFChgInCurrBorrow,
up.jsondoc_ ->'Data' ->> 'ICFChgInOthCurLiab' ICFChgInOthCurLiab,
up.jsondoc_ ->'Data' ->> 'ICFChgInProvisions' ICFChgInProvisions,
up.jsondoc_ ->'Data' ->> 'ICFChgPostEmplBnft' ICFChgPostEmplBnft,
up.jsondoc_ ->'Data' ->> 'ICFChgPrepayDefAst' ICFChgPrepayDefAst,
up.jsondoc_ ->'Data' ->> 'ICFDvdsPdMinShlder' ICFDvdsPdMinShlder,
up.jsondoc_ ->'Data' ->> 'ICFEndOfPeriodCash' ICFEndOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'ICFInterestExpense' ICFInterestExpense,
up.jsondoc_ ->'Data' ->> 'ICFInterestPaidFin' ICFInterestPaidFin,
up.jsondoc_ ->'Data' ->> 'ICFIssueCostNCBorr' ICFIssueCostNCBorr,
up.jsondoc_ ->'Data' ->> 'ICFNetFrgnExchDiff' ICFNetFrgnExchDiff,
up.jsondoc_ ->'Data' ->> 'ICFOthAftTaxIncExp' ICFOthAftTaxIncExp,
up.jsondoc_ ->'Data' ->> 'ICFPayFinLeaseLiab' ICFPayFinLeaseLiab,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleInvProp' ICFProcSaleInvProp,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleOthAsts' ICFProcSaleOthAsts,
up.jsondoc_ ->'Data' ->> 'ICFProcSlIntblAsts' ICFProcSlIntblAsts,
up.jsondoc_ ->'Data' ->> 'ICFProceedsSalePPE' ICFProceedsSalePPE,
up.jsondoc_ ->'Data' ->> 'ICFPurchFnclAssets' ICFPurchFnclAssets,
up.jsondoc_ ->'Data' ->> 'ICFPurchIntgblAsts' ICFPurchIntgblAsts,
up.jsondoc_ ->'Data' ->> 'ICFPurchInvestProp' ICFPurchInvestProp,
up.jsondoc_ ->'Data' ->> 'ICFTotMovementCash' ICFTotMovementCash,
up.jsondoc_ ->'Data' ->> 'ICFTranslAdjRelCsh' ICFTranslAdjRelCsh,
up.jsondoc_ ->'Data' ->> 'ICFUnexplAdjToCash' ICFUnexplAdjToCash,
up.jsondoc_ ->'Data' ->> 'IncFrmJointVenture' IncFrmJointVenture,
up.jsondoc_ ->'Data' ->> 'IncLossFrmRelParty' IncLossFrmRelParty,
up.jsondoc_ ->'Data' ->> 'IncomeTaxesPayable' IncomeTaxesPayable,
up.jsondoc_ ->'Data' ->> 'InvDaysExclCOSDepr' InvDaysExclCOSDepr,
up.jsondoc_ ->'Data' ->> 'LeaseholdImprvmnts' LeaseholdImprvmnts,
up.jsondoc_ ->'Data' ->> 'LiabsToEnterprises' LiabsToEnterprises,
up.jsondoc_ ->'Data' ->> 'MaxDebtToTangWorth' MaxDebtToTangWorth,
up.jsondoc_ ->'Data' ->> 'MaxNetTradeRecDays' MaxNetTradeRecDays,
up.jsondoc_ ->'Data' ->> 'MinNetProfitMargin' MinNetProfitMargin,
up.jsondoc_ ->'Data' ->> 'NetFixedAssetToTNW' NetFixedAssetToTNW,
up.jsondoc_ ->'Data' ->> 'NetProfDeprtoCPLTD' NetProfDeprtoCPLTD,
up.jsondoc_ ->'Data' ->> 'NumOutCommonShares' NumOutCommonShares,
up.jsondoc_ ->'Data' ->> 'OpLeaseCommitments' OpLeaseCommitments,
up.jsondoc_ ->'Data' ->> 'OpLeaseReceivables' OpLeaseReceivables,
up.jsondoc_ ->'Data' ->> 'OperExpExclDepAmor' OperExpExclDepAmor,
up.jsondoc_ ->'Data' ->> 'OthAdjRetainedProf' OthAdjRetainedProf,
up.jsondoc_ ->'Data' ->> 'OthIntangibleAsset' OthIntangibleAsset,
up.jsondoc_ ->'Data' ->> 'OthNonCurrentLiabs' OthNonCurrentLiabs,
up.jsondoc_ ->'Data' ->> 'OtherCurrentAssets' OtherCurrentAssets,
up.jsondoc_ ->'Data' ->> 'OtherIncomeExpense' OtherIncomeExpense,
up.jsondoc_ ->'Data' ->> 'OtherProvisionsLTP' OtherProvisionsLTP,
up.jsondoc_ ->'Data' ->> 'OtherReceivablesCP' OtherReceivablesCP,
up.jsondoc_ ->'Data' ->> 'ProvRetirementCost' ProvRetirementCost,
up.jsondoc_ ->'Data' ->> 'RentsRoyaltyIncome' RentsRoyaltyIncome,
up.jsondoc_ ->'Data' ->> 'RevaluationReserve' RevaluationReserve,
up.jsondoc_ ->'Data' ->> 'RtmntPlanActuarial' RtmntPlanActuarial,
up.jsondoc_ ->'Data' ->> 'STBankLoansPayable' STBankLoansPayable,
up.jsondoc_ ->'Data' ->> 'SecurOthFinAstsLTP' SecurOthFinAstsLTP,
up.jsondoc_ ->'Data' ->> 'ShareOptionCostLTP' ShareOptionCostLTP,
up.jsondoc_ ->'Data' ->> 'TotAdjRetainProfit' TotAdjRetainProfit,
up.jsondoc_ ->'Data' ->> 'TotalNonCurrAssets' TotalNonCurrAssets,
up.jsondoc_ ->'Data' ->> 'TransferToReserves' TransferToReserves,
up.jsondoc_ ->'Data' ->> 'UnrealGainInvestPr' UnrealGainInvestPr,
up.jsondoc_ ->'Data' ->> 'VehAndTranspoEquip' VehAndTranspoEquip,
up.jsondoc_ ->'Data' ->> 'CustomerAdvancesLTP' CustomerAdvancesLTP,
up.jsondoc_ ->'Data' ->> 'ICFInterestPaidOper' ICFInterestPaidOper,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleFnclAsts' ICFProcSaleFnclAsts,
up.jsondoc_ ->'Data' ->> 'SalesToNetFixedAsts' SalesToNetFixedAsts,
up.jsondoc_ ->'Data' ->> 'ShortTermSecurities' ShortTermSecurities,
up.jsondoc_ ->'Data' ->> 'UCAChgInFixedAssets' UCAChgInFixedAssets,
up.jsondoc_ ->'Data' ->> 'CashinHandandAtBanks' CashinHandandAtBanks,
up.jsondoc_ ->'Data' ->> 'GoodwillAmortization' GoodwillAmortization,
up.jsondoc_ ->'Data' ->> 'ICFCashPdtoEmployees' ICFCashPdtoEmployees,
up.jsondoc_ ->'Data' ->> 'ICFCashPdtoSuppliers' ICFCashPdtoSuppliers,
up.jsondoc_ ->'Data' ->> 'ICFMinorityInterests' ICFMinorityInterests,
up.jsondoc_ ->'Data' ->> 'SpecItemswEquityChar' SpecItemswEquityChar,
up.jsondoc_ ->'Data' ->> 'ConcessLicenseTrdmrks' ConcessLicenseTrdmrks,
up.jsondoc_ ->'Data' ->> 'ICFCashRecfrmCustomers' ICFCashRecfrmCustomers,
up.jsondoc_ ->'Data' ->> 'SubscribedCapCallNotPd' SubscribedCapCallNotPd,
up.jsondoc_ ->'Data' ->> 'TradePayDaysExclCOSDepr' TradePayDaysExclCOSDepr,
up.jsondoc_ ->'Data' ->> 'ICFCashFlowFromOpActDirect' ICFCashFlowFromOpActDirect,
up.jsondoc_ ->'Data' ->> 'Sales' Sales,
up.jsondoc_ ->'Data' ->> 'Deposits' Deposits,
up.jsondoc_ ->'Data' ->> 'CashAndCashEquivalents' CashAndCashEquivalents,
up.jsondoc_ ->'Data' ->> 'CashSurplus' CashSurplus,
up.jsondoc_ ->'Data' ->> 'LIBOR' LIBOR,
up.jsondoc_ ->'Data' ->> 'MoneyMarketRate' MoneyMarketRate,
up.jsondoc_ ->'Data' ->> 'RequiredBorrowings' RequiredBorrowings,
(up.jsondoc_ ->'Data' ->> 'StatementDate')::varchar(10)::date StatementDate,
--up.jsondoc_ ->'Data' ->> 'HistStmtVersionId' HistStmtVersionId,
up.wfid_::varchar wfid_,
up.taskid_::varchar taskid_,
up.versionid_::integer versionid_, 
up.statusid_::integer statusid_,
up.isdeleted_::boolean isdeleted_,
up.islatestversion_::boolean islatestversion_,
up.isvisible_::boolean isvisible_,
up.isvalid_::boolean isvalid_,
up.snapshotid_::integer snapshotid_,
up.contextuserid_::varchar contextuserid_ ,
up.createdby_::varchar createdby_ , 
up.createddate_::timestamp createddate_ , 
up.updatedby_::varchar updatedby_ , 
up.updateddate_::timestamp updateddate_ , 
t_ t_ ,
(case when up.updateddate_>up.createddate_ then up.updatedby_ else up.createdby_ end) as sourcepopulatedby_,
GREATEST(up.updateddate_,up.createddate_) as sourcepopulateddate_
from madata.uphiststmtfinancials up
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSDETAIL')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abupprojstmtfinancialsdetail - part a end', clock_timestamp();
ELSE
raise notice '% - Step abupprojstmtfinancialsdetail - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abupprojstmtfinancialsdetail;
CREATE TABLE olapts.abupprojstmtfinancialsdetail AS
select 
up.Id_::varchar projstmtfinancialid_,
up.pkid_,
up.jsondoc_->>'EntityId'::varchar entityid,
up.jsondoc_ -> 'Data' ->> 'CustomerName'::varchar customername,
up.jsondoc_ -> 'Data' ->> 'UserId'::varchar userid,
(up.jsondoc_->>'StatementId') statementid,
(up.jsondoc_->>'FinancialId') financialid,
up.jsondoc_->> 'FinancialTemplate'::varchar financialtemplatekey_,
cast(to_char((up.jsondoc_ -> 'Data' ->> 'StatementDate')::date,'YYYYMMDD') as integer) statementdatekey_,
(left(up.jsondoc_ -> 'Data' ->>'StatementYear',4))::numeric statementyear,
up.jsondoc_ ->> 'UserId'::varchar useridkey_,
up.jsondoc_ ->'Data' ->> 'Accountant'::varchar accountant,
up.jsondoc_ ->'Data' ->> 'AccountingStandard'::varchar accountingstandard, 
(up.jsondoc_ -> 'Data' ->>'OfficersComp')::numeric as officerscomp, 
up.jsondoc_ ->'Data' ->> 'Analyst'::varchar analyst, 
up.jsondoc_ ->'Data' ->> 'AuditMethod'::varchar auditmethod, 
((up.jsondoc_ -> 'Data' ->>'ReconcileID')::numeric)::integer as reconcileid,  
up.jsondoc_ ->'Data' ->> 'StatementType'::varchar statementtype,  
up.jsondoc_ ->'Data' ->> 'StatementSource'::varchar statementsource,
(up.jsondoc_ -> 'Data' ->>'StatementMonths')::numeric as statementmonths,
(up.jsondoc_ -> 'Data' ->>'Periods')::numeric as periods,
up.jsondoc_ ->'Data' ->> 'Cash' Cash,
up.jsondoc_ ->'Data' ->> 'EBIT' EBIT,
up.jsondoc_ ->'Data' ->> 'Land' Land,
up.jsondoc_ ->'Data' ->> 'CPLTD' CPLTD,
up.jsondoc_ ->'Data' ->> 'EBITDA' EBITDA,
up.jsondoc_ ->'Data' ->> 'LTDBank' LTDBank,
up.jsondoc_ ->'Data' ->> 'Patents' Patents,
up.jsondoc_ ->'Data' ->> 'CashToTA' CashToTA,
up.jsondoc_ ->'Data' ->> 'Goodwill' Goodwill,
up.jsondoc_ ->'Data' ->> 'LTDOther' LTDOther,
up.jsondoc_ ->'Data' ->> 'PBTToTNW' PBTToTNW,
up.jsondoc_ ->'Data' ->> 'Buildings' Buildings,
up.jsondoc_ ->'Data' ->> 'CPLTDBank' CPLTDBank,
up.jsondoc_ ->'Data' ->> 'DebtToTNW' DebtToTNW,
up.jsondoc_ ->'Data' ->> 'NetProfit' NetProfit,
up.jsondoc_ ->'Data' ->> 'AllOtherCL' AllOtherCL,
up.jsondoc_ ->'Data' ->> 'CPLTDOther' CPLTDOther,
up.jsondoc_ ->'Data' ->> 'MaxInvDays' MaxInvDays,
up.jsondoc_ ->'Data' ->> 'NWAToSales' NWAToSales,
up.jsondoc_ ->'Data' ->> 'OtherTaxes' OtherTaxes,
up.jsondoc_ ->'Data' ->> 'Overdrafts' Overdrafts,
up.jsondoc_ ->'Data' ->> 'QuickRatio' QuickRatio,
up.jsondoc_ ->'Data' ->> 'SalesToTNW' SalesToTNW,
up.jsondoc_ ->'Data' ->> 'CostOfSales' CostOfSales,
up.jsondoc_ ->'Data' ->> 'GrossProfit' GrossProfit,
up.jsondoc_ ->'Data' ->> 'Inventories' Inventories,
up.jsondoc_ ->'Data' ->> 'NetOpProfit' NetOpProfit,
up.jsondoc_ ->'Data' ->> 'OffBSAssets' OffBSAssets,
up.jsondoc_ ->'Data' ->> 'OtherEquity' OtherEquity,
up.jsondoc_ ->'Data' ->> 'ReturnOnTNW' ReturnOnTNW,
up.jsondoc_ ->'Data' ->> 'SalesGrowth' SalesGrowth,
up.jsondoc_ ->'Data' ->> 'TotalAssets' TotalAssets,
up.jsondoc_ ->'Data' ->> 'AuditOpinion' AuditOpinion,
up.jsondoc_ ->'Data' ->> 'CashAfterOps' CashAfterOps,
up.jsondoc_ ->'Data' ->> 'CurrentRatio' CurrentRatio,
up.jsondoc_ ->'Data' ->> 'DebtToEquity' DebtToEquity,
up.jsondoc_ ->'Data' ->> 'EBITDAGrowth' EBITDAGrowth,
up.jsondoc_ ->'Data' ->> 'EBITDAMargin' EBITDAMargin,
up.jsondoc_ ->'Data' ->> 'LongTermDebt' LongTermDebt,
up.jsondoc_ ->'Data' ->> 'NumEmployees' NumEmployees,
up.jsondoc_ ->'Data' ->> 'RDCapitalExp' RDCapitalExp,
up.jsondoc_ ->'Data' ->> 'SharePremium' SharePremium,
up.jsondoc_ ->'Data' ->> 'CashDividends' CashDividends,
up.jsondoc_ ->'Data' ->> 'InventoryDays' InventoryDays,
up.jsondoc_ ->'Data' ->> 'MaxSubordDebt' MaxSubordDebt,
up.jsondoc_ ->'Data' ->> 'MinQuickRatio' MinQuickRatio,
up.jsondoc_ ->'Data' ->> 'NetCashIncome' NetCashIncome,
up.jsondoc_ ->'Data' ->> 'OffBSLeverage' OffBSLeverage,
up.jsondoc_ ->'Data' ->> 'OtherOpIncome' OtherOpIncome,
up.jsondoc_ ->'Data' ->> 'OtherReserves' OtherReserves,
up.jsondoc_ ->'Data' ->> 'PrepaymentsCP' PrepaymentsCP,
up.jsondoc_ ->'Data' ->> 'RestructCosts' RestructCosts,
up.jsondoc_ ->'Data' ->> 'SalesRevenues' SalesRevenues,
up.jsondoc_ ->'Data' ->> 'TradePayables' TradePayables,
up.jsondoc_ ->'Data' ->> 'BadDebtExpense' BadDebtExpense,
up.jsondoc_ ->'Data' ->> 'BorFunToEquity' BorFunToEquity,
up.jsondoc_ ->'Data' ->> 'COGSToTradePay' COGSToTradePay,
up.jsondoc_ ->'Data' ->> 'DebtToNetWorth' DebtToNetWorth,
up.jsondoc_ ->'Data' ->> 'DeferredIntExp' DeferredIntExp,
up.jsondoc_ ->'Data' ->> 'DueToSholderCP' DueToSholderCP,
up.jsondoc_ ->'Data' ->> 'ExtraordIncExp' ExtraordIncExp,
up.jsondoc_ ->'Data' ->> 'GainInvestProp' GainInvestProp,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmFinAct' ICFCFFrmFinAct,
up.jsondoc_ ->'Data' ->> 'ICFDivsPaidFin' ICFDivsPaidFin,
up.jsondoc_ ->'Data' ->> 'ICFDivsRecOper' ICFDivsRecOper,
up.jsondoc_ ->'Data' ->> 'ICFPurchasePPE' ICFPurchasePPE,
up.jsondoc_ ->'Data' ->> 'InterestIncome' InterestIncome,
up.jsondoc_ ->'Data' ->> 'LTDConvertible' LTDConvertible,
up.jsondoc_ ->'Data' ->> 'MaxSalesGrowth' MaxSalesGrowth,
up.jsondoc_ ->'Data' ->> 'MinCashBalance' MinCashBalance,
up.jsondoc_ ->'Data' ->> 'MinCashFlowCov' MinCashFlowCov,
up.jsondoc_ ->'Data' ->> 'MinEarningsCov' MinEarningsCov,
up.jsondoc_ ->'Data' ->> 'MinInterestCov' MinInterestCov,
up.jsondoc_ ->'Data' ->> 'NetFixedAssets' NetFixedAssets,
up.jsondoc_ ->'Data' ->> 'NetIntangibles' NetIntangibles,
up.jsondoc_ ->'Data' ->> 'NetTradeReceiv' NetTradeReceiv,
up.jsondoc_ ->'Data' ->> 'NotesPayableCP' NotesPayableCP,
up.jsondoc_ ->'Data' ->> 'OpLeaseRentExp' OpLeaseRentExp,
up.jsondoc_ ->'Data' ->> 'OthNonOpIncExp' OthNonOpIncExp,
up.jsondoc_ ->'Data' ->> 'OthOperExpProv' OthOperExpProv,
up.jsondoc_ ->'Data' ->> 'PrepaymentsLTP' PrepaymentsLTP,
up.jsondoc_ ->'Data' ->> 'RestrictedCash' RestrictedCash,
up.jsondoc_ ->'Data' ->> 'RestructProvCP' RestructProvCP,
up.jsondoc_ ->'Data' ->> 'ReturnOnAssets' ReturnOnAssets,
up.jsondoc_ ->'Data' ->> 'STLoansPayable' STLoansPayable,
up.jsondoc_ ->'Data' ->> 'TaxProvisionCP' TaxProvisionCP,
up.jsondoc_ ->'Data' ->> 'TotalIncomeTax' TotalIncomeTax,
up.jsondoc_ ->'Data' ->> 'TreasuryShares' TreasuryShares,
up.jsondoc_ ->'Data' ->> 'WorkingCapital' WorkingCapital,
up.jsondoc_ ->'Data' ->> 'AllOthCurrLiabs' AllOthCurrLiabs,
up.jsondoc_ ->'Data' ->> 'AllOtherNCLiabs' AllOtherNCLiabs,
up.jsondoc_ ->'Data' ->> 'COGSToInventory' COGSToInventory,
up.jsondoc_ ->'Data' ->> 'CashEquivalents' CashEquivalents,
up.jsondoc_ ->'Data' ->> 'CashFrTradActiv' CashFrTradActiv,
up.jsondoc_ ->'Data' ->> 'CashGrossMargin' CashGrossMargin,
up.jsondoc_ ->'Data' ->> 'CashPdDivAndInt' CashPdDivAndInt,
up.jsondoc_ ->'Data' ->> 'CashPdToSupplrs' CashPdToSupplrs,
up.jsondoc_ ->'Data' ->> 'DCFCFFrmOperAct' DCFCFFrmOperAct,
up.jsondoc_ ->'Data' ->> 'DCFCFFromFinAct' DCFCFFromFinAct,
up.jsondoc_ ->'Data' ->> 'DCFChgNetIntang' DCFChgNetIntang,
up.jsondoc_ ->'Data' ->> 'DefIncomeTaxPay' DefIncomeTaxPay,
up.jsondoc_ ->'Data' ->> 'DeferredTaxAsts' DeferredTaxAsts,
up.jsondoc_ ->'Data' ->> 'DepAmortToSales' DepAmortToSales,
up.jsondoc_ ->'Data' ->> 'DeprecImpairCOS' DeprecImpairCOS,
up.jsondoc_ ->'Data' ->> 'DerivHedgAstsCP' DerivHedgAstsCP,
up.jsondoc_ ->'Data' ->> 'DerivHedgLiabCP' DerivHedgLiabCP,
up.jsondoc_ ->'Data' ->> 'DueFrmJVPartner' DueFrmJVPartner,
up.jsondoc_ ->'Data' ->> 'DueFrmSholderCP' DueFrmSholderCP,
up.jsondoc_ ->'Data' ->> 'DueToRelPartyCP' DueToRelPartyCP,
up.jsondoc_ ->'Data' ->> 'DueToSholderLTP' DueToSholderLTP,
up.jsondoc_ ->'Data' ->> 'EffTangNetWorth' EffTangNetWorth,
up.jsondoc_ ->'Data' ->> 'FinanceLeasesCP' FinanceLeasesCP,
up.jsondoc_ ->'Data' ->> 'GainDispDiscOps' GainDispDiscOps,
up.jsondoc_ ->'Data' ->> 'GeneralAdminExp' GeneralAdminExp,
up.jsondoc_ ->'Data' ->> 'HedgingReserves' HedgingReserves,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmOperAct' ICFCFFrmOperAct,
up.jsondoc_ ->'Data' ->> 'ICFDepreciation' ICFDepreciation,
up.jsondoc_ ->'Data' ->> 'ICFDivsPaidOper' ICFDivsPaidOper,
up.jsondoc_ ->'Data' ->> 'ICFIncTaxesPaid' ICFIncTaxesPaid,
up.jsondoc_ ->'Data' ->> 'ICFIncomeTaxExp' ICFIncomeTaxExp,
up.jsondoc_ ->'Data' ->> 'ICFProcNCBorrow' ICFProcNCBorrow,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleSubs' ICFProcSaleSubs,
up.jsondoc_ ->'Data' ->> 'InterestExpense' InterestExpense,
up.jsondoc_ ->'Data' ->> 'InterestPayable' InterestPayable,
up.jsondoc_ ->'Data' ->> 'LTDSubordinated' LTDSubordinated,
up.jsondoc_ ->'Data' ->> 'MaxTradePayDays' MaxTradePayDays,
up.jsondoc_ ->'Data' ->> 'MinCurrentRatio' MinCurrentRatio,
up.jsondoc_ ->'Data' ->> 'MinTangNetWorth' MinTangNetWorth,
up.jsondoc_ ->'Data' ->> 'NetCashAfterOps' NetCashAfterOps,
up.jsondoc_ ->'Data' ->> 'NetOpProfGrowth' NetOpProfGrowth,
up.jsondoc_ ->'Data' ->> 'NetOpProfMargin' NetOpProfMargin,
up.jsondoc_ ->'Data' ->> 'NetProfitGrowth' NetProfitGrowth,
up.jsondoc_ ->'Data' ->> 'NetProfitMargin' NetProfitMargin,
up.jsondoc_ ->'Data' ->> 'NetTradeRecDays' NetTradeRecDays,
up.jsondoc_ ->'Data' ->> 'OffrCompToSales' OffrCompToSales,
up.jsondoc_ ->'Data' ->> 'OthEquityResAdj' OthEquityResAdj,
up.jsondoc_ ->'Data' ->> 'OtherOpExpenses' OtherOpExpenses,
up.jsondoc_ ->'Data' ->> 'PermanentEquity' PermanentEquity,
up.jsondoc_ ->'Data' ->> 'PriorPeriodAdjs' PriorPeriodAdjs,
up.jsondoc_ ->'Data' ->> 'PriorYearTaxAdj' PriorYearTaxAdj,
up.jsondoc_ ->'Data' ->> 'ProfitBeforeTax' ProfitBeforeTax,
up.jsondoc_ ->'Data' ->> 'ResearchDevelop' ResearchDevelop,
up.jsondoc_ ->'Data' ->> 'RestructProvLTP' RestructProvLTP,
up.jsondoc_ ->'Data' ->> 'RetainedProfits' RetainedProfits,
up.jsondoc_ ->'Data' ->> 'SalesToTradeRec' SalesToTradeRec,
up.jsondoc_ ->'Data' ->> 'ShareOptionCost' ShareOptionCost,
up.jsondoc_ ->'Data' ->> 'TaxProvisionLTP' TaxProvisionLTP,
up.jsondoc_ ->'Data' ->> 'TotNonCurrLiabs' TotNonCurrLiabs,
up.jsondoc_ ->'Data' ->> 'TotalCurrAssets' TotalCurrAssets,
up.jsondoc_ ->'Data' ->> 'TotalIntExpense' TotalIntExpense,
up.jsondoc_ ->'Data' ->> 'TotalOpExpenses' TotalOpExpenses,
up.jsondoc_ ->'Data' ->> 'AdjstsScrtznProg' AdjstsScrtznProg,
up.jsondoc_ ->'Data' ->> 'AllOthCurrAssets' AllOthCurrAssets,
up.jsondoc_ ->'Data' ->> 'AllOtherExpenses' AllOtherExpenses,
up.jsondoc_ ->'Data' ->> 'AstsPlgedScrtztn' AstsPlgedScrtztn,
up.jsondoc_ ->'Data' ->> 'AstsRelToDiscOps' AstsRelToDiscOps,
up.jsondoc_ ->'Data' ->> 'BorrFundtoEBITDA' BorrFundtoEBITDA,
up.jsondoc_ ->'Data' ->> 'CPLTDConvertible' CPLTDConvertible,
up.jsondoc_ ->'Data' ->> 'CapitalizedCosts' CapitalizedCosts,
up.jsondoc_ ->'Data' ->> 'CashAftDebtAmort' CashAftDebtAmort,
up.jsondoc_ ->'Data' ->> 'CashClctdFrSales' CashClctdFrSales,
up.jsondoc_ ->'Data' ->> 'CashDivComShares' CashDivComShares,
up.jsondoc_ ->'Data' ->> 'CashFlowCoverage' CashFlowCoverage,
up.jsondoc_ ->'Data' ->> 'CashPdForOpCosts' CashPdForOpCosts,
up.jsondoc_ ->'Data' ->> 'ConstrInProgress' ConstrInProgress,
up.jsondoc_ ->'Data' ->> 'CurrentTaxReceiv' CurrentTaxReceiv,
up.jsondoc_ ->'Data' ->> 'DbtLessSubEffTNW' DbtLessSubEffTNW,
up.jsondoc_ ->'Data' ->> 'DeferredHedgGain' DeferredHedgGain,
up.jsondoc_ ->'Data' ->> 'DeferredIncomeCP' DeferredIncomeCP,
up.jsondoc_ ->'Data' ->> 'DerivHedgAstsLTP' DerivHedgAstsLTP,
up.jsondoc_ ->'Data' ->> 'DerivHedgLiabLTP' DerivHedgLiabLTP,
up.jsondoc_ ->'Data' ->> 'DividendsPayable' DividendsPayable,
up.jsondoc_ ->'Data' ->> 'DueFrmRelPartyCP' DueFrmRelPartyCP,
up.jsondoc_ ->'Data' ->> 'DueFrmSholderLTP' DueFrmSholderLTP,
up.jsondoc_ ->'Data' ->> 'DueToRelPartyLTP' DueToRelPartyLTP,
up.jsondoc_ ->'Data' ->> 'EarningsCoverage' EarningsCoverage,
up.jsondoc_ ->'Data' ->> 'EffectiveTaxRate' EffectiveTaxRate,
up.jsondoc_ ->'Data' ->> 'ExtraordIncExpNC' ExtraordIncExpNC,
up.jsondoc_ ->'Data' ->> 'FMVAdjFnclAssets' FMVAdjFnclAssets,
up.jsondoc_ ->'Data' ->> 'FinanceLeasesLTP' FinanceLeasesLTP,
up.jsondoc_ ->'Data' ->> 'GainLossFnclAsts' GainLossFnclAsts,
up.jsondoc_ ->'Data' ->> 'GoodsAndServices' GoodsAndServices,
up.jsondoc_ ->'Data' ->> 'GrossFixedAssets' GrossFixedAssets,
up.jsondoc_ ->'Data' ->> 'ICFBorrowCostsPd' ICFBorrowCostsPd,
up.jsondoc_ ->'Data' ->> 'ICFChgInDefTaxes' ICFChgInDefTaxes,
up.jsondoc_ ->'Data' ->> 'ICFChgInOtherRec' ICFChgInOtherRec,
up.jsondoc_ ->'Data' ->> 'ICFChgInTaxesPay' ICFChgInTaxesPay,
up.jsondoc_ ->'Data' ->> 'ICFChgInTradePay' ICFChgInTradePay,
up.jsondoc_ ->'Data' ->> 'ICFChgInTradeRec' ICFChgInTradeRec,
up.jsondoc_ ->'Data' ->> 'ICFChgOthWrkgCap' ICFChgOthWrkgCap,
up.jsondoc_ ->'Data' ->> 'ICFImpactChgExch' ICFImpactChgExch,
up.jsondoc_ ->'Data' ->> 'ICFIncomeTaxPaid' ICFIncomeTaxPaid,
up.jsondoc_ ->'Data' ->> 'ICFIntIncRecdInv' ICFIntIncRecdInv,
up.jsondoc_ ->'Data' ->> 'ICFNetProcIssCap' ICFNetProcIssCap,
up.jsondoc_ ->'Data' ->> 'ICFNetProfitLoss' ICFNetProfitLoss,
up.jsondoc_ ->'Data' ->> 'ICFOthNonCashAdj' ICFOthNonCashAdj,
up.jsondoc_ ->'Data' ->> 'ICFOtherInvestCF' ICFOtherInvestCF,
up.jsondoc_ ->'Data' ->> 'ICFRepayNCBorrow' ICFRepayNCBorrow,
up.jsondoc_ ->'Data' ->> 'ICFSalesAssocAff' ICFSalesAssocAff,
up.jsondoc_ ->'Data' ->> 'InterestCoverage' InterestCoverage,
up.jsondoc_ ->'Data' ->> 'MaterialExpenses' MaterialExpenses,
up.jsondoc_ ->'Data' ->> 'MaxCapitalExpend' MaxCapitalExpend,
up.jsondoc_ ->'Data' ->> 'MaxCashDividends' MaxCashDividends,
up.jsondoc_ ->'Data' ->> 'MaxOffBSLeverage' MaxOffBSLeverage,
up.jsondoc_ ->'Data' ->> 'NumOutPrefShares' NumOutPrefShares,
up.jsondoc_ ->'Data' ->> 'OffBSLiabilities' OffBSLiabilities,
up.jsondoc_ ->'Data' ->> 'OthIncAndTaxesPd' OthIncAndTaxesPd,
up.jsondoc_ ->'Data' ->> 'OthNonOpIncExpNC' OthNonOpIncExpNC,
up.jsondoc_ ->'Data' ->> 'OtherFixedAssets' OtherFixedAssets,
up.jsondoc_ ->'Data' ->> 'PBTToTotalAssets' PBTToTotalAssets,
up.jsondoc_ ->'Data' ->> 'PrefShareCapital' PrefShareCapital,
up.jsondoc_ ->'Data' ->> 'ProfitB4ExtItems' ProfitB4ExtItems,
up.jsondoc_ ->'Data' ->> 'RetireBenefitsCP' RetireBenefitsCP,
up.jsondoc_ ->'Data' ->> 'ReturnOnTotEqRes' ReturnOnTotEqRes,
up.jsondoc_ ->'Data' ->> 'STDebtToCurLiabs' STDebtToCurLiabs,
up.jsondoc_ ->'Data' ->> 'SalesToTotalAsts' SalesToTotalAsts,
up.jsondoc_ ->'Data' ->> 'SellMarketingExp' SellMarketingExp,
up.jsondoc_ ->'Data' ->> 'SocSecOthTaxesCP' SocSecOthTaxesCP,
up.jsondoc_ ->'Data' ->> 'StockDividendsNC' StockDividendsNC,
up.jsondoc_ ->'Data' ->> 'SubordDebtEquity' SubordDebtEquity,
up.jsondoc_ ->'Data' ->> 'SubordinatedDebt' SubordinatedDebt,
up.jsondoc_ ->'Data' ->> 'TangibleNetWorth' TangibleNetWorth,
up.jsondoc_ ->'Data' ->> 'TotalAssetGrowth' TotalAssetGrowth,
up.jsondoc_ ->'Data' ->> 'TotalCostofSales' TotalCostofSales,
up.jsondoc_ ->'Data' ->> 'TotalLiabilities' TotalLiabilities,
up.jsondoc_ ->'Data' ->> 'TradePayableDays' TradePayableDays,
up.jsondoc_ ->'Data' ->> 'TradePayablesLTP' TradePayablesLTP,
up.jsondoc_ ->'Data' ->> 'TradeReceivGross' TradeReceivGross,
up.jsondoc_ ->'Data' ->> 'AccumDeprecImpair' AccumDeprecImpair,
up.jsondoc_ ->'Data' ->> 'AmortImpairIntgbl' AmortImpairIntgbl,
up.jsondoc_ ->'Data' ->> 'BorrFundtoTotLiab' BorrFundtoTotLiab,
up.jsondoc_ ->'Data' ->> 'CPLTDSubordinated' CPLTDSubordinated,
up.jsondoc_ ->'Data' ->> 'CashDivPrefShares' CashDivPrefShares,
up.jsondoc_ ->'Data' ->> 'CgsInventoriesWIP' CgsInventoriesWIP,
up.jsondoc_ ->'Data' ->> 'DCFTotAdjustments' DCFTotAdjustments,
up.jsondoc_ ->'Data' ->> 'DecommEnvirCostCP' DecommEnvirCostCP,
up.jsondoc_ ->'Data' ->> 'DeferredIncomeLTP' DeferredIncomeLTP,
up.jsondoc_ ->'Data' ->> 'DeprecImpairOpExp' DeprecImpairOpExp,
up.jsondoc_ ->'Data' ->> 'DerivativesFMVAdj' DerivativesFMVAdj,
up.jsondoc_ ->'Data' ->> 'DueFrmRelPartyLTP' DueFrmRelPartyLTP,
up.jsondoc_ ->'Data' ->> 'EquityAndReserves' EquityAndReserves,
up.jsondoc_ ->'Data' ->> 'ExchGainSaleOfBus' ExchGainSaleOfBus,
up.jsondoc_ ->'Data' ->> 'ExchRatePeriodAvg' ExchRatePeriodAvg,
up.jsondoc_ ->'Data' ->> 'ExchRatePeriodEnd' ExchRatePeriodEnd,
up.jsondoc_ ->'Data' ->> 'ForexTranslEquity' ForexTranslEquity,
up.jsondoc_ ->'Data' ->> 'FurnitureFixtures' FurnitureFixtures,
up.jsondoc_ ->'Data' ->> 'GainDispFixedAsts' GainDispFixedAsts,
up.jsondoc_ ->'Data' ->> 'GrossProfitMargin' GrossProfitMargin,
up.jsondoc_ ->'Data' ->> 'ICFAmortAstImpair' ICFAmortAstImpair,
up.jsondoc_ ->'Data' ->> 'ICFAssocProfShare' ICFAssocProfShare,
up.jsondoc_ ->'Data' ->> 'ICFCFDispFixAsset' ICFCFDispFixAsset,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmInvestAct' ICFCFFrmInvestAct,
up.jsondoc_ ->'Data' ->> 'ICFChgInInventory' ICFChgInInventory,
up.jsondoc_ ->'Data' ->> 'ICFChgOthCurrAsts' ICFChgOthCurrAsts,
up.jsondoc_ ->'Data' ->> 'ICFDivsRecdInvest' ICFDivsRecdInvest,
up.jsondoc_ ->'Data' ->> 'ICFIntIncRecdOper' ICFIntIncRecdOper,
up.jsondoc_ ->'Data' ->> 'ICFInvestAssocAff' ICFInvestAssocAff,
up.jsondoc_ ->'Data' ->> 'ICFNetProfitB4Tax' ICFNetProfitB4Tax,
up.jsondoc_ ->'Data' ->> 'ICFOthFinancingCF' ICFOthFinancingCF,
up.jsondoc_ ->'Data' ->> 'ICFPurchOtherAsts' ICFPurchOtherAsts,
up.jsondoc_ ->'Data' ->> 'ICFPurchaseSaleTS' ICFPurchaseSaleTS,
up.jsondoc_ ->'Data' ->> 'InvestInJVPartner' InvestInJVPartner,
up.jsondoc_ ->'Data' ->> 'InvestmentInAssoc' InvestmentInAssoc,
up.jsondoc_ ->'Data' ->> 'InvestmentPropNet' InvestmentPropNet,
up.jsondoc_ ->'Data' ->> 'MinPensionLiabAdj' MinPensionLiabAdj,
up.jsondoc_ ->'Data' ->> 'MinReturnOnAssets' MinReturnOnAssets,
up.jsondoc_ ->'Data' ->> 'MinReturnOnEquity' MinReturnOnEquity,
up.jsondoc_ ->'Data' ->> 'MinWorkingCapital' MinWorkingCapital,
up.jsondoc_ ->'Data' ->> 'MinorityIntEquity' MinorityIntEquity,
up.jsondoc_ ->'Data' ->> 'MinorityInterests' MinorityInterests,
up.jsondoc_ ->'Data' ->> 'NetInterestIncExp' NetInterestIncExp,
up.jsondoc_ ->'Data' ->> 'NetOthFinanIncExp' NetOthFinanIncExp,
up.jsondoc_ ->'Data' ->> 'OthDebtServiceExp' OthDebtServiceExp,
up.jsondoc_ ->'Data' ->> 'OthFnclAstIncDvds' OthFnclAstIncDvds,
up.jsondoc_ ->'Data' ->> 'OthNonCurrentAsts' OthNonCurrentAsts,
up.jsondoc_ ->'Data' ->> 'OthNonIncRelTaxes' OthNonIncRelTaxes,
up.jsondoc_ ->'Data' ->> 'OthReceivablesLTP' OthReceivablesLTP,
up.jsondoc_ ->'Data' ->> 'OtherProvisionsCP' OtherProvisionsCP,
up.jsondoc_ ->'Data' ->> 'PBTToTotEquityRes' PBTToTotEquityRes,
up.jsondoc_ ->'Data' ->> 'PersonnelBenefExp' PersonnelBenefExp,
up.jsondoc_ ->'Data' ->> 'PlantAndEquipment' PlantAndEquipment,
up.jsondoc_ ->'Data' ->> 'ProfitB4TaxMargin' ProfitB4TaxMargin,
up.jsondoc_ ->'Data' ->> 'RetireBenefitsLTP' RetireBenefitsLTP,
up.jsondoc_ ->'Data' ->> 'RlzdForexTranslGL' RlzdForexTranslGL,
up.jsondoc_ ->'Data' ->> 'STOthLoansPayable' STOthLoansPayable,
up.jsondoc_ ->'Data' ->> 'SalestoWorkingCap' SalestoWorkingCap,
up.jsondoc_ ->'Data' ->> 'SecurOthFinAstsCP' SecurOthFinAstsCP,
up.jsondoc_ ->'Data' ->> 'ShareOptionCostCP' ShareOptionCostCP,
up.jsondoc_ ->'Data' ->> 'SocSecOthTaxesLTP' SocSecOthTaxesLTP,
up.jsondoc_ ->'Data' ->> 'StartUpCapitalExp' StartUpCapitalExp,
up.jsondoc_ ->'Data' ->> 'SustainableGrowth' SustainableGrowth,
up.jsondoc_ ->'Data' ->> 'TotEquityResLiabs' TotEquityResLiabs,
up.jsondoc_ ->'Data' ->> 'TotEquityReserves' TotEquityReserves,
up.jsondoc_ ->'Data' ->> 'TotLiabtoTotAsset' TotLiabtoTotAsset,
up.jsondoc_ ->'Data' ->> 'TotOthEqResIncExp' TotOthEqResIncExp,
up.jsondoc_ ->'Data' ->> 'TotalCurrentLiabs' TotalCurrentLiabs,
up.jsondoc_ ->'Data' ->> 'UnrealGainFixAsts' UnrealGainFixAsts,
up.jsondoc_ ->'Data' ->> 'UnrealGainInvests' UnrealGainInvests,
up.jsondoc_ ->'Data' ->> 'AccIntgbleAstAmort' AccIntgbleAstAmort,
up.jsondoc_ ->'Data' ->> 'AccuOthEqtyRsrvInc' AccuOthEqtyRsrvInc,
up.jsondoc_ ->'Data' ->> 'AccumGoodwilImpair' AccumGoodwilImpair,
up.jsondoc_ ->'Data' ->> 'AllOthNonCurrAssts' AllOthNonCurrAssts,
up.jsondoc_ ->'Data' ->> 'AllowForDoubtAccts' AllowForDoubtAccts,
up.jsondoc_ ->'Data' ->> 'BillingsInExcCosts' BillingsInExcCosts,
up.jsondoc_ ->'Data' ->> 'BorrFundtoEffTgWth' BorrFundtoEffTgWth,
up.jsondoc_ ->'Data' ->> 'CapAndRestrReserve' CapAndRestrReserve,
up.jsondoc_ ->'Data' ->> 'CapitalExpenditure' CapitalExpenditure,
up.jsondoc_ ->'Data' ->> 'CashAndEquivalents' CashAndEquivalents,
up.jsondoc_ ->'Data' ->> 'CommonShareCapital' CommonShareCapital,
up.jsondoc_ ->'Data' ->> 'CurrentIncomeTaxes' CurrentIncomeTaxes,
up.jsondoc_ ->'Data' ->> 'CustomerAdvancesCP' CustomerAdvancesCP,
up.jsondoc_ ->'Data' ->> 'DCFBegOfPeriodCash' DCFBegOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'DCFCFFromInvestAct' DCFCFFromInvestAct,
up.jsondoc_ ->'Data' ->> 'DCFChgNetFixAssets' DCFChgNetFixAssets,
up.jsondoc_ ->'Data' ->> 'DCFEndOfPeriodCash' DCFEndOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'DCFTotMovementCash' DCFTotMovementCash,
up.jsondoc_ ->'Data' ->> 'DecommEnvirCostLTP' DecommEnvirCostLTP,
up.jsondoc_ ->'Data' ->> 'DeferredIncTaxesNC' DeferredIncTaxesNC,
up.jsondoc_ ->'Data' ->> 'DividendPayoutRate' DividendPayoutRate,
up.jsondoc_ ->'Data' ->> 'ExpensesOwnWorkCap' ExpensesOwnWorkCap,
up.jsondoc_ ->'Data' ->> 'GrantsAndSubsidies' GrantsAndSubsidies,
up.jsondoc_ ->'Data' ->> 'ICFAcqSubNetCshAcq' ICFAcqSubNetCshAcq,
up.jsondoc_ ->'Data' ->> 'ICFBegOfPeriodCash' ICFBegOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'ICFCFDispFnclAsset' ICFCFDispFnclAsset,
up.jsondoc_ ->'Data' ->> 'ICFCFExtItemInvest' ICFCFExtItemInvest,
up.jsondoc_ ->'Data' ->> 'ICFCFExtraItemsFin' ICFCFExtraItemsFin,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmExtrItemOp' ICFCFFrmExtrItemOp,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmHedgingFin' ICFCFFrmHedgingFin,
up.jsondoc_ ->'Data' ->> 'ICFCFFrmHedingOper' ICFCFFrmHedingOper,
up.jsondoc_ ->'Data' ->> 'ICFCFHedgingInvest' ICFCFHedgingInvest,
up.jsondoc_ ->'Data' ->> 'ICFChgFnclInstrTrd' ICFChgFnclInstrTrd,
up.jsondoc_ ->'Data' ->> 'ICFChgInCurrBorrow' ICFChgInCurrBorrow,
up.jsondoc_ ->'Data' ->> 'ICFChgInOthCurLiab' ICFChgInOthCurLiab,
up.jsondoc_ ->'Data' ->> 'ICFChgInProvisions' ICFChgInProvisions,
up.jsondoc_ ->'Data' ->> 'ICFChgPostEmplBnft' ICFChgPostEmplBnft,
up.jsondoc_ ->'Data' ->> 'ICFChgPrepayDefAst' ICFChgPrepayDefAst,
up.jsondoc_ ->'Data' ->> 'ICFDvdsPdMinShlder' ICFDvdsPdMinShlder,
up.jsondoc_ ->'Data' ->> 'ICFEndOfPeriodCash' ICFEndOfPeriodCash,
up.jsondoc_ ->'Data' ->> 'ICFInterestExpense' ICFInterestExpense,
up.jsondoc_ ->'Data' ->> 'ICFInterestPaidFin' ICFInterestPaidFin,
up.jsondoc_ ->'Data' ->> 'ICFIssueCostNCBorr' ICFIssueCostNCBorr,
up.jsondoc_ ->'Data' ->> 'ICFNetFrgnExchDiff' ICFNetFrgnExchDiff,
up.jsondoc_ ->'Data' ->> 'ICFOthAftTaxIncExp' ICFOthAftTaxIncExp,
up.jsondoc_ ->'Data' ->> 'ICFPayFinLeaseLiab' ICFPayFinLeaseLiab,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleInvProp' ICFProcSaleInvProp,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleOthAsts' ICFProcSaleOthAsts,
up.jsondoc_ ->'Data' ->> 'ICFProcSlIntblAsts' ICFProcSlIntblAsts,
up.jsondoc_ ->'Data' ->> 'ICFProceedsSalePPE' ICFProceedsSalePPE,
up.jsondoc_ ->'Data' ->> 'ICFPurchFnclAssets' ICFPurchFnclAssets,
up.jsondoc_ ->'Data' ->> 'ICFPurchIntgblAsts' ICFPurchIntgblAsts,
up.jsondoc_ ->'Data' ->> 'ICFPurchInvestProp' ICFPurchInvestProp,
up.jsondoc_ ->'Data' ->> 'ICFTotMovementCash' ICFTotMovementCash,
up.jsondoc_ ->'Data' ->> 'ICFTranslAdjRelCsh' ICFTranslAdjRelCsh,
up.jsondoc_ ->'Data' ->> 'ICFUnexplAdjToCash' ICFUnexplAdjToCash,
up.jsondoc_ ->'Data' ->> 'IncFrmJointVenture' IncFrmJointVenture,
up.jsondoc_ ->'Data' ->> 'IncLossFrmRelParty' IncLossFrmRelParty,
up.jsondoc_ ->'Data' ->> 'IncomeTaxesPayable' IncomeTaxesPayable,
up.jsondoc_ ->'Data' ->> 'InvDaysExclCOSDepr' InvDaysExclCOSDepr,
up.jsondoc_ ->'Data' ->> 'LeaseholdImprvmnts' LeaseholdImprvmnts,
up.jsondoc_ ->'Data' ->> 'LiabsToEnterprises' LiabsToEnterprises,
up.jsondoc_ ->'Data' ->> 'MaxDebtToTangWorth' MaxDebtToTangWorth,
up.jsondoc_ ->'Data' ->> 'MaxNetTradeRecDays' MaxNetTradeRecDays,
up.jsondoc_ ->'Data' ->> 'MinNetProfitMargin' MinNetProfitMargin,
up.jsondoc_ ->'Data' ->> 'NetFixedAssetToTNW' NetFixedAssetToTNW,
up.jsondoc_ ->'Data' ->> 'NetProfDeprtoCPLTD' NetProfDeprtoCPLTD,
up.jsondoc_ ->'Data' ->> 'NumOutCommonShares' NumOutCommonShares,
up.jsondoc_ ->'Data' ->> 'OpLeaseCommitments' OpLeaseCommitments,
up.jsondoc_ ->'Data' ->> 'OpLeaseReceivables' OpLeaseReceivables,
up.jsondoc_ ->'Data' ->> 'OperExpExclDepAmor' OperExpExclDepAmor,
up.jsondoc_ ->'Data' ->> 'OthAdjRetainedProf' OthAdjRetainedProf,
up.jsondoc_ ->'Data' ->> 'OthIntangibleAsset' OthIntangibleAsset,
up.jsondoc_ ->'Data' ->> 'OthNonCurrentLiabs' OthNonCurrentLiabs,
up.jsondoc_ ->'Data' ->> 'OtherCurrentAssets' OtherCurrentAssets,
up.jsondoc_ ->'Data' ->> 'OtherIncomeExpense' OtherIncomeExpense,
up.jsondoc_ ->'Data' ->> 'OtherProvisionsLTP' OtherProvisionsLTP,
up.jsondoc_ ->'Data' ->> 'OtherReceivablesCP' OtherReceivablesCP,
up.jsondoc_ ->'Data' ->> 'ProvRetirementCost' ProvRetirementCost,
up.jsondoc_ ->'Data' ->> 'RentsRoyaltyIncome' RentsRoyaltyIncome,
up.jsondoc_ ->'Data' ->> 'RevaluationReserve' RevaluationReserve,
up.jsondoc_ ->'Data' ->> 'RtmntPlanActuarial' RtmntPlanActuarial,
up.jsondoc_ ->'Data' ->> 'STBankLoansPayable' STBankLoansPayable,
up.jsondoc_ ->'Data' ->> 'SecurOthFinAstsLTP' SecurOthFinAstsLTP,
up.jsondoc_ ->'Data' ->> 'ShareOptionCostLTP' ShareOptionCostLTP,
up.jsondoc_ ->'Data' ->> 'TotAdjRetainProfit' TotAdjRetainProfit,
up.jsondoc_ ->'Data' ->> 'TotalNonCurrAssets' TotalNonCurrAssets,
up.jsondoc_ ->'Data' ->> 'TransferToReserves' TransferToReserves,
up.jsondoc_ ->'Data' ->> 'UnrealGainInvestPr' UnrealGainInvestPr,
up.jsondoc_ ->'Data' ->> 'VehAndTranspoEquip' VehAndTranspoEquip,
up.jsondoc_ ->'Data' ->> 'CustomerAdvancesLTP' CustomerAdvancesLTP,
up.jsondoc_ ->'Data' ->> 'ICFInterestPaidOper' ICFInterestPaidOper,
up.jsondoc_ ->'Data' ->> 'ICFProcSaleFnclAsts' ICFProcSaleFnclAsts,
up.jsondoc_ ->'Data' ->> 'SalesToNetFixedAsts' SalesToNetFixedAsts,
up.jsondoc_ ->'Data' ->> 'ShortTermSecurities' ShortTermSecurities,
up.jsondoc_ ->'Data' ->> 'UCAChgInFixedAssets' UCAChgInFixedAssets,
up.jsondoc_ ->'Data' ->> 'CashinHandandAtBanks' CashinHandandAtBanks,
up.jsondoc_ ->'Data' ->> 'GoodwillAmortization' GoodwillAmortization,
up.jsondoc_ ->'Data' ->> 'ICFCashPdtoEmployees' ICFCashPdtoEmployees,
up.jsondoc_ ->'Data' ->> 'ICFCashPdtoSuppliers' ICFCashPdtoSuppliers,
up.jsondoc_ ->'Data' ->> 'ICFMinorityInterests' ICFMinorityInterests,
up.jsondoc_ ->'Data' ->> 'SpecItemswEquityChar' SpecItemswEquityChar,
up.jsondoc_ ->'Data' ->> 'ConcessLicenseTrdmrks' ConcessLicenseTrdmrks,
up.jsondoc_ ->'Data' ->> 'ICFCashRecfrmCustomers' ICFCashRecfrmCustomers,
up.jsondoc_ ->'Data' ->> 'SubscribedCapCallNotPd' SubscribedCapCallNotPd,
up.jsondoc_ ->'Data' ->> 'TradePayDaysExclCOSDepr' TradePayDaysExclCOSDepr,
up.jsondoc_ ->'Data' ->> 'ICFCashFlowFromOpActDirect' ICFCashFlowFromOpActDirect,
up.jsondoc_ ->'Data' ->> 'Sales' Sales,
up.jsondoc_ ->'Data' ->> 'Deposits' Deposits,
up.jsondoc_ ->'Data' ->> 'CashAndCashEquivalents' CashAndCashEquivalents,
up.jsondoc_ ->'Data' ->> 'CashSurplus' CashSurplus,
up.jsondoc_ ->'Data' ->> 'LIBOR' LIBOR,
up.jsondoc_ ->'Data' ->> 'MoneyMarketRate' MoneyMarketRate,
up.jsondoc_ ->'Data' ->> 'RequiredBorrowings' RequiredBorrowings,
(up.jsondoc_ ->'Data' ->> 'StatementDate')::varchar(10)::date StatementDate,
--up.jsondoc_ ->'Data' ->> 'HistStmtVersionId' HistStmtVersionId,
up.wfid_::varchar wfid_,
up.taskid_::varchar taskid_,
up.versionid_::integer versionid_, 
up.statusid_::integer statusid_,
up.isdeleted_::boolean isdeleted_,
up.islatestversion_::boolean islatestversion_,
up.isvisible_::boolean isvisible_,
up.isvalid_::boolean isvalid_,
up.snapshotid_::integer snapshotid_,
up.contextuserid_::varchar contextuserid_ ,
up.createdby_::varchar createdby_ , 
up.createddate_::timestamp createddate_ , 
up.updatedby_::varchar updatedby_ , 
up.updateddate_::timestamp updateddate_ , 
t_ t_ ,
(case when up.updateddate_>up.createddate_ then up.updatedby_ else up.createdby_ end) as sourcepopulatedby_,
GREATEST(up.updateddate_,up.createddate_) as sourcepopulateddate_
from madata.uphiststmtfinancials up
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSDETAIL')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abupprojstmtfinancialsdetail - part b end', clock_timestamp();

--abupprojstmtfinancialsdetail
raise notice '% - Step abupprojstmtfinancialsdetail_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abupprojstmtfinancialsdetail_idx;
DROP INDEX if exists olapts.abupprojstmtfinancialsdetail_idx2;

CREATE INDEX IF NOT EXISTS abupprojstmtfinancialsdetail_idx ON olapts.abupprojstmtfinancialsdetail (projstmtfinancialid_,wfid_);
CREATE INDEX IF NOT EXISTS abupprojstmtfinancialsdetail_idx2 ON olapts.abupprojstmtfinancialsdetail (pkid_,versionid_,wfid_);	

raise notice '% - Step abupprojstmtfinancialsdetail_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abupprojstmtfinancialsdetail - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abupprojstmtfinancialsdetailflag;
CREATE TABLE olapts.abupprojstmtfinancialsdetailflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.uphiststmtfinancials up
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSDETAIL';
delete from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSDETAIL';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPPROJSTMTFINANCIALSDETAIL' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSDETAILFLAG';
delete from olapts.refreshhistory where tablename = 'ABUPPROJSTMTFINANCIALSDETAILFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUPPROJSTMTFINANCIALSDETAILFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abupprojstmtfinancialsdetail - part c end', clock_timestamp();

raise notice '% - Step abupprojstmtfinancialsdetailflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abupprojstmtfinancialsdetailflag_idx;
DROP INDEX if exists olapts.abupprojstmtfinancialsdetailflag_idx2;

CREATE INDEX IF NOT EXISTS abupprojstmtfinancialsdetailflag_idx ON olapts.abupprojstmtfinancialsdetailflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abupprojstmtfinancialsdetailflag_idx2 ON olapts.abupprojstmtfinancialsdetailflag (pkid_,versionid_,wfid_);

raise notice '% - Step abupprojstmtfinancialsdetailflag_idx - part a end', clock_timestamp(); 

END $$;

-- End Financial Related Tables

-- Rating Related Tables

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABRATINGOVERRIDE') THEN
raise notice '% - Step abratingoverride - part a start', clock_timestamp();
insert into olapts.abratingoverride
select 
id_ dimratingoverrideid_,
pkid_,
jsondoc_->>'Comments' as "comments",
(jsondoc_->>'Id')::int as "Id",
(jsondoc_->>'IsLatest')::boolean islatest,
jsondoc_->>'ModelGrade' modelgrade,
jsondoc_->>'ModelId' modelid,
(jsondoc_->>'ModifiedDate')::timestamp modifieddate,
(jsondoc_->>'OrderIndex')::int orderindex,
jsondoc_->>'OverrideGrade' overridegrade,
(jsondoc_->>'OverridePd')::numeric overridepd,
jsondoc_->>'OverrideReason' overridereason,
(jsondoc_->>'RejectDate')::timestamp rejectdate,
(jsondoc_->>'Rejected')::boolean rejected,
jsondoc_->>'RejectUser' rejectuser,
jsondoc_->>'ScenarioId' scenarioid,
jsondoc_->>'User' as "user",
jsondoc_->>'OverrideAuthority' overrideauthority,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
fkid_ratingscenario,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.ratingoverride
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABRATINGOVERRIDE')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abratingoverride - part a end', clock_timestamp();
ELSE
raise notice '% - Step abratingoverride - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abratingoverride;
CREATE TABLE olapts.abratingoverride AS
select 
id_ dimratingoverrideid_,
pkid_,
jsondoc_->>'Comments' as "comments",
(jsondoc_->>'Id')::int as "Id",
(jsondoc_->>'IsLatest')::boolean islatest,
jsondoc_->>'ModelGrade' modelgrade,
jsondoc_->>'ModelId' modelid,
(jsondoc_->>'ModifiedDate')::timestamp modifieddate,
(jsondoc_->>'OrderIndex')::int orderindex,
jsondoc_->>'OverrideGrade' overridegrade,
(jsondoc_->>'OverridePd')::numeric overridepd,
jsondoc_->>'OverrideReason' overridereason,
(jsondoc_->>'RejectDate')::timestamp rejectdate,
(jsondoc_->>'Rejected')::boolean rejected,
jsondoc_->>'RejectUser' rejectuser,
jsondoc_->>'ScenarioId' scenarioid,
jsondoc_->>'User' as "user",
jsondoc_->>'OverrideAuthority' overrideauthority,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
fkid_ratingscenario,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.ratingoverride
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABRATINGOVERRIDE')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;
raise notice '% - Step abratingoverride - part b end', clock_timestamp();

--abratingoverride
raise notice '% - Step abratingoverride_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abratingoverride_idx;
DROP INDEX if exists olapts.abratingoverride_idx2;

CREATE INDEX IF NOT EXISTS abratingoverride_idx ON olapts.abratingoverride (dimratingoverrideid_,wfid_);
CREATE INDEX IF NOT EXISTS abratingoverride_idx2 ON olapts.abratingoverride (pkid_,versionid_,wfid_);	

raise notice '% - Step abratingoverride_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abratingoverride - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abratingoverrideflag;
CREATE TABLE olapts.abratingoverrideflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_ ::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.ratingoverride
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRATINGOVERRIDE';
delete from olapts.refreshhistory where tablename = 'ABRATINGOVERRIDE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRATINGOVERRIDE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRATINGOVERRIDEFLAG';
delete from olapts.refreshhistory where tablename = 'ABRATINGOVERRIDEFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRATINGOVERRIDEFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abratingoverride - part c end', clock_timestamp();

raise notice '% - Step abratingoverrideflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abratingoverrideflag_idx;
DROP INDEX if exists olapts.abratingoverrideflag_idx2;

CREATE INDEX IF NOT EXISTS abratingoverrideflag_idx ON olapts.abratingoverrideflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abratingoverrideflag_idx2 ON olapts.abratingoverrideflag (pkid_,versionid_,wfid_);

raise notice '% - Step abratingoverrideflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABENTITYRATING') THEN
raise notice '% - Step abentityrating - part a start', clock_timestamp();
insert into olapts.abentityrating
select
id_ factentityratingid_,
pkid_::varchar as pkid_,
(jsondoc_->>'ApprovalStatus') approvalstatus,
(jsondoc_->>'ApprovedDate')::timestamp approveddate,
(jsondoc_->>'ApprovedGrade') approvedgrade,
(jsondoc_->>'ApprovedPd')::numeric approvedpd,
(jsondoc_->>'ApproveId') approveid,
(jsondoc_->>'Approver') approver,
(jsondoc_->>'CascadeGrade') cascadegrade,
(jsondoc_->>'CascadeNote') cascadenote,
(jsondoc_->>'CascadePd')::numeric cascadepd,
(jsondoc_->>'CascadeReason') cascadereason,
(jsondoc_->>'CascadeUserId') cascadeuserid,
(jsondoc_->>'Comments') as "comments",
(jsondoc_->>'ConfigVersion')::int configversion,
(jsondoc_->>'DefaultComment') defaultcomment,
(jsondoc_->>'DefaultDate')::date defaultdate,
(jsondoc_->>'DefaultGrade') defaultgrade,
(jsondoc_->>'DefaultPd')::numeric defaultpd,
(jsondoc_->>'DefaultReason') defaultreason,
(jsondoc_->>'EntityId')::numeric entityid,
(jsondoc_->>'FinalGrade') finalgrade,
(jsondoc_->>'FinalPd')::numeric finalpd,
(case when jsondoc_->>'FinalScore'='Infinity' then 'NaN' else jsondoc_->>'FinalScore' end)::numeric finalscore,
(jsondoc_->>'Id')::numeric id,
(jsondoc_->>'IsApproved')::boolean isapproved,
(jsondoc_->>'IsDefault')::boolean isdefault,
(jsondoc_->>'IsOutOfDate')::boolean isoutofdate,
(jsondoc_->>'LatestApprovedScenarioId') latestapprovedscenarioid,
(jsondoc_->>'MasterApprovedGrade') masterapprovedgrade,
(jsondoc_->>'MasterApprovedPd')::numeric masterapprovedpd,
(jsondoc_->>'MasterCascadeGrade') mastercascadegrade,
(jsondoc_->>'MasterCascadePd')::numeric mastercascadepd,
(jsondoc_->>'MasterFinalGrade') masterfinalgrade,
(jsondoc_->>'MasterFinalPd')::numeric masterfinalpd,
(jsondoc_->>'MasterGrade') mastergrade,
(jsondoc_->>'MasterOverlayPd')::numeric masteroverlaypd,
(jsondoc_->>'MasterOverlayRating') masteroverlayrating,
(jsondoc_->>'MasterOverrideGrade') masteroverridegrade,
(jsondoc_->>'MasterOverridePd')::numeric masteroverridepd,
(jsondoc_->>'MasterPd')::numeric masterpd,
(jsondoc_->>'MasterSourceGrade') mastersourcegrade,
(jsondoc_->>'MasterSourcePd')::numeric mastersourcepd,
(jsondoc_->>'ModelGrade') modelgrade,
(jsondoc_->>'ModelPd')::numeric modelpd,
(jsondoc_->>'OutOfDateReason') outofdatereason,
(jsondoc_->>'OverlayPd')::numeric overlaypd,
(jsondoc_->>'OverlayRating') overlayrating,
(jsondoc_->>'OverrideGrade') overridegrade,
(jsondoc_->>'OverridePd')::numeric overridepd,
(jsondoc_->>'SourceEntityId')::numeric sourceentityid,
(jsondoc_->>'SourceEntityRatingVersionId')::int sourceentityratingversionid,
(jsondoc_->>'SourceGrade') sourcegrade,
(jsondoc_->>'SourceLongName') sourcelongname,
(jsondoc_->>'SourcePd')::numeric sourcepd,
(jsondoc_->>'TransientApprovedGrade') transientapprovedgrade,
(jsondoc_->>'TransientApprovedPd')::numeric transientapprovedpd,
(jsondoc_->>'TransientGrade') transientgrade,
(jsondoc_->>'TransientPd')::numeric transientpd,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
statusid_::int4,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.entityrating
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABENTITYRATING')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abentityrating - part a end', clock_timestamp();
ELSE
raise notice '% - Step abentityrating - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abentityrating;
CREATE TABLE olapts.abentityrating AS
select
id_ factentityratingid_,
pkid_::varchar as pkid_,
(jsondoc_->>'ApprovalStatus') approvalstatus,
(jsondoc_->>'ApprovedDate')::timestamp approveddate,
(jsondoc_->>'ApprovedGrade') approvedgrade,
(jsondoc_->>'ApprovedPd')::numeric approvedpd,
(jsondoc_->>'ApproveId') approveid,
(jsondoc_->>'Approver') approver,
(jsondoc_->>'CascadeGrade') cascadegrade,
(jsondoc_->>'CascadeNote') cascadenote,
(jsondoc_->>'CascadePd')::numeric cascadepd,
(jsondoc_->>'CascadeReason') cascadereason,
(jsondoc_->>'CascadeUserId') cascadeuserid,
(jsondoc_->>'Comments') as "comments",
(jsondoc_->>'ConfigVersion')::int configversion,
(jsondoc_->>'DefaultComment') defaultcomment,
(jsondoc_->>'DefaultDate')::date defaultdate,
(jsondoc_->>'DefaultGrade') defaultgrade,
(jsondoc_->>'DefaultPd')::numeric defaultpd,
(jsondoc_->>'DefaultReason') defaultreason,
(jsondoc_->>'EntityId')::numeric entityid,
(jsondoc_->>'FinalGrade') finalgrade,
(jsondoc_->>'FinalPd')::numeric finalpd,
(case when jsondoc_->>'FinalScore'='Infinity' then 'NaN' else jsondoc_->>'FinalScore' end)::numeric finalscore,
(jsondoc_->>'Id')::numeric id,
(jsondoc_->>'IsApproved')::boolean isapproved,
(jsondoc_->>'IsDefault')::boolean isdefault,
(jsondoc_->>'IsOutOfDate')::boolean isoutofdate,
(jsondoc_->>'LatestApprovedScenarioId') latestapprovedscenarioid,
(jsondoc_->>'MasterApprovedGrade') masterapprovedgrade,
(jsondoc_->>'MasterApprovedPd')::numeric masterapprovedpd,
(jsondoc_->>'MasterCascadeGrade') mastercascadegrade,
(jsondoc_->>'MasterCascadePd')::numeric mastercascadepd,
(jsondoc_->>'MasterFinalGrade') masterfinalgrade,
(jsondoc_->>'MasterFinalPd')::numeric masterfinalpd,
(jsondoc_->>'MasterGrade') mastergrade,
(jsondoc_->>'MasterOverlayPd')::numeric masteroverlaypd,
(jsondoc_->>'MasterOverlayRating') masteroverlayrating,
(jsondoc_->>'MasterOverrideGrade') masteroverridegrade,
(jsondoc_->>'MasterOverridePd')::numeric masteroverridepd,
(jsondoc_->>'MasterPd')::numeric masterpd,
(jsondoc_->>'MasterSourceGrade') mastersourcegrade,
(jsondoc_->>'MasterSourcePd')::numeric mastersourcepd,
(jsondoc_->>'ModelGrade') modelgrade,
(jsondoc_->>'ModelPd')::numeric modelpd,
(jsondoc_->>'OutOfDateReason') outofdatereason,
(jsondoc_->>'OverlayPd')::numeric overlaypd,
(jsondoc_->>'OverlayRating') overlayrating,
(jsondoc_->>'OverrideGrade') overridegrade,
(jsondoc_->>'OverridePd')::numeric overridepd,
(jsondoc_->>'SourceEntityId')::numeric sourceentityid,
(jsondoc_->>'SourceEntityRatingVersionId')::int sourceentityratingversionid,
(jsondoc_->>'SourceGrade') sourcegrade,
(jsondoc_->>'SourceLongName') sourcelongname,
(jsondoc_->>'SourcePd')::numeric sourcepd,
(jsondoc_->>'TransientApprovedGrade') transientapprovedgrade,
(jsondoc_->>'TransientApprovedPd')::numeric transientapprovedpd,
(jsondoc_->>'TransientGrade') transientgrade,
(jsondoc_->>'TransientPd')::numeric transientpd,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
statusid_::int4,
isdeleted_::boolean ,
islatestversion_::boolean ,
isvisible_::boolean ,
isvalid_::boolean ,
baseversionid_::int4 ,
snapshotid_::int4 ,
contextuserid_::varchar ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.entityrating
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABENTITYRATING')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abentityrating - part b end', clock_timestamp();

--abentityrating
raise notice '% - Step abentityrating_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abentityrating_idx;
DROP INDEX if exists olapts.abentityrating_idx2;

CREATE INDEX IF NOT EXISTS abentityrating_idx ON olapts.abentityrating (factentityratingid_,wfid_);
CREATE INDEX IF NOT EXISTS abentityrating_idx2 ON olapts.abentityrating (pkid_,versionid_,wfid);	

raise notice '% - Step abentityrating_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abentityrating - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abentityratingflag;
CREATE TABLE olapts.abentityratingflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.entityrating
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENTITYRATING';
delete from olapts.refreshhistory where tablename = 'ABENTITYRATING';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENTITYRATING' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENTITYRATINGFLAG';
delete from olapts.refreshhistory where tablename = 'ABENTITYRATINGFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENTITYRATINGFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abentityrating - part c end', clock_timestamp();

raise notice '% - Step abentityratingflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abentityratingflag_idx;
DROP INDEX if exists olapts.abentityratingflag_idx2;

CREATE INDEX IF NOT EXISTS abentityratingflag_idx ON olapts.abentityratingflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abentityratingflag_idx2 ON olapts.abentityratingflag (pkid_,versionid_,wfid_);

raise notice '% - Step abentityratingflag_idx - part a end', clock_timestamp(); 

END $$;

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABRATINGSCENARIO') THEN
raise notice '% - Step abratingscenario - part a start', clock_timestamp();
	insert into olapts.abratingscenario
	select 
		id_ factratingscenarioid_,
		pkid_,
		rs.jsondoc_ ->> 'Id' RatingScenarioId,
		rs.versionid_ versionid_,
		rs.jsondoc_ ->> 'EntityId' entityid,
		rs.jsondoc_ ->> 'FinancialContext' OriginalFinancialContext,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0:0#0;0#0:#0:0;0:0' 
			when jsondoc_->>'UsedFinancial' = 'False' then '0:0#0;0#0:#0:0;0:0'
			when jsondoc_->>'FinancialContext' = '' then '0:0#0;0#0:#0:0;0:0'
			when jsondoc_->>'FinancialContext' is null then '0:0#0;0#0:#0:0;0:0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0:0#0;0#0:#0:0;0:0'
			when jsondoc_->>'FinancialContext' = '0' then '0:0#0;0#0:#0:0;0:0'
			else jsondoc_->>'FinancialContext'
		end as FinancialContext,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0'
			when jsondoc_->>'FinancialContext' = '' then '0'
			when jsondoc_->>'FinancialContext' is null then '0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0'
			when jsondoc_->>'FinancialContext' = '0' then '0'
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',1),':',1)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',1),':',1) end)
		end as FinancialId,
		case 
			when jsondoc_->>'FinancialContext' = '###' then 0
			when jsondoc_->>'UsedFinancial' = 'False' then 0
			when jsondoc_->>'FinancialContext' = '' then 0
			when jsondoc_->>'FinancialContext' is null then 0
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then 0
			when jsondoc_->>'FinancialContext' = '0' then 0
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',1),':',2)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',1),':',2) end)::int
		end as FinancialVersionId,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0'
			when jsondoc_->>'FinancialContext' = '' then '0'
			when jsondoc_->>'FinancialContext' is null then '0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0'
			when jsondoc_->>'FinancialContext' = '0' then '0'
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',2),';',1)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',2),';',1) end)
		end as peeranalysis_version_match,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0'
			when jsondoc_->>'FinancialContext' = '' then '0'
			when jsondoc_->>'FinancialContext' is null then '0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0'
			when jsondoc_->>'FinancialContext' = '0' then '0'
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',2),';',2)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',2),';',2) end)
		end as entity_version_match,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0'
			when jsondoc_->>'FinancialContext' = '' then '0'
			when jsondoc_->>'FinancialContext' is null then '0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0'
			when jsondoc_->>'FinancialContext' = '0' then '0'
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',3),':',1)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',3),':',1) end)
		end as projection_version_match,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0:0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0:0'
			when jsondoc_->>'FinancialContext' = '' then '0:0'
			when jsondoc_->>'FinancialContext' is null then '0:0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0:0'
			when jsondoc_->>'FinancialContext' = '0' then '0:0'
			else coalesce(NULLIF(SPLIT_PART(jsondoc_->>'FinancialContext','#',4),''),'0:0') 
		end as stmts_versions_,
		rs.jsondoc_ ->> 'Name' "name",
		rs.jsondoc_ ->> 'ScenarioTypeRef' ScenarioTypeRef,
		rs.jsondoc_ ->> 'AmUser' AmUser,
		rs.jsondoc_ ->> 'RatingStatus' RatingStatus,
		(rs.jsondoc_ ->> 'NextReviewDate')::varchar(10)::date NextReviewDate,
		(rs.jsondoc_ ->> 'Creditcommitteedate')::varchar(10)::date Creditcommitteedate,
		rs.jsondoc_ ->> 'ModelId' ModelId,
		(rs.jsondoc_ ->> 'IsPrimary')::boolean IsPrimary,
		rs.jsondoc_ ->> 'FinalGrade' FinalGrade,
		(case when rs.jsondoc_->>'FinalScore' ilike '%infinity%' then 'NaN' else rs.jsondoc_->>'FinalScore' end)::numeric as FinalScore,
		rs.jsondoc_ ->> 'ModelGrade' ModelGrade,
		rs.jsondoc_ ->> 'ModelPd' ModelPd,
		nullif((rs.jsondoc_->> 'ApprovedDate'),'')::timestamp  as ApprovedDate,
		rs.jsondoc_ ->> 'MasterGrade' MasterGrade,
		rs.jsondoc_ ->> 'StatementCount' StatementCount,
		rs.jsondoc_ ->> 'LatestStatementId' LatestStatementId,
		rs.jsondoc_ ->> 'OverrideGrade' OverrideGrade,
		rs.jsondoc_ ->> 'OverridePd' OverridePd,
		rs.jsondoc_ ->> 'ApproveId' ApproveId,
		rs.jsondoc_ ->> 'Approver' Approver,
		(rs.jsondoc_ ->> 'IsLatestApprovedScenario')::boolean IsLatestApprovedScenario,
		rs.jsondoc_ ->> 'ApprovalStatus' ApprovalStatus,
		rs.jsondoc_ ->> 'ConfigVersion' ConfigVersion,
		(rs.jsondoc_ ->> 'IsApproved')::boolean IsApproved,
		rs.jsondoc_ ->> 'ModelInputsChanged' ModelInputsChanged,
		rs.jsondoc_ ->> 'MasterOverrideGrade' MasterOverrideGrade,
		rs.jsondoc_ ->> 'MasterOverridePd' MasterOverridePd,
		rs.jsondoc_ ->> 'SelectedFinancialId' SelectedFinancialId,
		rs.jsondoc_ ->> 'MasterPd' MasterPd,
		rs.jsondoc_ ->> 'ModelVersion' ModelVersion,
		rs.jsondoc_ ->> 'OverlayPd' OverlayPd,
		rs.jsondoc_ ->> 'OverlayRating' OverlayRating,
		rs.jsondoc_ ->> 'ProjectionId' ProjectionId,
		rs.jsondoc_ ->> 'ProposedRating' ProposedRating,
		(rs.jsondoc_ ->> 'UsedFinancial')::boolean UsedFinancial,
		rs.jsondoc_->>'ParentId' parentid,
		rs.jsondoc_->>'ParentName' parentname,
		(rs.jsondoc_->>'ExtendedReviewDate')::date as extendedreviewdate,
		wfid_,
		taskid_,
		snapshotid_,
		contextuserid_,
		createdby_,
		createddate_,
		updatedby_,
		updateddate_,
		isvalid_::boolean,
		isdeleted_::boolean,
		isvisible_::boolean,
		islatestversion_::boolean,
		t_ t_ ,
		baseversionid_,
		statusid_,
		(case when rs.updateddate_>rs.createddate_ then rs.updatedby_ else rs.createdby_ end) as sourcepopulatedby_,
		GREATEST(rs.updateddate_,rs.createddate_) as sourcepopulateddate_
		FROM madata.ratingscenario rs
		
		where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABRATINGSCENARIO')
		and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
	;
	
raise notice '% - Step abratingscenario - part a end', clock_timestamp();
ELSE
raise notice '% - Step abratingscenario - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abratingscenario;
	CREATE TABLE olapts.abratingscenario AS
	select 
		id_ factratingscenarioid_,
		pkid_,
		rs.jsondoc_ ->> 'Id' RatingScenarioId,
		rs.versionid_ versionid_,
		rs.jsondoc_ ->> 'EntityId' entityid,
		rs.jsondoc_ ->> 'FinancialContext' OriginalFinancialContext,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0:0#0;0#0:#0:0;0:0' 
			when jsondoc_->>'UsedFinancial' = 'False' then '0:0#0;0#0:#0:0;0:0'
			when jsondoc_->>'FinancialContext' = '' then '0:0#0;0#0:#0:0;0:0'
			when jsondoc_->>'FinancialContext' is null then '0:0#0;0#0:#0:0;0:0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0:0#0;0#0:#0:0;0:0'
			when jsondoc_->>'FinancialContext' = '0' then '0:0#0;0#0:#0:0;0:0'
			else jsondoc_->>'FinancialContext'
		end as FinancialContext,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0'
			when jsondoc_->>'FinancialContext' = '' then '0'
			when jsondoc_->>'FinancialContext' is null then '0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0'
			when jsondoc_->>'FinancialContext' = '0' then '0'
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',1),':',1)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',1),':',1) end)
		end as FinancialId,
		case 
			when jsondoc_->>'FinancialContext' = '###' then 0
			when jsondoc_->>'UsedFinancial' = 'False' then 0
			when jsondoc_->>'FinancialContext' = '' then 0
			when jsondoc_->>'FinancialContext' is null then 0
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then 0
			when jsondoc_->>'FinancialContext' = '0' then 0
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',1),':',2)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',1),':',2) end)::int
		end as FinancialVersionId,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0'
			when jsondoc_->>'FinancialContext' = '' then '0'
			when jsondoc_->>'FinancialContext' is null then '0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0'
			when jsondoc_->>'FinancialContext' = '0' then '0'
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',2),';',1)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',2),';',1) end)
		end as peeranalysis_version_match,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0'
			when jsondoc_->>'FinancialContext' = '' then '0'
			when jsondoc_->>'FinancialContext' is null then '0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0'
			when jsondoc_->>'FinancialContext' = '0' then '0'
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',2),';',2)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',2),';',2) end)
		end as entity_version_match,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0'
			when jsondoc_->>'FinancialContext' = '' then '0'
			when jsondoc_->>'FinancialContext' is null then '0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0'
			when jsondoc_->>'FinancialContext' = '0' then '0'
			else (case when SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',3),':',1)='' then '0' else SPLIT_PART(SPLIT_PART(jsondoc_->>'FinancialContext','#',3),':',1) end)
		end as projection_version_match,
		case 
			when jsondoc_->>'FinancialContext' = '###' then '0:0'
			when jsondoc_->>'UsedFinancial' = 'False' then '0:0'
			when jsondoc_->>'FinancialContext' = '' then '0:0'
			when jsondoc_->>'FinancialContext' is null then '0:0'
			when jsondoc_->>'FinancialContext' ~ '^-?\d+\.?\d+$' = 'true' then '0:0'
			when jsondoc_->>'FinancialContext' = '0' then '0:0'
			else coalesce(NULLIF(SPLIT_PART(jsondoc_->>'FinancialContext','#',4),''),'0:0') 
		end as stmts_versions_,
		rs.jsondoc_ ->> 'Name' "name",
		rs.jsondoc_ ->> 'ScenarioTypeRef' ScenarioTypeRef,
		rs.jsondoc_ ->> 'AmUser' AmUser,
		rs.jsondoc_ ->> 'RatingStatus' RatingStatus,
		(rs.jsondoc_ ->> 'NextReviewDate')::varchar(10)::date NextReviewDate,
		(rs.jsondoc_ ->> 'Creditcommitteedate')::varchar(10)::date Creditcommitteedate,
		rs.jsondoc_ ->> 'ModelId' ModelId,
		(rs.jsondoc_ ->> 'IsPrimary')::boolean IsPrimary,
		rs.jsondoc_ ->> 'FinalGrade' FinalGrade,
		(case when rs.jsondoc_->>'FinalScore' ilike '%infinity%' then 'NaN' else rs.jsondoc_->>'FinalScore' end)::numeric as FinalScore,
		rs.jsondoc_ ->> 'ModelGrade' ModelGrade,
		rs.jsondoc_ ->> 'ModelPd' ModelPd,
		nullif((rs.jsondoc_->> 'ApprovedDate'),'')::timestamp  as ApprovedDate,
		rs.jsondoc_ ->> 'MasterGrade' MasterGrade,
		rs.jsondoc_ ->> 'StatementCount' StatementCount,
		rs.jsondoc_ ->> 'LatestStatementId' LatestStatementId,
		rs.jsondoc_ ->> 'OverrideGrade' OverrideGrade,
		rs.jsondoc_ ->> 'OverridePd' OverridePd,
		rs.jsondoc_ ->> 'ApproveId' ApproveId,
		rs.jsondoc_ ->> 'Approver' Approver,
		(rs.jsondoc_ ->> 'IsLatestApprovedScenario')::boolean IsLatestApprovedScenario,
		rs.jsondoc_ ->> 'ApprovalStatus' ApprovalStatus,
		rs.jsondoc_ ->> 'ConfigVersion' ConfigVersion,
		(rs.jsondoc_ ->> 'IsApproved')::boolean IsApproved,
		rs.jsondoc_ ->> 'ModelInputsChanged' ModelInputsChanged,
		rs.jsondoc_ ->> 'MasterOverrideGrade' MasterOverrideGrade,
		rs.jsondoc_ ->> 'MasterOverridePd' MasterOverridePd,
		rs.jsondoc_ ->> 'SelectedFinancialId' SelectedFinancialId,
		rs.jsondoc_ ->> 'MasterPd' MasterPd,
		rs.jsondoc_ ->> 'ModelVersion' ModelVersion,
		rs.jsondoc_ ->> 'OverlayPd' OverlayPd,
		rs.jsondoc_ ->> 'OverlayRating' OverlayRating,
		rs.jsondoc_ ->> 'ProjectionId' ProjectionId,
		rs.jsondoc_ ->> 'ProposedRating' ProposedRating,
		(rs.jsondoc_ ->> 'UsedFinancial')::boolean UsedFinancial,
		rs.jsondoc_->>'ParentId' parentid,
		rs.jsondoc_->>'ParentName' parentname,
		(rs.jsondoc_->>'ExtendedReviewDate')::timestamp as extendedreviewdate,
		wfid_,
		taskid_,
		snapshotid_,
		contextuserid_,
		createdby_,
		createddate_,
		updatedby_,
		updateddate_,
		isvalid_::boolean,
		isdeleted_::boolean,
		isvisible_::boolean,
		islatestversion_::boolean,
		t_ t_ ,
		baseversionid_,
		statusid_,
		(case when rs.updateddate_>rs.createddate_ then rs.updatedby_ else rs.createdby_ end) as sourcepopulatedby_,
		GREATEST(rs.updateddate_,rs.createddate_) as sourcepopulateddate_
		FROM madata.ratingscenario rs
		where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABRATINGSCENARIO')
		and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
	;
	raise notice '% - Step abratingscenario - part b end', clock_timestamp();
	
	--abratingscenario
	raise notice '% - Step abratingscenario_idx - part a start', clock_timestamp(); 

	DROP INDEX IF EXISTS olapts.abratingscenario_idx;
	DROP INDEX IF EXISTS olapts.abratingscenario_idx2;
	DROP INDEX IF EXISTS olapts.abratingscenario_idx3;
	DROP INDEX IF EXISTS olapts.abratingscenario_idx4;
	DROP INDEX IF EXISTS olapts.abratingscenario_idx5;

	
	CREATE INDEX IF NOT EXISTS abratingscenario_idx_date_brin ON olapts.abratingscenario USING BRIN (sourcepopulateddate_);
	CREATE INDEX IF NOT EXISTS abratingscenario_idx_pkid_hash ON olapts.abratingscenario USING hash (pkid_);
	CREATE INDEX IF NOT EXISTS abratingscenario_idx_pkid_btree ON olapts.abratingscenario (pkid_,wfid_);
	CREATE INDEX IF NOT EXISTS abratingscenario_idx_btree ON olapts.abratingscenario (pkid_,versionid_,sourcepopulateddate_) include(entityid,entity_version_match,financialid,financialversionid,financialcontext,stmts_versions_,isprimary,islatestapprovedscenario,approvalstatus,parentid,isvalid_,isdeleted_,isvisible_,islatestversion_,wfid_);
	CREATE INDEX IF NOT EXISTS abratingscenario_idx_pkid_v ON olapts.abratingscenario USING btree (pkid_,versionid_,wfid_);

	CREATE STATISTICS if not exists abratingscenario_stat ON pkid_,entityid,sourcepopulateddate_,wfid_ FROM olapts.abratingscenario;

	raise notice '% - Step abratingscenario_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abratingscenario - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abratingscenarioflag;
CREATE TABLE olapts.abratingscenarioflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
(jsondoc_ ->> 'IsLatestApprovedScenario')::boolean IsLatestApprovedScenario,
(jsondoc_ ->> 'ApprovalStatus')::int ApprovalStatus,
(jsondoc_ ->> 'IsPrimary')::boolean IsPrimary,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.ratingscenario
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRATINGSCENARIO';
delete from olapts.refreshhistory where tablename = 'ABRATINGSCENARIO';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRATINGSCENARIO' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOFLAG';
delete from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRATINGSCENARIOFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abratingscenario - part c end', clock_timestamp();

END$$;

--mapinstance feed
DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABMAPINSTANCE') THEN
raise notice '% - Step abmapinstance - part a start', clock_timestamp();
insert into olapts.abmapinstance
SELECT 
id_ factmappdinstanceid_,
pkid_::varchar as pkid_,
(jsondoc_->>'Alerts') alerts,
(jsondoc_->>'Errors') errors,
(jsondoc_->>'Grade') grade,
(jsondoc_->>'Pd')::numeric pd,
(jsondoc_->>'Score')::numeric score,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
fkid_entity,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.mapinstance
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABMAPINSTANCE')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

raise notice '% - Step abmapinstance - part a end', clock_timestamp();
ELSE
raise notice '% - Step abmapinstance - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmapinstance;
CREATE TABLE olapts.abmapinstance AS
SELECT 
id_ factmappdinstanceid_,
pkid_::varchar as pkid_,
(jsondoc_->>'Alerts') alerts,
(jsondoc_->>'Errors') errors,
(jsondoc_->>'Grade') grade,
(jsondoc_->>'Pd')::numeric pd,
(jsondoc_->>'Score')::numeric score,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
fkid_entity,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.mapinstance
where GREATEST(updateddate_,createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABMAPINSTANCE')
and GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;
raise notice '% - Step abmapinstance - part b end', clock_timestamp();

--abmapinstance
raise notice '% - Step abmapinstance_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abmapinstance_idx;
DROP INDEX if exists olapts.abmapinstance_idx2;

CREATE INDEX IF NOT EXISTS abmapinstance_idx ON olapts.abmapinstance (factmappdinstanceid_,wfid_);
CREATE INDEX IF NOT EXISTS abmapinstance_idx2 ON olapts.abmapinstance (pkid_,versionid_,wfid_);
	

raise notice '% - Step abmapinstance_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abmapinstance - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmapinstanceflag;
CREATE TABLE olapts.abmapinstanceflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.mapinstance
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMAPINSTANCE';
delete from olapts.refreshhistory where tablename = 'ABMAPINSTANCE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMAPINSTANCE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMAPINSTANCEFLAG';
delete from olapts.refreshhistory where tablename = 'ABMAPINSTANCEFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMAPINSTANCEFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmapinstance - part c end', clock_timestamp();

raise notice '% - Step abmapinstanceflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abmapinstanceflag_idx;
DROP INDEX if exists olapts.abmapinstanceflag_idx2;

CREATE INDEX IF NOT EXISTS abmapinstanceflag_idx ON olapts.abmapinstanceflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abmapinstanceflag_idx2 ON olapts.abmapinstanceflag (pkid_,versionid_,wfid_);

raise notice '% - Step abmapinstanceflag_idx - part a end', clock_timestamp(); 

END $$;

-- End Rating Related Tables

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

-- Lookup Tables
--abaccount
raise notice '% - Step abaccount - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abaccount;
CREATE TABLE olapts.abaccount AS
  select 
   lm.name||':'||la.languageid::varchar||':'||l.modelid::varchar||':'||l.accountid::varchar as dimaccountid_,   
   la.languageid::varchar||':'||l.modelid::varchar||':'||l.accountid::varchar as pkid_,
   la.languageid
   ,l.modelid
   ,l.accountid
   ,lm.name financialtemplatekey_ 
   ,la.description
   ,l.label
   ,l.constantlabel 
   ,current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.languageaccounts l 
join madata.languages la 
on l.languageid=la.languageid
join (select distinct name,modelid FROM madata.languagemodels ) lm
on l.modelid=lm.modelid
;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABACCOUNT';
delete from olapts.refreshhistory where tablename = 'ABACCOUNT';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABACCOUNT' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abaccount - part a end', clock_timestamp();

--ApprovalStatus

raise notice '% - Step abapprovalstatus - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abapprovalstatus;
CREATE TABLE olapts.abapprovalstatus AS
select l.jsondoc_->>'Id' approvalstatuskey_,
l.jsondoc_->>'Label' approvalstatusvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ApprovalStatus';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABAPPROVALSTATUS';
delete from olapts.refreshhistory where tablename = 'ABAPPROVALSTATUS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABAPPROVALSTATUS' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abapprovalstatus - part a end', clock_timestamp();

-- BusinessPortfolio

raise notice '% - Step abbusinessportfolio - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abbusinessportfolio;
CREATE TABLE olapts.abbusinessportfolio AS
select l.jsondoc_->>'Key' businessportfoliokey_,
l.jsondoc_->>'Value' businessportfoliovalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'BusinessPortfolio';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABBUSINESSPORTFOLIO';
delete from olapts.refreshhistory where tablename = 'ABBUSINESSPORTFOLIO';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABBUSINESSPORTFOLIO' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abbusinessportfolio - part a end', clock_timestamp();

--Country
raise notice '% - Step abcountry - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcountry;
CREATE TABLE olapts.abcountry AS
select l.jsondoc_->>'Key' countrykey_,
l.jsondoc_->>'Value' countryvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Country';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCOUNTRY';
delete from olapts.refreshhistory where tablename = 'ABCOUNTRY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCOUNTRY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcountry - part a end', clock_timestamp();

--Currency
raise notice '% - Step abcurrency - part a start', clock_timestamp();

DROP TABLE IF EXISTS olapts.abcurrency;
CREATE TABLE olapts.abcurrency AS
select l.jsondoc_->>'Key' currencykey_,
l.jsondoc_->>'Value' currencyvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Currency';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCURRENCY';
delete from olapts.refreshhistory where tablename = 'ABCURRENCY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCURRENCY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcurrency - part a end', clock_timestamp();

--Division
raise notice '% - Step abdivision - part a start', clock_timestamp();

DROP TABLE IF EXISTS olapts.abdivision;
CREATE TABLE olapts.abdivision AS
select l.jsondoc_->>'Key' divisionkey_,
l.jsondoc_->>'Value' divisionvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Division';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABDIVISION';
delete from olapts.refreshhistory where tablename = 'ABDIVISION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABDIVISION' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abdivision - part a end', clock_timestamp();

--ElMasterScale
raise notice '% - Step abelmasterscale - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abelmasterscale;
CREATE TABLE olapts.abelmasterscale AS
select l.jsondoc_->>'Id' elmasterscalekey_,
l.jsondoc_->>'Grade' elmasterscalevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ElMasterScale';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABELMASTERSCALE';
delete from olapts.refreshhistory where tablename = 'ABELMASTERSCALE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABELMASTERSCALE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abelmasterscale - part a end', clock_timestamp();

--ElModelScale
raise notice '% - Step abelmodelscale - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abelmodelscale;
CREATE TABLE olapts.abelmodelscale AS
select l.jsondoc_->>'Id' elmasterscalekey_,
l.jsondoc_->>'Grade' elmasterscalevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ElModelScale';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABELMODELSCALE';
delete from olapts.refreshhistory where tablename = 'ABELMODELSCALE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABELMODELSCALE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abelmodelscale - part a end', clock_timestamp();

--EntityType
raise notice '% - Step abentitytype - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abentitytype;
CREATE TABLE olapts.abentitytype AS
select l.jsondoc_->>'Key' entitytypekey_,
l.jsondoc_->>'Value' entitytypevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EntityType';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENTITYTYPE';
delete from olapts.refreshhistory where tablename = 'ABENTITYTYPE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENTITYTYPE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abentitytype - part a end', clock_timestamp();

--FinancialStatementType
raise notice '% - Step abfinancialstatementtype - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abfinancialstatementtype;
CREATE TABLE olapts.abfinancialstatementtype AS
select l.jsondoc_->>'Key' entitytypekey_,
l.jsondoc_->>'Value' entitytypevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'FinancialStatementType';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFINANCIALSTATEMENTTYPE';
delete from olapts.refreshhistory where tablename = 'ABFINANCIALSTATEMENTTYPE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFINANCIALSTATEMENTTYPE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abfinancialstatementtype - part a end', clock_timestamp();

--FirmType
raise notice '% - Step abfirmtype - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abfirmtype;
CREATE TABLE olapts.abfirmtype AS
select l.jsondoc_->>'Key' firmtypekey_,
l.jsondoc_->>'Value' firmtypevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'FirmType';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFIRMTYPE';
delete from olapts.refreshhistory where tablename = 'ABFIRMTYPE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFIRMTYPE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abfirmtype - part a end', clock_timestamp();

--GiftIndustrySector
raise notice '% - Step abgiftindustrysector - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abgiftindustrysector ;
CREATE TABLE olapts.abgiftindustrysector AS
select l.jsondoc_->>'Id' giftindustrysectorkey_,
l.jsondoc_->>'Display' giftindustrysectorvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'GiftIndustrySector';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABGIFTINDUSTRYSECTOR';
delete from olapts.refreshhistory where tablename = 'ABGIFTINDUSTRYSECTOR';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABGIFTINDUSTRYSECTOR' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abgiftindustrysector - part a end', clock_timestamp();

--IftIndustrySector
raise notice '% - Step abiftindustrysector - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abiftindustrysector ;
CREATE TABLE olapts.abiftindustrysector AS
select l.jsondoc_->>'Id' iftindustrysectorkey_,
l.jsondoc_->>'Value' iftindustrysectorvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'IftIndustrySector';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABIFTINDUSTRYSECTOR';
delete from olapts.refreshhistory where tablename = 'ABIFTINDUSTRYSECTOR';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABIFTINDUSTRYSECTOR' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abiftindustrysector - part a end', clock_timestamp();

--IndClassification
raise notice '% - Step abindclassification - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abindclassification;
CREATE TABLE olapts.abindclassification AS
select l.jsondoc_->>'Key' firmtypekey_,
l.jsondoc_->>'Value' firmtypevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'IndClassification';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABINDCLASSIFICATION';
delete from olapts.refreshhistory where tablename = 'ABINDCLASSIFICATION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABINDCLASSIFICATION' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abindclassification - part a end', clock_timestamp();

-- abindustry
raise notice '% - Step abindustry - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abindustry;
CREATE TABLE olapts.abindustry AS
with indtemp as
(
    SELECT 
        jsondoc_ ->> 'HierarchyKey' as HierarchyKey, jsondoc_ ->> 'Key' as indkey, (jsondoc_->>'CreditLensStandardIndustry')::varchar as creditlensstandardindustrykey_
        ,greatest(updateddate_,createddate_) as sourcepopulateddate_, 
		(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
		string_to_array(jsondoc_ ->> 'HierarchyKey','|') AS indkeytemp, array_length(string_to_array(jsondoc_ ->> 'HierarchyKey','|'),1) as level
        ,2 pri,isdeleted_,jsondoc_->>'Value' as indval,t_,jsondoc_ ->> 'Active' as isactive
        FROM madata.standard_lookup 
        where t_ in ('SicIndustry','NaicsIndustry','NaceIndustry')
        union
        SELECT
        jsondoc_ ->> 'HierarchyKey' as HierarchyKey, jsondoc_ ->> 'Key' as indkey, (jsondoc_->>'CreditLensStandardIndustry')::varchar as creditlensstandardindustrykey_
        ,greatest(updateddate_,createddate_) as sourcepopulateddate_, 
		(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
		string_to_array(jsondoc_ ->> 'HierarchyKey','|') AS indkeytemp, array_length(string_to_array(jsondoc_ ->> 'HierarchyKey','|'),1) as level
        ,1 pri,isdeleted_,jsondoc_->>'Value' as indval,t_,jsondoc_ ->> 'Active' as isactive
        FROM madata.custom_lookup 
        where t_ in ('SicIndustry','NaicsIndustry','NaceIndustry')
)
select 
case when lv1.level = 1 then ''
when lv1.level = 2 then lv1.indkeytemp[2] 
when lv1.level = 3 then lv1.indkeytemp[2] || '|' || lv1.indkeytemp[3] 
when lv1.level = 4 then lv1.indkeytemp[2] || '|' || lv1.indkeytemp[3] || '|' || lv1.indkeytemp[4] 
when lv1.level = 5 then lv1.indkeytemp[2] || '|' || lv1.indkeytemp[3] || '|' || lv1.indkeytemp[4] || '|' || lv1.indkeytemp[5] 
end as industrycodekey_,
lv1.indkey as levelkey,
case when lv1.t_ = 'NaicsIndustry' then 'NAICS' when lv1.t_ = 'SicIndustry' then 'SIC' else lv1.t_ end as level0,
lv1.indkeytemp[1] || '-' || lv1.indkeytemp[1] as level1,
lv1.indkeytemp[2] || '-' || lv2.indval as level2,
lv1.indkeytemp[3] || '-' || lv3.indval as level3,
lv1.indkeytemp[4] || '-' || lv4.indval as level4,
lv1.indkeytemp[5] || '-' || lv5.indval as level5,
null as level6,
null as level7,
null as level8,
null as level9,
null as level10,
null as level11,
null as level12,
null as level13,
null as level14,
null as level15,
lv1.t_ t_ ,
lv1.sourcepopulateddate_ as  sourcepopulateddate_,
lv1.sourcepopulatedby_ as  sourcepopulatedby_,
lv2.creditlensstandardindustrykey_,
lv1.isdeleted_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from indtemp lv1
left join indtemp lv2 on (lv1.indkeytemp[1] = lv2.indkeytemp[1] and lv1.indkeytemp[2] = lv2.indkey and lv2.level = 2)
left join indtemp lv3 on (lv1.indkeytemp[1] = lv3.indkeytemp[1] and lv1.indkeytemp[2] = lv3.indkeytemp[2] and lv1.indkeytemp[3] = lv3.indkey and lv3.level = 3)
left join indtemp lv4 on (lv1.indkeytemp[1] = lv4.indkeytemp[1] and lv1.indkeytemp[2] = lv4.indkeytemp[2] and lv1.indkeytemp[3] = lv4.indkeytemp[3] and lv1.indkeytemp[4] = lv4.indkey and lv4.level = 4)
left join indtemp lv5 on (lv1.indkeytemp[1] = lv5.indkeytemp[1] and lv1.indkeytemp[2] = lv5.indkeytemp[2] and lv1.indkeytemp[3] = lv5.indkeytemp[3] and lv1.indkeytemp[4] = lv5.indkeytemp[4] and lv4.level = 4 and lv1.indkeytemp[5] = lv5.indkey and lv5.level = 5)
where lv1.level > 1
order by industrycodekey_, level0, level1,level2;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABINDUSTRY';
delete from olapts.refreshhistory where tablename = 'ABINDUSTRY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABINDUSTRY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abindustry - part a end', clock_timestamp();

--languages
raise notice '% - Step ablanguages - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ablanguages;
CREATE TABLE olapts.ablanguages AS
select 
languageid,
description,
culture,
specificculture,
data
from madata.languages;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLANGUAGES';
delete from olapts.refreshhistory where tablename = 'ABLANGUAGES';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLANGUAGES' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step ablanguages - part a end', clock_timestamp();

-- languagestmtconsts
raise notice '% - Step ablanguagestmtconsts - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ablanguagestmtconsts;
CREATE TABLE olapts.ablanguagestmtconsts AS
select 
languageid,
modelid,
statementconstantid,
label,
attributes,
constantlabel
from madata.languagestmtconsts;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLANGUAGESTMTCONSTS';
delete from olapts.refreshhistory where tablename = 'ABLANGUAGESTMTCONSTS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLANGUAGESTMTCONSTS' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step ablanguagestmtconsts - part a end', clock_timestamp();

-- MapCrePdOverrideSetting
raise notice '% - Step abmapcrepdoverridesetting - part a start', clock_timestamp();

DROP TABLE IF EXISTS olapts.abmapcrepdoverridesetting;
CREATE TABLE olapts.abmapcrepdoverridesetting AS
select 
jsondoc_ ->> 'Id' key_, 
jsondoc_ ->> 'Reason' display_,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MapCrePdOverrideSetting';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMAPCREPDOVERRIDESETTING';
delete from olapts.refreshhistory where tablename = 'ABMAPCREPDOVERRIDESETTING';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMAPCREPDOVERRIDESETTING' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmapcrepdoverridesetting - part a end', clock_timestamp();

--MapModel
raise notice '% - Step abmapmodel - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmapmodel;
CREATE TABLE olapts.abmapmodel AS
select l.jsondoc_->>'Id' mapmodelkey_,
l.jsondoc_->>'Name' mapmodelvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MapModel';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMAPMODEL';
delete from olapts.refreshhistory where tablename = 'ABMAPMODEL';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMAPMODEL' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmapmodel - part a end', clock_timestamp();

--MapPdModelScale
raise notice '% - Step abmappdmodelscale - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmappdmodelscale;
CREATE TABLE olapts.abmappdmodelscale AS
select l.jsondoc_->>'Id' mappdmodelscalekey_,
l.jsondoc_->>'Grade' mappdmodelscalevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MapPdModelScale';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMAPPDMODELSCALE';
delete from olapts.refreshhistory where tablename = 'ABMAPPDMODELSCALE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMAPPDMODELSCALE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmappdmodelscale - part a end', clock_timestamp();

--MapPdOverrideSetting
raise notice '% - Step abmappdoverridesetting - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmappdoverridesetting;
CREATE TABLE olapts.abmappdoverridesetting AS
select l.jsondoc_->>'Id' mappdoverridesettingkey_,
l.jsondoc_->>'Reason' mappdoverridesettingvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MapPdOverrideSetting';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMAPPDOVERRIDESETTING';
delete from olapts.refreshhistory where tablename = 'ABMAPPDOVERRIDESETTING';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMAPPDOVERRIDESETTING' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmappdoverridesetting - part a end', clock_timestamp();

--MasterScale
raise notice '% - Step abmasterscale - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmasterscale;
CREATE TABLE olapts.abmasterscale AS
select l.jsondoc_->>'Id' masterscalekey_,
l.jsondoc_->>'Grade' masterscalevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MasterScale';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMASTERSCALE';
delete from olapts.refreshhistory where tablename = 'ABMASTERSCALE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMASTERSCALE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmasterscale - part a end', clock_timestamp();

--OnList
raise notice '% - Step abonlist - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abonlist;
CREATE TABLE olapts.abonlist AS
select l.jsondoc_->>'Key' onlistkey_,
l.jsondoc_->>'Value' onlistvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'OnList';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABONLIST';
delete from olapts.refreshhistory where tablename = 'ABONLIST';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABONLIST' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abonlist - part a end', clock_timestamp();


--ProbabilityOfDefault
raise notice '% - Step abprobabilityofdefault - part a start', clock_timestamp();

DROP TABLE IF EXISTS olapts.abprobabilityofdefault;
CREATE TABLE olapts.abprobabilityofdefault AS
select l.jsondoc_->>'Id' probabilityofdefaultkey_,
l.jsondoc_->>'Range' probabilityofdefaultvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ProbabilityOfDefault';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPROBABILITYOFDEFAULT';
delete from olapts.refreshhistory where tablename = 'ABPROBABILITYOFDEFAULT';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPROBABILITYOFDEFAULT' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abprobabilityofdefault - part a end', clock_timestamp();


--RatingScenarioType
raise notice '% - Step abratingscenariotype - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abratingscenariotype;
CREATE TABLE olapts.abratingscenariotype AS
select l.jsondoc_->>'Key' ratingscenariotypekey_,
l.jsondoc_->>'Name' ratingscenariotypevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'RatingScenarioType';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOTYPE';
delete from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOTYPE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRATINGSCENARIOTYPE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abratingscenariotype - part a end', clock_timestamp();

--ReportFrequencyType
raise notice '% - Step abreportfrequencytype - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abreportfrequencytype;
CREATE TABLE olapts.abreportfrequencytype AS
select l.jsondoc_->>'Key' reportfrequencytypekey_,
l.jsondoc_->>'Value' reportfrequencytypevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ReportFrequencyType';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABREPORTFREQUENCYTYPE';
delete from olapts.refreshhistory where tablename = 'ABREPORTFREQUENCYTYPE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABREPORTFREQUENCYTYPE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abreportfrequencytype - part a end', clock_timestamp();

--ReportingFrequency
raise notice '% - Step abreportingfrequency - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abreportingfrequency;
CREATE TABLE olapts.abreportingfrequency AS
select l.jsondoc_->>'Key' reportingfrequencykey_,
l.jsondoc_->>'Value' reportingfrequencyvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ReportingFrequency';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABREPORTINGFREQUENCY';
delete from olapts.refreshhistory where tablename = 'ABREPORTINGFREQUENCY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABREPORTINGFREQUENCY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abreportingfrequency - part a end', clock_timestamp();

--State
raise notice '% - Step abstate - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abstate;
CREATE TABLE olapts.abstate AS
select l.jsondoc_->>'Key' statekey_,
l.jsondoc_->>'Value' statevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'State';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSTATE';
delete from olapts.refreshhistory where tablename = 'ABSTATE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSTATE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abstate - part a end', clock_timestamp();

--UserRole
raise notice '% - Step abuserrole - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abuserrole;
CREATE TABLE olapts.abuserrole AS
select l.jsondoc_->>'Key' userrolekey_,
l.jsondoc_->>'Value' userrolevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'UserRole';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUSERROLE';
delete from olapts.refreshhistory where tablename = 'ABUSERROLE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUSERROLE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abuserrole - part a end', clock_timestamp();

--dimusers
raise notice '% - Step abusers - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abusers;
CREATE TABLE olapts.abusers AS
select 
id_ dimusersid_,
pkid_,
jsondoc_->>'AuthenticationType' authenticationtype,
jsondoc_->>'Email' email,
jsondoc_->>'Id' id,
(jsondoc_->>'IdInt')::int idint,
(jsondoc_->>'IsSuperAdministrator')::boolean as issuperadministrator,
(jsondoc_->>'IsSystemUser')::boolean as issystemuser,
(jsondoc_->>'LastLoginDate')::date lastlogindate,
jsondoc_->>'Name' as "name",
jsondoc_->>'Password' as "password",
jsondoc_->>'Phone' phone,
jsondoc_->>'TenantId' tenantid,
jsondoc_->>'UserIdGuid' useridguid,
wfid_::varchar ,
taskid_::varchar ,
versionid_::int4 ,
isdeleted_::boolean ,
islatestversion_::boolean ,
baseversionid_::int4 ,
contextuserid_::varchar ,
isvisible_::boolean ,
isvalid_::boolean ,
snapshotid_::int4 ,
t_::varchar ,
createdby_::varchar ,
createddate_::timestamp ,
updatedby_::varchar ,
updateddate_::timestamp ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.users;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUSERS';
delete from olapts.refreshhistory where tablename = 'ABUSERS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUSERS' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abusers - part a end', clock_timestamp();

-- Model C Category
--BusinessActivity
raise notice '% - Step abbusinessactivity - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abbusinessactivity;
CREATE TABLE olapts.abbusinessactivity AS
select 
l.jsondoc_->>'Key' businessactivitykey_ ,
l.jsondoc_->>'Value' businessactivityvalue,
l.jsondoc_->>'Score' businessactivityscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'BusinessActivity';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABBUSINESSACTIVITY';
delete from olapts.refreshhistory where tablename = 'ABBUSINESSACTIVITY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABBUSINESSACTIVITY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abbusinessactivity - part a end', clock_timestamp();


--BuildingCondition
raise notice '% - Step abbuildingcondition - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abbuildingcondition;
CREATE TABLE olapts.abbuildingcondition AS
select 
l.jsondoc_->>'Key' buildingconditionkey_ ,
l.jsondoc_->>'Value' buildingconditionvalue,
l.jsondoc_->>'Score' buildingconditionscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'BuildingCondition';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABBUILDINGCONDITION';
delete from olapts.refreshhistory where tablename = 'ABBUILDINGCONDITION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABBUILDINGCONDITION' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abbuildingcondition - part a end', clock_timestamp();

--CompetitionCmodel
raise notice '% - Step abcompetitioncmodel - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcompetitioncmodel;
CREATE TABLE olapts.abcompetitioncmodel AS
select 
l.jsondoc_->>'Key' competitioncmodelkey_ ,
l.jsondoc_->>'Value' competitioncmodelvalue,
l.jsondoc_->>'Score' competitioncmodelscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CompetitionCmodel';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCOMPETITIONCMODEL';
delete from olapts.refreshhistory where tablename = 'ABCOMPETITIONCMODEL';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCOMPETITIONCMODEL' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcompetitioncmodel - part a end', clock_timestamp();

--Customer
raise notice '% - Step abcustomer - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcustomer;
CREATE TABLE olapts.abcustomer AS
select 
l.jsondoc_->>'Key' customerkey_ ,
l.jsondoc_->>'Value' customervalue,
l.jsondoc_->>'Score' customerscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Customer';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCUSTOMER';
delete from olapts.refreshhistory where tablename = 'ABCUSTOMER';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCUSTOMER' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcustomer - part a end', clock_timestamp();

--TrueFalse
raise notice '% - Step abtruefalse - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abtruefalse;
CREATE TABLE olapts.abtruefalse AS
select 
l.jsondoc_->>'Key' truefalsekey_ ,
l.jsondoc_->>'Value' truefalsevalue,
l.jsondoc_->>'Score' truefalsescore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'TrueFalse';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABTRUEFALSE';
delete from olapts.refreshhistory where tablename = 'ABTRUEFALSE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABTRUEFALSE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abtruefalse - part a end', clock_timestamp();

--GeographicalCoverage
raise notice '% - Step abgeographicalcoverage - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abgeographicalcoverage;
CREATE TABLE olapts.abgeographicalcoverage AS
select 
l.jsondoc_->>'Key' geographicalcoveragekey_ ,
l.jsondoc_->>'Value' geographicalcoveragevalue,
l.jsondoc_->>'Score' geographicalcoveragescore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'GeographicalCoverage';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABGEOGRAPHICALCOVERAGE';
delete from olapts.refreshhistory where tablename = 'ABGEOGRAPHICALCOVERAGE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABGEOGRAPHICALCOVERAGE' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abgeographicalcoverage - part a end', clock_timestamp();

--ManagerCapability
raise notice '% - Step abmanagercapability - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmanagercapability;
CREATE TABLE olapts.abmanagercapability AS
select 
l.jsondoc_->>'Key' managercapabilitykey_ ,
l.jsondoc_->>'Value' managercapabilityvalue,
l.jsondoc_->>'Score' managercapabilityscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ManagerCapability';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMANAGERCAPABILITY';
delete from olapts.refreshhistory where tablename = 'ABMANAGERCAPABILITY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMANAGERCAPABILITY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmanagercapability - part a end', clock_timestamp();

--Marketing
raise notice '% - Step abmarketing - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmarketing;
CREATE TABLE olapts.abmarketing AS
select 
l.jsondoc_->>'Key' marketingkey_ ,
l.jsondoc_->>'Value' marketingvalue,
l.jsondoc_->>'Score' marketingscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Marketing';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMARKETING';
delete from olapts.refreshhistory where tablename = 'ABMARKETING';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMARKETING' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmarketing - part a end', clock_timestamp();

--ProductsAndServices
raise notice '% - Step abproductsandservices - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abproductsandservices;
CREATE TABLE olapts.abproductsandservices AS
select 
l.jsondoc_->>'Key' productsandserviceskey_ ,
l.jsondoc_->>'Value' productsandservicesvalue,
l.jsondoc_->>'Score' productsandservicesscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ProductsAndServices';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPRODUCTSANDSERVICES';
delete from olapts.refreshhistory where tablename = 'ABPRODUCTSANDSERVICES';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPRODUCTSANDSERVICES' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abproductsandservices - part a end', clock_timestamp();

--YesNo
raise notice '% - Step abyesno - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abyesno;
CREATE TABLE olapts.abyesno AS
select 
l.jsondoc_->>'Key' yesnokey_ ,
l.jsondoc_->>'Value' yesnovalue,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'YesNo';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABYESNO';
delete from olapts.refreshhistory where tablename = 'ABYESNO';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABYESNO' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abyesno - part a end', clock_timestamp();

--Sector
raise notice '% - Step absector - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.absector;
CREATE TABLE olapts.absector AS
select 
l.jsondoc_->>'Key' sectorkey_ ,
l.jsondoc_->>'Value' sectorvalue,
l.jsondoc_->>'Score' sectorscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Sector';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSECTOR';
delete from olapts.refreshhistory where tablename = 'ABSECTOR';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSECTOR' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step absector - part a end', clock_timestamp();

--Succession
raise notice '% - Step absuccession - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.absuccession;
CREATE TABLE olapts.absuccession AS
select 
l.jsondoc_->>'Key' successionkey_ ,
l.jsondoc_->>'Value' successionvalue,
l.jsondoc_->>'Score' successionscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Succession';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSUCCESSION';
delete from olapts.refreshhistory where tablename = 'ABSUCCESSION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSUCCESSION' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step absuccession - part a end', clock_timestamp();

--Technology
raise notice '% - Step abtechnology - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abtechnology;
CREATE TABLE olapts.abtechnology AS
select 
l.jsondoc_->>'Key' technologykey_ ,
l.jsondoc_->>'Value' technologyvalue,
l.jsondoc_->>'Score' technologyscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Technology';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABTECHNOLOGY';
delete from olapts.refreshhistory where tablename = 'ABTECHNOLOGY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABTECHNOLOGY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abtechnology - part a end', clock_timestamp();
--End Model C Category lookup


--Shipping Scorecard New lookup
--AbilityToManageSp
raise notice '% - Step ababilitytomanagesp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ababilitytomanagesp;
CREATE TABLE olapts.ababilitytomanagesp AS
select 
l.jsondoc_->>'Id' abilitytomanagespkey_ ,
l.jsondoc_->>'Value' abilitytomanagespvalue,
l.jsondoc_->>'Score' abilitytomanagespscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'AbilityToManageSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABABILITYTOMANAGESP';
delete from olapts.refreshhistory where tablename = 'ABABILITYTOMANAGESP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABABILITYTOMANAGESP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step ababilitytomanagesp - part a end', clock_timestamp();


--AssetControlSp
raise notice '% - Step abassetcontrolsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abassetcontrolsp;
CREATE TABLE olapts.abassetcontrolsp AS
select 
l.jsondoc_->>'Id' assetcontrolspkey_ ,
l.jsondoc_->>'Value' assetcontrolspvalue,
l.jsondoc_->>'Score' assetcontrolspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'AssetControlSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABASSETCONTROLSP';
delete from olapts.refreshhistory where tablename = 'ABASSETCONTROLSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABASSETCONTROLSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abassetcontrolsp - part a end', clock_timestamp();


--CapabilityToreMarketSp
raise notice '% - Step abcapabilitytoremarketsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcapabilitytoremarketsp;
CREATE TABLE olapts.abcapabilitytoremarketsp AS
select 
l.jsondoc_->>'Id' capabilitytoremarketspkey_ ,
l.jsondoc_->>'Value' capabilitytoremarketspvalue,
l.jsondoc_->>'Score' capabilitytoremarketspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CapabilityToreMarketSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCAPABILITYTOREMARKETSP';
delete from olapts.refreshhistory where tablename = 'ABCAPABILITYTOREMARKETSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCAPABILITYTOREMARKETSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcapabilitytoremarketsp - part a end', clock_timestamp();


--ContractStructureSp
raise notice '% - Step abcontractstructuresp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcontractstructuresp;
CREATE TABLE olapts.abcontractstructuresp AS
select 
l.jsondoc_->>'Id' contractstructurespkey_ ,
l.jsondoc_->>'Value' contractstructurespvalue,
l.jsondoc_->>'Score' contractstructurespscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ContractStructureSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCONTRACTSTRUCTURESP';
delete from olapts.refreshhistory where tablename = 'ABCONTRACTSTRUCTURESP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCONTRACTSTRUCTURESP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcontractstructuresp - part a end', clock_timestamp();


--CooperationYearsSp
raise notice '% - Step abcooperationyearssp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcooperationyearssp;
CREATE TABLE olapts.abcooperationyearssp AS
select 
l.jsondoc_->>'Id' cooperationyearsspkey_ ,
l.jsondoc_->>'Value' cooperationyearsspvalue,
l.jsondoc_->>'Score' cooperationyearsspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CooperationYearsSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCOOPERATIONYEARSSP';
delete from olapts.refreshhistory where tablename = 'ABCOOPERATIONYEARSSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCOOPERATIONYEARSSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcooperationyearssp - part a end', clock_timestamp();

--CorporateGovernanceSp
raise notice '% - Step abcorporategovernancesp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcorporategovernancesp;
CREATE TABLE olapts.abcorporategovernancesp AS
select 
l.jsondoc_->>'Id' corporategovernancespkey_ ,
l.jsondoc_->>'Value' corporategovernancespvalue,
l.jsondoc_->>'Score' corporategovernancespscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CorporateGovernanceSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCORPORATEGOVERNANCESP';
delete from olapts.refreshhistory where tablename = 'ABCORPORATEGOVERNANCESP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCORPORATEGOVERNANCESP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcorporategovernancesp - part a end', clock_timestamp();

--CreditHistorySp
raise notice '% - Step abcredithistorysp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcredithistorysp;
CREATE TABLE olapts.abcredithistorysp AS
select 
l.jsondoc_->>'Id' credithistoryspkey_ ,
l.jsondoc_->>'Value' credithistoryspvalue,
l.jsondoc_->>'Score' credithistoryspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CreditHistorySp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCREDITHISTORYSP';
delete from olapts.refreshhistory where tablename = 'ABCREDITHISTORYSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCREDITHISTORYSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcredithistorysp - part a end', clock_timestamp();


--CurrentResaleValueSp
raise notice '% - Step abcurrentresalevaluesp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcurrentresalevaluesp;
CREATE TABLE olapts.abcurrentresalevaluesp AS
select 
l.jsondoc_->>'Id' currentresalevaluespkey_ ,
l.jsondoc_->>'Value' currentresalevaluespvalue,
l.jsondoc_->>'Score' currentresalevaluespscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CurrentResaleValueSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCURRENTRESALEVALUESP';
delete from olapts.refreshhistory where tablename = 'ABCURRENTRESALEVALUESP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCURRENTRESALEVALUESP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abcurrentresalevaluesp - part a end', clock_timestamp();


--DeadWeightTonnageSp
raise notice '% - Step abdeadweighttonnagesp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abdeadweighttonnagesp;
CREATE TABLE olapts.abdeadweighttonnagesp AS
select 
l.jsondoc_->>'Id' deadweighttonnagespkey_ ,
l.jsondoc_->>'Value' deadweighttonnagespvalue,
l.jsondoc_->>'Score' deadweighttonnagespscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'DeadWeightTonnageSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABDEADWEIGHTTONNAGESP';
delete from olapts.refreshhistory where tablename = 'ABDEADWEIGHTTONNAGESP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABDEADWEIGHTTONNAGESP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abdeadweighttonnagesp - part a end', clock_timestamp();


--DebtServiceCoverageRatioSp
raise notice '% - Step abdebtservicecoverageratiosp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abdebtservicecoverageratiosp;
CREATE TABLE olapts.abdebtservicecoverageratiosp AS
select 
l.jsondoc_->>'Id' debtservicecoverageratiospkey_ ,
l.jsondoc_->>'Value' debtservicecoverageratiospvalue,
l.jsondoc_->>'Score' debtservicecoverageratiospscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'DebtServiceCoverageRatioSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABDEBTSERVICECOVERAGERATIOSP';
delete from olapts.refreshhistory where tablename = 'ABDEBTSERVICECOVERAGERATIOSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABDEBTSERVICECOVERAGERATIOSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abdebtservicecoverageratiosp - part a end', clock_timestamp();


--ExternalPaymentBahaviorSp
raise notice '% - Step abexternalpaymentbahaviorsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abexternalpaymentbahaviorsp;
CREATE TABLE olapts.abexternalpaymentbahaviorsp AS
select 
l.jsondoc_->>'Id' externalpaymentbahaviorspkey_ ,
l.jsondoc_->>'Value' externalpaymentbahaviorspvalue,
l.jsondoc_->>'Score' externalpaymentbahaviorspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ExternalPaymentBahaviorSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABEXTERNALPAYMENTBAHAVIORSP';
delete from olapts.refreshhistory where tablename = 'ABEXTERNALPAYMENTBAHAVIORSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABEXTERNALPAYMENTBAHAVIORSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abexternalpaymentbahaviorsp - part a end', clock_timestamp();


--FinanceVesselsSp
raise notice '% - Step abfinancevesselssp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abfinancevesselssp;
CREATE TABLE olapts.abfinancevesselssp AS
select 
l.jsondoc_->>'Id' financevesselsspkey_ ,
l.jsondoc_->>'Value' financevesselsspvalue,
l.jsondoc_->>'Score' financevesselsspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'FinanceVesselsSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFINANCEVESSELSSP';
delete from olapts.refreshhistory where tablename = 'ABFINANCEVESSELSSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFINANCEVESSELSSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abfinancevesselssp - part a end', clock_timestamp();


--FinancialtermsSp
raise notice '% - Step abfinancialtermssp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abfinancialtermssp;
CREATE TABLE olapts.abfinancialtermssp AS
select 
l.jsondoc_->>'Id' financialtermsspkey_ ,
l.jsondoc_->>'Value' financialtermsspvalue,
l.jsondoc_->>'Score' financialtermsspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'FinancialtermsSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFINANCIALTERMSSP';
delete from olapts.refreshhistory where tablename = 'ABFINANCIALTERMSSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFINANCIALTERMSSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abfinancialtermssp - part a end', clock_timestamp();

--InformationDisclosureSp
raise notice '% - Step abinformationdisclosuresp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abinformationdisclosuresp;
CREATE TABLE olapts.abinformationdisclosuresp AS
select 
l.jsondoc_->>'Id' informationdisclosurespkey_ ,
l.jsondoc_->>'Value' informationdisclosurespvalue,
l.jsondoc_->>'Score' informationdisclosurespscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'InformationDisclosureSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABINFORMATIONDISCLOSURESP';
delete from olapts.refreshhistory where tablename = 'ABINFORMATIONDISCLOSURESP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABINFORMATIONDISCLOSURESP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abinformationdisclosuresp - part a end', clock_timestamp();

--InsuranceAgainstDamagesSp
raise notice '% - Step abinsuranceagainstdamagessp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abinsuranceagainstdamagessp;
CREATE TABLE olapts.abinsuranceagainstdamagessp AS
select 
l.jsondoc_->>'Id' insuranceagainstdamagesspkey_ ,
l.jsondoc_->>'Value' insuranceagainstdamagesspvalue,
l.jsondoc_->>'Score' insuranceagainstdamagesspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'InsuranceAgainstDamagesSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABINSURANCEAGAINSTDAMAGESSP';
delete from olapts.refreshhistory where tablename = 'ABINSURANCEAGAINSTDAMAGESSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABINSURANCEAGAINSTDAMAGESSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abinsuranceagainstdamagessp - part a end', clock_timestamp();

--LegislativeRegulatoryRiskSp
raise notice '% - Step ablegislativeregulatoryrisksp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ablegislativeregulatoryrisksp;
CREATE TABLE olapts.ablegislativeregulatoryrisksp AS
select 
l.jsondoc_->>'Id' legislativeregulatoryriskspkey_ ,
l.jsondoc_->>'Value' legislativeregulatoryriskspvalue,
l.jsondoc_->>'Score' legislativeregulatoryriskspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'LegislativeRegulatoryRiskSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLEGISLATIVEREGULATORYRISKSP';
delete from olapts.refreshhistory where tablename = 'ABLEGISLATIVEREGULATORYRISKSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLEGISLATIVEREGULATORYRISKSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step ablegislativeregulatoryrisksp - part a end', clock_timestamp();


--LiquidationTimeObjectSp
raise notice '% - Step abliquidationtimeobjectsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abliquidationtimeobjectsp;
CREATE TABLE olapts.abliquidationtimeobjectsp AS
select 
l.jsondoc_->>'Id' liquidationtimeobjectspkey_ ,
l.jsondoc_->>'Value' liquidationtimeobjectspvalue,
l.jsondoc_->>'Score' liquidationtimeobjectspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'LiquidationTimeObjectSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLIQUIDATIONTIMEOBJECTSP';
delete from olapts.refreshhistory where tablename = 'ABLIQUIDATIONTIMEOBJECTSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLIQUIDATIONTIMEOBJECTSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abliquidationtimeobjectsp - part a end', clock_timestamp();


--LtvSp
raise notice '% - Step abltvsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abltvsp;
CREATE TABLE olapts.abltvsp AS
select 
l.jsondoc_->>'Id' ltvkey_ ,
l.jsondoc_->>'Value' ltvvalue,
l.jsondoc_->>'Score' ltvscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'LtvSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLTVSP';
delete from olapts.refreshhistory where tablename = 'ABLTVSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLTVSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abltvsp - part a end', clock_timestamp();


--MarketStructureSp
raise notice '% - Step abmarketstructuresp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmarketstructuresp;
CREATE TABLE olapts.abmarketstructuresp AS
select 
l.jsondoc_->>'Id' marketstructurespkey_ ,
l.jsondoc_->>'Value' marketstructurespvalue,
l.jsondoc_->>'Score' marketstructurespscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MarketStructureSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMARKETSTRUCTURESP';
delete from olapts.refreshhistory where tablename = 'ABMARKETSTRUCTURESP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMARKETSTRUCTURESP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmarketstructuresp - part a end', clock_timestamp();


--OperatingYearsSp
raise notice '% - Step aboperatingyearssp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboperatingyearssp;
CREATE TABLE olapts.aboperatingyearssp AS
select 
l.jsondoc_->>'Id' operatingyearsspkey_ ,
l.jsondoc_->>'Value' operatingyearsspvalue,
l.jsondoc_->>'Score' operatingyearsspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'OperatingYearsSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATINGYEARSSP';
delete from olapts.refreshhistory where tablename = 'ABOPERATINGYEARSSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATINGYEARSSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step aboperatingyearssp - part a end', clock_timestamp();


--OperatorsTrackSp
raise notice '% - Step aboperatorstracksp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboperatorstracksp;
CREATE TABLE olapts.aboperatorstracksp AS
select 
l.jsondoc_->>'Id' operatorstrackspkey_ ,
l.jsondoc_->>'Value' operatorstrackspvalue,
l.jsondoc_->>'Score' operatorstrackspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'OperatorsTrackSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATORSTRACKSP';
delete from olapts.refreshhistory where tablename = 'ABOPERATORSTRACKSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATORSTRACKSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step aboperatorstracksp - part a end', clock_timestamp();


--ParentalSupportSp
raise notice '% - Step abparentalsupportsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abparentalsupportsp;
CREATE TABLE olapts.abparentalsupportsp AS
select 
l.jsondoc_->>'Id' parentalsupportspkey_ ,
l.jsondoc_->>'Value' parentalsupportspvalue,
l.jsondoc_->>'Score' parentalsupportspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ParentalSupportSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPARENTALSUPPORTSP';
delete from olapts.refreshhistory where tablename = 'ABPARENTALSUPPORTSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPARENTALSUPPORTSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abparentalsupportsp - part a end', clock_timestamp();


--PermitsLicensingSp
raise notice '% - Step abpermitslicensingsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abpermitslicensingsp;
CREATE TABLE olapts.abpermitslicensingsp AS
select 
l.jsondoc_->>'Id' permitslicensingspkey_ ,
l.jsondoc_->>'Value' permitslicensingspvalue,
l.jsondoc_->>'Score' permitslicensingspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'PermitsLicensingSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPERMITSLICENSINGSP';
delete from olapts.refreshhistory where tablename = 'ABPERMITSLICENSINGSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPERMITSLICENSINGSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abpermitslicensingsp - part a end', clock_timestamp();

--PoliticalRiskSp
raise notice '% - Step abpoliticalrisksp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abpoliticalrisksp;
CREATE TABLE olapts.abpoliticalrisksp AS
select 
l.jsondoc_->>'Id' politicalriskspkey_ ,
l.jsondoc_->>'Value' politicalriskspvalue,
l.jsondoc_->>'Score' politicalriskspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'PoliticalRiskSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPOLITICALRISKSP';
delete from olapts.refreshhistory where tablename = 'ABPOLITICALRISKSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPOLITICALRISKSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abpoliticalrisksp - part a end', clock_timestamp();

--QualityOfCharterersSp
raise notice '% - Step abqualityofchartererssp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abqualityofchartererssp;
CREATE TABLE olapts.abqualityofchartererssp AS
select 
l.jsondoc_->>'Id' qualityofcharterersspkey_ ,
l.jsondoc_->>'Value' qualityofcharterersspvalue,
l.jsondoc_->>'Score' qualityofcharterersspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'QualityOfCharterersSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABQUALITYOFCHARTERERSSP';
delete from olapts.refreshhistory where tablename = 'ABQUALITYOFCHARTERERSSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABQUALITYOFCHARTERERSSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abqualityofchartererssp - part a end', clock_timestamp();

--RightsAndMeansSp
raise notice '% - Step abrightsandmeanssp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abrightsandmeanssp;
CREATE TABLE olapts.abrightsandmeanssp AS
select 
l.jsondoc_->>'Id' rightsandmeansspkey_ ,
l.jsondoc_->>'Value' rightsandmeansspvalue,
l.jsondoc_->>'Score' rightsandmeansspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'RightsAndMeansSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRIGHTSANDMEANSSP';
delete from olapts.refreshhistory where tablename = 'ABRIGHTSANDMEANSSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRIGHTSANDMEANSSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abrightsandmeanssp - part a end', clock_timestamp();



--SensitivityOfTheAssetSp
raise notice '% - Step absensitivityoftheassetsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.absensitivityoftheassetsp;
CREATE TABLE olapts.absensitivityoftheassetsp AS
select 
l.jsondoc_->>'Id' sensitivityoftheassetspkey_ ,
l.jsondoc_->>'Value' sensitivityoftheassetspvalue,
l.jsondoc_->>'Score' sensitivityoftheassetspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'SensitivityOfTheAssetSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSENSITIVITYOFTHEASSETSP';
delete from olapts.refreshhistory where tablename = 'ABSENSITIVITYOFTHEASSETSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSENSITIVITYOFTHEASSETSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step absensitivityoftheassetsp - part a end', clock_timestamp();

--SuccessionPlanSp
raise notice '% - Step absuccessionplansp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.absuccessionplansp;
CREATE TABLE olapts.absuccessionplansp AS
select 
l.jsondoc_->>'Id' successionplankey_ ,
l.jsondoc_->>'Value' successionplanvalue,
l.jsondoc_->>'Score' successionplanscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'SuccessionPlanSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSUCCESSIONPLANSP';
delete from olapts.refreshhistory where tablename = 'ABSUCCESSIONPLANSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSUCCESSIONPLANSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step absuccessionplansp - part a end', clock_timestamp();


--SupplyandDemandSp
raise notice '% - Step absupplyanddemandsp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.absupplyanddemandsp;
CREATE TABLE olapts.absupplyanddemandsp AS
select 
l.jsondoc_->>'Id' supplyanddemandspkey_ ,
l.jsondoc_->>'Value' supplyanddemandspvalue,
l.jsondoc_->>'Score' supplyanddemandspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'SupplyandDemandSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSUPPLYANDDEMANDSP';
delete from olapts.refreshhistory where tablename = 'ABSUPPLYANDDEMANDSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSUPPLYANDDEMANDSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step absupplyanddemandsp - part a end', clock_timestamp();


--TotalIncomeTotalOperatingExpensesSp
raise notice '% - Step abtotalincometotaloperatingexpensessp - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abtotalincometotaloperatingexpensessp;
CREATE TABLE olapts.abtotalincometotaloperatingexpensessp AS
select 
l.jsondoc_->>'Id' totalincometotaloperatingexpensesspkey_ ,
l.jsondoc_->>'Value' totalincometotaloperatingexpensesspvalue,
l.jsondoc_->>'Score' totalincometotaloperatingexpensesspscore,
isdeleted_::boolean,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'TotalIncomeTotalOperatingExpensesSp';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'TOTALINCOMETOTALOPERATINGEXPENSESSP';
delete from olapts.refreshhistory where tablename = 'TOTALINCOMETOTALOPERATINGEXPENSESSP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'TOTALINCOMETOTALOPERATINGEXPENSESSP' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abtotalincometotaloperatingexpensessp - part a end', clock_timestamp();

--End Shipping Scorecard New lookup

-- End Lookup Tables
END $$;


--ABFACTBANKSYSTEM
DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

-- Custom Table
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABFACTBANKSYSTEM') THEN
	raise notice '% - Step abfactbanksystem - part a start', clock_timestamp(); 
	insert into olapts.abfactbanksystem
	(
		 "CustID"
		,"Seq_NO"
		,"AFM"
		,"ShippingID"
		,"CustomerName" 
		,"PeerCode"
		,"Score" 
		,"Grade"
		,"EquivalentPD"
		,"OverrideGrade"
		,"OverrideReason"
		,"OverrideAuthorized" 
		,"OverrideAuthorizer" -- not migrated
		,"OverridePD" 
		,"CustomerState"			-- TBD how this will be captured in CL
		,"ArchiveDate"
		,"EffectiveDate"
		,"EffectiveTime"
		,"NextReviewdate" 			-- TBD how this will be captured in CL
		,"RelationshipWithAlpha" 
		,"AM"   					-- TBD how this will be captured in CL
		,"UserName"
		,"UserProfile"				-- User can have more than one profile in CL
		,"MFA_Model"
		,"AccessGroup" 
		,"IRTModel"
		,"RespOffice"
		,"RespOfficer"
		,"CreditCommittee"
		,"Reviewtype"
		,"GroupCode"
		,"CDICode"
		,"ContractNumber"
		,"IndicationFinancialDiff1"
		,"IndicationFinancialDiff2"
		,"ActiveInactiveFlag"
		, sourcepopulatedby_
		, sourcepopulateddate_
	)
	select *
	from
	(	
	select
		distinct on (fe.pkid_, frs.jsondoc_ ->> 'ApproveId')
		fe.pkid_::numeric(9,0) as "CustID"
		,(frs.jsondoc_ ->> 'ApproveId')::varchar(36) as "Seq_NO"
		,(fe.jsondoc_ ->> 'Gc18')::varchar(50) as "AFM"
		,(fe.jsondoc_ ->> 'Gc16')::varchar(50) as "ShippingID"
		,(fe.jsondoc_ ->> 'LongName')::varchar(75) as "CustomerName" 
		,(dpa.jsondoc_ ->> 'PeerSIC')::varchar(50) as "PeerCode"
		,(frs.jsondoc_ ->> 'FinalScore')::varchar(10) as "Score" 
		,drsm.ratingscalevalue::varchar(10) as "Grade"
		,(frs.jsondoc_ ->> 'ModelPd')::varchar(10) as "EquivalentPD"
		,drso.ratingscalevalue::varchar(10) as "OverrideGrade"
		,dos.overridesettingvalue::varchar(255) as "OverrideReason"
		,(case when frs.jsondoc_ ->> 'OverrideGrade' is not null then true end)::varchar(255) as "OverrideAuthorized" 
		,(dro.jsondoc_ ->> 'OverrideAuthority')::varchar(255) as "OverrideAuthorizer" -- not migrated
		,(frs.jsondoc_ ->> 'OverridePd')::varchar(10) as "OverridePD" 
		,(frs.jsondoc_ ->> 'RatingStatus')::varchar(50) as "CustomerState"			-- TBD how this will be captured in CL
		,(frs.jsondoc_ ->> 'ApprovedDate')::timestamp as "ArchiveDate"
		,(frs.jsondoc_ ->> 'Creditcommitteedate')::timestamp as "EffectiveDate"
		,(frs.jsondoc_ ->> 'ApprovedDate')::timestamp as "EffectiveTime"
		,(frs.jsondoc_ ->> 'NextReviewDate')::timestamp as "NextReviewdate" 			-- TBD how this will be captured in CL
		,(fe.jsondoc_ ->> 'RelationShipType')::varchar(50) as "RelationshipWithAlpha" 
		,(frs.jsondoc_ ->> 'AmUser')::varchar(50) as "AM"   					-- TBD how this will be captured in CL
		,(dua.jsondoc_ ->> 'Name')::varchar(50) as "UserName"
		,null::varchar(50) as "UserProfile"				-- User can have more than one profile in CL
		,dft.name::varchar(50) as "MFA_Model"
		,dbp.businessportfoliovalue::varchar(50) as "AccessGroup" 
		,(frs.jsondoc_ ->> 'ModelId')::varchar(50) as "IRTModel"
		,(fe.jsondoc_ ->> 'ResponsibleOffice')::varchar(50) as "RespOffice"
		,(fe.jsondoc_ ->> 'ResponsibleOfficer')::varchar(50) as "RespOfficer"
		,(fe.jsondoc_ ->> 'CreditCommittee')::varchar(50) as "CreditCommittee"
		,(fe.jsondoc_ ->> 'ReviewType')::varchar(50) as "Reviewtype"
		,(fe.jsondoc_ ->> 'GroupId')::varchar(50) as "GroupCode"
		,(fe.jsondoc_ ->> 'CdiCode')::varchar(50) as "CDICode"
		,(fe.jsondoc_ ->> 'CreditNumber')::varchar(50) as "ContractNumber"
		,(fe.jsondoc_ ->> 'Gc115')::varchar(50) as "IndicationFinancialDiff1"
		,(fe.jsondoc_ ->> 'Gc116')::varchar(50) as "IndicationFinancialDiff2"
		,(fe.jsondoc_ ->> 'Gc117')::varchar(50) as "ActiveInactiveFlag"
		,frs.jsondoc_ ->> 'Approver' as sourcepopulatedby_
		,(frs.jsondoc_ ->> 'ApprovedDate')::timestamp sourcepopulateddate_
		from madata.ratingscenario frs
		inner join madata.entity fe
		on fe.pkid_ = frs.fkid_entity and fe.isvisible_ and fe.isvalid_ 
		and coalesce(fe.updateddate_, fe.createddate_) < (frs.jsondoc_ ->> 'ApprovedDate')::timestamp
		left join olapts.abfactbanksystem fbs
		on fbs."Seq_NO" = frs.jsondoc_ ->> 'ApproveId'	
		left join madata.financial df
		on regexp_replace(frs.jsondoc_ ->> 'FinancialContext', ':.*', '') = df.pkid_ and regexp_replace(frs.jsondoc_ ->> 'FinancialContext', '[0-9]*:([0-9]*)#.*', '\1') = df.versionid_::varchar and df.isvisible_ and df.isvalid_ 
		left join madata.peeranalysis dpa
		on df.pkid_ = dpa.fkid_financial and regexp_replace(frs.jsondoc_ ->> 'FinancialContext', '[0-9]*:[0-9]*#([0-9]*);.*', '\1') = dpa.versionid_::varchar and dpa.isvisible_ and dpa.isvalid_ 
		left join madata.ratingoverride dro
		on dro.fkid_ratingscenario = frs.pkid_ and (dro.jsondoc_ ->> 'IsLatest')::boolean and dro.islatestversion_ and not dro.isdeleted_ and dro.isvisible_ and dro.isvalid_ and not coalesce((dro.jsondoc_ ->> 'Rejected')::boolean,false) 
		left join olapts.dimratingscale drsm
		on frs.jsondoc_ ->> 'ModelGrade' = drsm.ratingscalekey_
		left join olapts.dimratingscale drso
		on frs.jsondoc_ ->> 'OverrideGrade' = drso.ratingscalekey_ 
		left join olapts.dimoverridesetting dos
		on dos.overridesettingkey_ = dro.jsondoc_ ->> 'OverrideReason'
		left join madata.users dua
		on frs.jsondoc_ ->> 'Approver' = dua.jsondoc_ ->> 'Id' and dua.islatestversion_ and dua.isvisible_ and dua.isvalid_
		left join madata.languagemodels dft
		on (df.jsondoc_ ->> 'FinancialTemplate')::numeric = dft.modelid
		left join olapts.abbusinessportfolio dbp
		on dbp.businessportfoliokey_ = fe.jsondoc_ ->> 'BusinessPortfolio'
		where frs.islatestversion_::boolean 
		and not frs.isdeleted_::boolean
		and frs.isvalid_::boolean
		and frs.isvisible_::boolean 
		and (frs.jsondoc_ ->> 'IsPrimary')::boolean 
		and (frs.jsondoc_ ->> 'IsApproved')::boolean
		and frs.snapshotid_ = 0 and fbs."AA" is null
		and	coalesce(frs.updateddate_, frs.createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABFACTBANKSYSTEM')
		order by fe.pkid_, frs.jsondoc_ ->> 'ApproveId', coalesce(fe.updateddate_, fe.createddate_) desc nulls last
	) XX
	order by XX."ArchiveDate"
	on conflict ("AA") do nothing;
	
	raise notice '% - Step abfactbanksystem - part a end', clock_timestamp(); 
ELSE
	raise notice '% - Step abfactbanksystem - part b start', clock_timestamp(); 
	
	DROP VIEW IF EXISTS olapts.abrs_export_new;
	DROP TABLE IF EXISTS olapts.ABFACTBANKSYSTEM;
	
	CREATE SEQUENCE IF NOT EXISTS olapts.abfactbanksystem_aa
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1
	;

	PERFORM setval('olapts.abfactbanksystem_aa'::regclass, MAX("AA")::bigint) FROM olapts.factbanksystem;

	CREATE TABLE IF NOT EXISTS olapts.abfactbanksystem
	(
		"AA" numeric(9,0) NOT NULL DEFAULT nextval('olapts.abfactbanksystem_aa'::regclass),
		"CustID" numeric(9,0),
		"Seq_NO" character varying(36) COLLATE pg_catalog."default",
		"AFM" character varying(100) COLLATE pg_catalog."default",
		"ShippingID" character varying(100) COLLATE pg_catalog."default",
		"CustomerName" character varying(75) COLLATE pg_catalog."default",
		"PeerCode" character varying(100) COLLATE pg_catalog."default",
		"Score" character varying(10) COLLATE pg_catalog."default",
		"Grade" character varying(10) COLLATE pg_catalog."default",
		"EquivalentPD" character varying(10) COLLATE pg_catalog."default",
		"OverrideGrade" character varying(10) COLLATE pg_catalog."default",
		"OverrideReason" character varying(255) COLLATE pg_catalog."default",
		"OverrideAuthorized" character varying(255) COLLATE pg_catalog."default",
		"OverrideAuthorizer" character varying(255) COLLATE pg_catalog."default",
		"OverridePD" character varying(10) COLLATE pg_catalog."default",
		"CustomerState" character varying(100) COLLATE pg_catalog."default",
		"ArchiveDate" timestamp without time zone,
		"EffectiveDate" timestamp without time zone,
		"EffectiveTime" timestamp without time zone,
		"NextReviewdate" timestamp without time zone,
		"RelationshipWithAlpha" character varying(100) COLLATE pg_catalog."default",
		"AM" character varying(100) COLLATE pg_catalog."default",
		"UserName" character varying(100) COLLATE pg_catalog."default",
		"UserProfile" character varying(100) COLLATE pg_catalog."default",
		"MFA_Model" character varying(100) COLLATE pg_catalog."default",
		"AccessGroup" character varying(100) COLLATE pg_catalog."default",
		"IRTModel" character varying(100) COLLATE pg_catalog."default",
		"RespOffice" character varying(100) COLLATE pg_catalog."default",
		"RespOfficer" character varying(100) COLLATE pg_catalog."default",
		"CreditCommittee" character varying(100) COLLATE pg_catalog."default",
		"Reviewtype" character varying(100) COLLATE pg_catalog."default",
		"GroupCode" character varying(100) COLLATE pg_catalog."default",
		"CDICode" character varying(100) COLLATE pg_catalog."default",
		"ContractNumber" character varying(100) COLLATE pg_catalog."default",
		"IndicationFinancialDiff1" character varying(100) COLLATE pg_catalog."default",
		"IndicationFinancialDiff2" character varying(100) COLLATE pg_catalog."default",
		"ActiveInactiveFlag" character varying(100) COLLATE pg_catalog."default",
		sourcepopulatedby_ character varying(100) COLLATE pg_catalog."default",
		sourcepopulateddate_ timestamp without time zone,
		populateddate_ timestamp without time zone DEFAULT timezone('utc'::text, (timeofday())::timestamp with time zone),
		CONSTRAINT pk_abfactbanksystem PRIMARY KEY ("AA")
	);

	-- copy previously reported data
	insert into olapts.abfactbanksystem
	select 
	"AA",
	"CustID",
	"Seq_NO",
	"AFM",
	"ShippingID",
	"CustomerName",
	"PeerCode",
	"Score",
	"Grade",
	"EquivalentPD",
	"OverrideGrade",
	"OverrideReason",
	"OverrideAuthorized",
	"OverrideAuthorizer",
	"OverridePD",
	"CustomerState",
	"ArchiveDate",
	"EffectiveDate",
	"EffectiveTime",
	"NextReviewdate",
	"RelationshipWithAlpha",
	"AM",
	"UserName",
	"UserProfile",
	"MFA_Model",
	"AccessGroup",
	"IRTModel",
	"RespOffice",
	"RespOfficer",
	"CreditCommittee",
	"Reviewtype",
	"GroupCode",
	"CDICode",
	"ContractNumber",
	"IndicationFinancialDiff1",
	"IndicationFinancialDiff2",
	"ActiveInactiveFlag",
	sourcepopulatedby_,
	sourcepopulateddate_,
	populateddate_
	from olapts.factbanksystem;
	
	
	-- set the max populateddate_ from reported data
	delete from olapts.refreshhistory where tablename = 'ABFACTBANKSYSTEM';
	insert into olapts.refreshhistory(tablename,asofdate) select 'ABFACTBANKSYSTEM' tablename,(max(coalesce(populateddate_,'2000-01-01 12:00:00.475338'))) as asofdate from olapts.factbanksystem;
	
	insert into olapts.abfactbanksystem
	(
		 "CustID"
		,"Seq_NO"
		,"AFM"
		,"ShippingID"
		,"CustomerName" 
		,"PeerCode"
		,"Score" 
		,"Grade"
		,"EquivalentPD"
		,"OverrideGrade"
		,"OverrideReason"
		,"OverrideAuthorized" 
		,"OverrideAuthorizer" -- not migrated
		,"OverridePD" 
		,"CustomerState"			-- TBD how this will be captured in CL
		,"ArchiveDate"
		,"EffectiveDate"
		,"EffectiveTime"
		,"NextReviewdate" 			-- TBD how this will be captured in CL
		,"RelationshipWithAlpha" 
		,"AM"   					-- TBD how this will be captured in CL
		,"UserName"
		,"UserProfile"				-- User can have more than one profile in CL
		,"MFA_Model"
		,"AccessGroup" 
		,"IRTModel"
		,"RespOffice"
		,"RespOfficer"
		,"CreditCommittee"
		,"Reviewtype"
		,"GroupCode"
		,"CDICode"
		,"ContractNumber"
		,"IndicationFinancialDiff1"
		,"IndicationFinancialDiff2"
		,"ActiveInactiveFlag"
		, sourcepopulatedby_
		, sourcepopulateddate_
	)
	select *
	from
	(	
	select
		distinct on (fe.pkid_, frs.jsondoc_ ->> 'ApproveId')
		fe.pkid_::numeric(9,0) as "CustID"
		,(frs.jsondoc_ ->> 'ApproveId')::varchar(36) as "Seq_NO"
		,(fe.jsondoc_ ->> 'Gc18')::varchar(50) as "AFM"
		,(fe.jsondoc_ ->> 'Gc16')::varchar(50) as "ShippingID"
		,(fe.jsondoc_ ->> 'LongName')::varchar(75) as "CustomerName" 
		,(dpa.jsondoc_ ->> 'PeerSIC')::varchar(50) as "PeerCode"
		,(frs.jsondoc_ ->> 'FinalScore')::varchar(10) as "Score" 
		,drsm.ratingscalevalue::varchar(10) as "Grade"
		,(frs.jsondoc_ ->> 'ModelPd')::varchar(10) as "EquivalentPD"
		,drso.ratingscalevalue::varchar(10) as "OverrideGrade"
		,dos.overridesettingvalue::varchar(255) as "OverrideReason"
		,(case when frs.jsondoc_ ->> 'OverrideGrade' is not null then true end)::varchar(255) as "OverrideAuthorized" 
		,(dro.jsondoc_ ->> 'OverrideAuthority')::varchar(255) as "OverrideAuthorizer" -- not migrated
		,(frs.jsondoc_ ->> 'OverridePd')::varchar(10) as "OverridePD" 
		,(frs.jsondoc_ ->> 'RatingStatus')::varchar(50) as "CustomerState"			-- TBD how this will be captured in CL
		,(frs.jsondoc_ ->> 'ApprovedDate')::timestamp as "ArchiveDate"
		,(frs.jsondoc_ ->> 'Creditcommitteedate')::timestamp as "EffectiveDate"
		,(frs.jsondoc_ ->> 'ApprovedDate')::timestamp as "EffectiveTime"
		,(frs.jsondoc_ ->> 'NextReviewDate')::timestamp as "NextReviewdate" 			-- TBD how this will be captured in CL
		,(fe.jsondoc_ ->> 'RelationShipType')::varchar(50) as "RelationshipWithAlpha" 
		,(frs.jsondoc_ ->> 'AmUser')::varchar(50) as "AM"   					-- TBD how this will be captured in CL
		,(dua.jsondoc_ ->> 'Name')::varchar(50) as "UserName"
		,null::varchar(50) as "UserProfile"				-- User can have more than one profile in CL
		,dft.name::varchar(50) as "MFA_Model"
		,dbp.businessportfoliovalue::varchar(50) as "AccessGroup" 
		,(frs.jsondoc_ ->> 'ModelId')::varchar(50) as "IRTModel"
		,(fe.jsondoc_ ->> 'ResponsibleOffice')::varchar(50) as "RespOffice"
		,(fe.jsondoc_ ->> 'ResponsibleOfficer')::varchar(50) as "RespOfficer"
		,(fe.jsondoc_ ->> 'CreditCommittee')::varchar(50) as "CreditCommittee"
		,(fe.jsondoc_ ->> 'ReviewType')::varchar(50) as "Reviewtype"
		,(fe.jsondoc_ ->> 'GroupId')::varchar(50) as "GroupCode"
		,(fe.jsondoc_ ->> 'CdiCode')::varchar(50) as "CDICode"
		,(fe.jsondoc_ ->> 'CreditNumber')::varchar(50) as "ContractNumber"
		,(fe.jsondoc_ ->> 'Gc115')::varchar(50) as "IndicationFinancialDiff1"
		,(fe.jsondoc_ ->> 'Gc116')::varchar(50) as "IndicationFinancialDiff2"
		,(fe.jsondoc_ ->> 'Gc117')::varchar(50) as "ActiveInactiveFlag"
		,frs.jsondoc_ ->> 'Approver' as sourcepopulatedby_
		,(frs.jsondoc_ ->> 'ApprovedDate')::timestamp sourcepopulateddate_
		from madata.ratingscenario frs
		inner join madata.entity fe
		on fe.pkid_ = frs.fkid_entity and fe.isvisible_ and fe.isvalid_ 
		and coalesce(fe.updateddate_, fe.createddate_) < (frs.jsondoc_ ->> 'ApprovedDate')::timestamp
		left join olapts.abfactbanksystem fbs
		on fbs."Seq_NO" = frs.jsondoc_ ->> 'ApproveId'	
		left join madata.financial df
		on regexp_replace(frs.jsondoc_ ->> 'FinancialContext', ':.*', '') = df.pkid_ and regexp_replace(frs.jsondoc_ ->> 'FinancialContext', '[0-9]*:([0-9]*)#.*', '\1') = df.versionid_::varchar and df.isvisible_ and df.isvalid_ 
		left join madata.peeranalysis dpa
		on df.pkid_ = dpa.fkid_financial and regexp_replace(frs.jsondoc_ ->> 'FinancialContext', '[0-9]*:[0-9]*#([0-9]*);.*', '\1') = dpa.versionid_::varchar and dpa.isvisible_ and dpa.isvalid_ 
		left join madata.ratingoverride dro
		on dro.fkid_ratingscenario = frs.pkid_ and (dro.jsondoc_ ->> 'IsLatest')::boolean and dro.islatestversion_ and not dro.isdeleted_ and dro.isvisible_ and dro.isvalid_ and not coalesce((dro.jsondoc_ ->> 'Rejected')::boolean,false) 
		left join olapts.dimratingscale drsm
		on frs.jsondoc_ ->> 'ModelGrade' = drsm.ratingscalekey_
		left join olapts.dimratingscale drso
		on frs.jsondoc_ ->> 'OverrideGrade' = drso.ratingscalekey_ 
		left join olapts.dimoverridesetting dos
		on dos.overridesettingkey_ = dro.jsondoc_ ->> 'OverrideReason'
		left join madata.users dua
		on frs.jsondoc_ ->> 'Approver' = dua.jsondoc_ ->> 'Id' and dua.islatestversion_ and dua.isvisible_ and dua.isvalid_
		left join madata.languagemodels dft
		on (df.jsondoc_ ->> 'FinancialTemplate')::numeric = dft.modelid
		left join olapts.abbusinessportfolio dbp
		on dbp.businessportfoliokey_ = fe.jsondoc_ ->> 'BusinessPortfolio'
		where frs.islatestversion_ 
		and not frs.isdeleted_ 
		and frs.isvalid_ 
		and frs.isvisible_ 
		and (frs.jsondoc_ ->> 'IsPrimary')::boolean 
		and (frs.jsondoc_ ->> 'IsApproved')::boolean
		and frs.snapshotid_ = 0 and fbs."AA" is null
		and	coalesce(frs.updateddate_, frs.createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABFACTBANKSYSTEM')
		order by fe.pkid_, frs.jsondoc_ ->> 'ApproveId', coalesce(fe.updateddate_, fe.createddate_) desc nulls last
	) XX
	order by XX."ArchiveDate"
	on conflict ("AA") do nothing;
	
	raise notice '% - Step abfactbanksystem - part b end', clock_timestamp(); 
END IF;

raise notice '% - Step abfactbanksystem - part c start', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFACTBANKSYSTEM';
delete from olapts.refreshhistory where tablename = 'ABFACTBANKSYSTEM';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFACTBANKSYSTEM' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abfactbanksystem - part c end', clock_timestamp(); 

END $$;

-- End Custom Tables

DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

-- Custom MAP Models - New Rating model 1: "Shipping Scorecard New" (Id: PdModelShippingScorecard)

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABMODELSHIPPINGSCORECARD') THEN
raise notice '% - Step abmodelshippingscorecard - part a start', clock_timestamp();
insert into olapts.abmodelshippingscorecard
SELECT 
mi.id_ factmappdinstanceid_,
mi.pkid_::varchar as pkid_,
(mi.jsondoc_->>'AbilityToManage')::int4 abilitytomanage,
l1.jsondoc_->>'Value' abilitytomanageval,
(mi.jsondoc_->>'AssetControl')::int4 assetcontrol,
l2.jsondoc_->>'Value' assetcontrolval,
(mi.jsondoc_->>'CapabilityToreMarket')::int4 capabilitytoremarket,
l3.jsondoc_->>'Value' capabilitytoremarketval,
(mi.jsondoc_->>'ContractStructure')::int4 contractstructure,
l4.jsondoc_->>'Value' contractstructureval,
(mi.jsondoc_->>'CooperationYears')::int4 cooperationyears,
l5.jsondoc_->>'Value' cooperationyearsval,
(mi.jsondoc_->>'CorporateGovernance')::int4 corporategovernance,
l6.jsondoc_->>'Value' corporategovernanceval,
(mi.jsondoc_->>'CreditHistory')::int4 credithistory,
l7.jsondoc_->>'Value' credithistoryval,
(mi.jsondoc_->>'CurrentResaleValue')::int4 currentresalevalue,
l8.jsondoc_->>'Value' currentresalevalueval,
(mi.jsondoc_->>'DeadWeightTonnage')::int4 deadweighttonnage,
l9.jsondoc_->>'Value' deadweighttonnageval,
(mi.jsondoc_->>'DebtServiceCoverageRatio')::int4 debtservicecoverageratio,
l10.jsondoc_->>'Value' debtservicecoverageratioval,
(mi.jsondoc_->>'ExternalPaymentBahavior')::int4 externalpaymentbahavior,
l11.jsondoc_->>'Value' externalpaymentbahaviorval,
(mi.jsondoc_->>'FinanceVessels')::int4 financevessels,
l12.jsondoc_->>'Value' financevesselsval,
(mi.jsondoc_->>'Financialterms')::int4 financialterms,
l13.jsondoc_->>'Value' financialtermsval,
(mi.jsondoc_->>'InformationDisclosure')::int4 informationdisclosure,
l14.jsondoc_->>'Value' informationdisclosureval,
(mi.jsondoc_->>'InsuranceAgainstDamages')::int4 insuranceagainstdamages,
l15.jsondoc_->>'Value' insuranceagainstdamagesval,
(mi.jsondoc_->>'LegislativeRegulatoryRisk')::int4 legislativeregulatoryrisk,
l16.jsondoc_->>'Value' legislativeregulatoryriskval,
(mi.jsondoc_->>'LiquidationTimeObject')::int4 liquidationtimeobject,
l17.jsondoc_->>'Value' liquidationtimeobjectval,
(mi.jsondoc_->>'Ltv')::int4 ltv,
l18.jsondoc_->>'Value' ltvval,
(mi.jsondoc_->>'MarketStructure')::int4 marketstructure,
l19.jsondoc_->>'Value' marketstructureval,
(mi.jsondoc_->>'OperatingYears')::int4 operatingyears,
l20.jsondoc_->>'Value' operatingyearsval,
(mi.jsondoc_->>'OperatorsTrack')::int4 operatorstrack,
l21.jsondoc_->>'Value' operatorstrackval,
(mi.jsondoc_->>'ParentalSupport')::int4 parentalsupport,
l22.jsondoc_->>'Value' parentalsupportval,
(mi.jsondoc_->>'PermitsLicensing')::int4 permitslicensing,
l23.jsondoc_->>'Value' permitslicensingval,
(mi.jsondoc_->>'PoliticalRisk')::int4 politicalrisk,
l24.jsondoc_->>'Value' politicalriskval,
(mi.jsondoc_->>'QualityOfCharterers')::int4 qualityofcharterers,
l25.jsondoc_->>'Value' qualityofcharterersval,
(mi.jsondoc_->>'RightsAndMeans')::int4 rightsandmeans,
l26.jsondoc_->>'Value' rightsandmeansval,
(mi.jsondoc_->>'SensitivityOfTheAsset')::int4 sensitivityoftheasset,
l27.jsondoc_->>'Value' sensitivityoftheassetval,
(mi.jsondoc_->>'SuccessionPlan')::int4 successionplan,
l28.jsondoc_->>'Value' successionplanval,
(mi.jsondoc_->>'SupplyandDemand')::int4 supplyanddemand,
l29.jsondoc_->>'Value' supplyanddemandval,
(mi.jsondoc_->>'TotalIncomeTotalOperatingExpen')::int4 totalincometotaloperatingexpen,
l30.jsondoc_->>'Value' totalincometotaloperatingexpenval,
(mi.jsondoc_->>'Token')::varchar token,
mi.wfid_::varchar ,
mi.taskid_::varchar ,
mi.versionid_::int4 ,
mi.isdeleted_::boolean ,
mi.islatestversion_::boolean ,
mi.baseversionid_::int4 ,
mi.contextuserid_::varchar ,
mi.isvisible_::boolean ,
mi.isvalid_::boolean ,
mi.snapshotid_::int4 ,
mi.t_::varchar ,
mi.createdby_::varchar ,
mi.createddate_::timestamp ,
mi.updatedby_::varchar ,
mi.updateddate_::timestamp ,
mi.fkid_entity,
(case when mi.updateddate_>mi.createddate_ then mi.updatedby_ else mi.createdby_ end) as sourcepopulatedby_,
GREATEST(mi.updateddate_,mi.createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_
from madata.mapinstance mi
left join madata.custom_lookup l1 on l1.t_ = 'AbilityToManageSp' and l1.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AbilityToManage'
left join madata.custom_lookup l2 on l2.t_ = 'AssetControlSp' and l2.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AssetControl'
left join madata.custom_lookup l3 on l3.t_ = 'CapabilityToreMarketSp' and l3.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CapabilityToreMarket'
left join madata.custom_lookup l4 on l4.t_ = 'ContractStructureSp' and l4.jsondoc_->>'Id' = mi.jsondoc_ ->> 'ContractStructure'
left join madata.custom_lookup l5 on l5.t_ = 'CooperationYearsSp' and l5.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CooperationYears'
left join madata.custom_lookup l6 on l6.t_ = 'CorporateGovernanceSp' and l6.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CorporateGovernance'
left join madata.custom_lookup l7 on l7.t_ = 'CreditHistorySp' and l7.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CreditHistory'
left join madata.custom_lookup l8 on l8.t_ = 'CurrentResaleValueSp' and l8.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CurrentResaleValue'
left join madata.custom_lookup l9 on l9.t_ = 'DeadWeightTonnageSp' and l9.jsondoc_->>'Id' = mi.jsondoc_ ->> 'DeadWeightTonnage'
left join madata.custom_lookup l10 on l10.t_ = 'DebtServiceCoverageRatioSp' and l10.jsondoc_->>'Id' = mi.jsondoc_ ->> 'DebtServiceCoverageRatio'
left join madata.custom_lookup l11 on l11.t_ = 'ExternalPaymentBahaviorSp' and l11.jsondoc_->>'Id' = mi.jsondoc_ ->> 'ExternalPaymentBahavior'
left join madata.custom_lookup l12 on l12.t_ = 'FinanceVesselsSp' and l12.jsondoc_->>'Id' = mi.jsondoc_ ->> 'FinanceVessels'
left join madata.custom_lookup l13 on l13.t_ = 'FinancialtermsSp' and l13.jsondoc_->>'Id' = mi.jsondoc_ ->> 'Financialterms'
left join madata.custom_lookup l14 on l14.t_ = 'InformationDisclosureSp' and l14.jsondoc_->>'Id' = mi.jsondoc_ ->> 'InformationDisclosure'
left join madata.custom_lookup l15 on l15.t_ = 'InsuranceAgainstDamagesSp' and l15.jsondoc_->>'Id' = mi.jsondoc_ ->> 'InsuranceAgainstDamages'
left join madata.custom_lookup l16 on l16.t_ = 'LegislativeRegulatoryRiskSp' and l16.jsondoc_->>'Id' = mi.jsondoc_ ->> 'LegislativeRegulatoryRisk'
left join madata.custom_lookup l17 on l17.t_ = 'LiquidationTimeObjectSp' and l17.jsondoc_->>'Id' = mi.jsondoc_ ->> 'LiquidationTimeObject'
left join madata.custom_lookup l18 on l18.t_ = 'LtvSp' and l18.jsondoc_->>'Id' = mi.jsondoc_ ->> 'Ltv'
left join madata.custom_lookup l19 on l19.t_ = 'MarketStructureSp' and l19.jsondoc_->>'Id' = mi.jsondoc_ ->> 'MarketStructure'
left join madata.custom_lookup l20 on l20.t_ = 'OperatingYearsSp' and l20.jsondoc_->>'Id' = mi.jsondoc_ ->> 'OperatingYears'
left join madata.custom_lookup l21 on l21.t_ = 'OperatorsTrackSp' and l21.jsondoc_->>'Id' = mi.jsondoc_ ->> 'OperatorsTrack'
left join madata.custom_lookup l22 on l22.t_ = 'ParentalSupportSp' and l22.jsondoc_->>'Id' = mi.jsondoc_ ->> 'ParentalSupport'
left join madata.custom_lookup l23 on l23.t_ = 'PermitsLicensingSp' and l23.jsondoc_->>'Id' = mi.jsondoc_ ->> 'PermitsLicensing'
left join madata.custom_lookup l24 on l24.t_ = 'PoliticalRiskSp' and l24.jsondoc_->>'Id' = mi.jsondoc_ ->> 'PoliticalRisk'
left join madata.custom_lookup l25 on l25.t_ = 'QualityOfCharterersSp' and l25.jsondoc_->>'Id' = mi.jsondoc_ ->> 'QualityOfCharterers'
left join madata.custom_lookup l26 on l26.t_ = 'RightsAndMeansSp' and l26.jsondoc_->>'Id' = mi.jsondoc_ ->> 'RightsAndMeans'
left join madata.custom_lookup l27 on l27.t_ = 'SensitivityOfTheAssetSp' and l27.jsondoc_->>'Id' = mi.jsondoc_ ->> 'SensitivityOfTheAsset'
left join madata.custom_lookup l28 on l28.t_ = 'SuccessionPlanSp' and l28.jsondoc_->>'Id' = mi.jsondoc_ ->> 'SuccessionPlan'
left join madata.custom_lookup l29 on l29.t_ = 'SupplyandDemandSp' and l29.jsondoc_->>'Id' = mi.jsondoc_ ->> 'SupplyandDemand'
left join madata.custom_lookup l30 on l30.t_ = 'TotalIncomeTotalOperatingExpensesSp' and l30.jsondoc_->>'Id' = mi.jsondoc_ ->> 'TotalIncomeTotalOperatingExpen'
where GREATEST(mi.updateddate_,mi.createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABMODELSHIPPINGSCORECARD')
and GREATEST(mi.updateddate_,mi.createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
and mi.t_ = 'PdModelShippingScorecard'
;


raise notice '% - Step abmodelshippingscorecard - part a end', clock_timestamp();
ELSE
raise notice '% - Step abmodelshippingscorecard - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmodelshippingscorecard;
CREATE TABLE olapts.abmodelshippingscorecard AS
SELECT 
mi.id_ factmappdinstanceid_,
mi.pkid_::varchar as pkid_,
(mi.jsondoc_->>'AbilityToManage')::int4 abilitytomanage,
l1.jsondoc_->>'Value' abilitytomanageval,
(mi.jsondoc_->>'AssetControl')::int4 assetcontrol,
l2.jsondoc_->>'Value' assetcontrolval,
(mi.jsondoc_->>'CapabilityToreMarket')::int4 capabilitytoremarket,
l3.jsondoc_->>'Value' capabilitytoremarketval,
(mi.jsondoc_->>'ContractStructure')::int4 contractstructure,
l4.jsondoc_->>'Value' contractstructureval,
(mi.jsondoc_->>'CooperationYears')::int4 cooperationyears,
l5.jsondoc_->>'Value' cooperationyearsval,
(mi.jsondoc_->>'CorporateGovernance')::int4 corporategovernance,
l6.jsondoc_->>'Value' corporategovernanceval,
(mi.jsondoc_->>'CreditHistory')::int4 credithistory,
l7.jsondoc_->>'Value' credithistoryval,
(mi.jsondoc_->>'CurrentResaleValue')::int4 currentresalevalue,
l8.jsondoc_->>'Value' currentresalevalueval,
(mi.jsondoc_->>'DeadWeightTonnage')::int4 deadweighttonnage,
l9.jsondoc_->>'Value' deadweighttonnageval,
(mi.jsondoc_->>'DebtServiceCoverageRatio')::int4 debtservicecoverageratio,
l10.jsondoc_->>'Value' debtservicecoverageratioval,
(mi.jsondoc_->>'ExternalPaymentBahavior')::int4 externalpaymentbahavior,
l11.jsondoc_->>'Value' externalpaymentbahaviorval,
(mi.jsondoc_->>'FinanceVessels')::int4 financevessels,
l12.jsondoc_->>'Value' financevesselsval,
(mi.jsondoc_->>'Financialterms')::int4 financialterms,
l13.jsondoc_->>'Value' financialtermsval,
(mi.jsondoc_->>'InformationDisclosure')::int4 informationdisclosure,
l14.jsondoc_->>'Value' informationdisclosureval,
(mi.jsondoc_->>'InsuranceAgainstDamages')::int4 insuranceagainstdamages,
l15.jsondoc_->>'Value' insuranceagainstdamagesval,
(mi.jsondoc_->>'LegislativeRegulatoryRisk')::int4 legislativeregulatoryrisk,
l16.jsondoc_->>'Value' legislativeregulatoryriskval,
(mi.jsondoc_->>'LiquidationTimeObject')::int4 liquidationtimeobject,
l17.jsondoc_->>'Value' liquidationtimeobjectval,
(mi.jsondoc_->>'Ltv')::int4 ltv,
l18.jsondoc_->>'Value' ltvval,
(mi.jsondoc_->>'MarketStructure')::int4 marketstructure,
l19.jsondoc_->>'Value' marketstructureval,
(mi.jsondoc_->>'OperatingYears')::int4 operatingyears,
l20.jsondoc_->>'Value' operatingyearsval,
(mi.jsondoc_->>'OperatorsTrack')::int4 operatorstrack,
l21.jsondoc_->>'Value' operatorstrackval,
(mi.jsondoc_->>'ParentalSupport')::int4 parentalsupport,
l22.jsondoc_->>'Value' parentalsupportval,
(mi.jsondoc_->>'PermitsLicensing')::int4 permitslicensing,
l23.jsondoc_->>'Value' permitslicensingval,
(mi.jsondoc_->>'PoliticalRisk')::int4 politicalrisk,
l24.jsondoc_->>'Value' politicalriskval,
(mi.jsondoc_->>'QualityOfCharterers')::int4 qualityofcharterers,
l25.jsondoc_->>'Value' qualityofcharterersval,
(mi.jsondoc_->>'RightsAndMeans')::int4 rightsandmeans,
l26.jsondoc_->>'Value' rightsandmeansval,
(mi.jsondoc_->>'SensitivityOfTheAsset')::int4 sensitivityoftheasset,
l27.jsondoc_->>'Value' sensitivityoftheassetval,
(mi.jsondoc_->>'SuccessionPlan')::int4 successionplan,
l28.jsondoc_->>'Value' successionplanval,
(mi.jsondoc_->>'SupplyandDemand')::int4 supplyanddemand,
l29.jsondoc_->>'Value' supplyanddemandval,
(mi.jsondoc_->>'TotalIncomeTotalOperatingExpen')::int4 totalincometotaloperatingexpen,
l30.jsondoc_->>'Value' totalincometotaloperatingexpenval,
(mi.jsondoc_->>'Token')::varchar token,
mi.wfid_::varchar ,
mi.taskid_::varchar ,
mi.versionid_::int4 ,
mi.isdeleted_::boolean ,
mi.islatestversion_::boolean ,
mi.baseversionid_::int4 ,
mi.contextuserid_::varchar ,
mi.isvisible_::boolean ,
mi.isvalid_::boolean ,
mi.snapshotid_::int4 ,
mi.t_::varchar ,
mi.createdby_::varchar ,
mi.createddate_::timestamp ,
mi.updatedby_::varchar ,
mi.updateddate_::timestamp ,
mi.fkid_entity,
(case when mi.updateddate_>mi.createddate_ then mi.updatedby_ else mi.createdby_ end) as sourcepopulatedby_,
GREATEST(mi.updateddate_,mi.createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_
from madata.mapinstance mi
left join madata.custom_lookup l1 on l1.t_ = 'AbilityToManageSp' and l1.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AbilityToManage'
left join madata.custom_lookup l2 on l2.t_ = 'AssetControlSp' and l2.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AssetControl'
left join madata.custom_lookup l3 on l3.t_ = 'CapabilityToreMarketSp' and l3.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CapabilityToreMarket'
left join madata.custom_lookup l4 on l4.t_ = 'ContractStructureSp' and l4.jsondoc_->>'Id' = mi.jsondoc_ ->> 'ContractStructure'
left join madata.custom_lookup l5 on l5.t_ = 'CooperationYearsSp' and l5.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CooperationYears'
left join madata.custom_lookup l6 on l6.t_ = 'CorporateGovernanceSp' and l6.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CorporateGovernance'
left join madata.custom_lookup l7 on l7.t_ = 'CreditHistorySp' and l7.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CreditHistory'
left join madata.custom_lookup l8 on l8.t_ = 'CurrentResaleValueSp' and l8.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CurrentResaleValue'
left join madata.custom_lookup l9 on l9.t_ = 'DeadWeightTonnageSp' and l9.jsondoc_->>'Id' = mi.jsondoc_ ->> 'DeadWeightTonnage'
left join madata.custom_lookup l10 on l10.t_ = 'DebtServiceCoverageRatioSp' and l10.jsondoc_->>'Id' = mi.jsondoc_ ->> 'DebtServiceCoverageRatio'
left join madata.custom_lookup l11 on l11.t_ = 'ExternalPaymentBahaviorSp' and l11.jsondoc_->>'Id' = mi.jsondoc_ ->> 'ExternalPaymentBahavior'
left join madata.custom_lookup l12 on l12.t_ = 'FinanceVesselsSp' and l12.jsondoc_->>'Id' = mi.jsondoc_ ->> 'FinanceVessels'
left join madata.custom_lookup l13 on l13.t_ = 'FinancialtermsSp' and l13.jsondoc_->>'Id' = mi.jsondoc_ ->> 'Financialterms'
left join madata.custom_lookup l14 on l14.t_ = 'InformationDisclosureSp' and l14.jsondoc_->>'Id' = mi.jsondoc_ ->> 'InformationDisclosure'
left join madata.custom_lookup l15 on l15.t_ = 'InsuranceAgainstDamagesSp' and l15.jsondoc_->>'Id' = mi.jsondoc_ ->> 'InsuranceAgainstDamages'
left join madata.custom_lookup l16 on l16.t_ = 'LegislativeRegulatoryRiskSp' and l16.jsondoc_->>'Id' = mi.jsondoc_ ->> 'LegislativeRegulatoryRisk'
left join madata.custom_lookup l17 on l17.t_ = 'LiquidationTimeObjectSp' and l17.jsondoc_->>'Id' = mi.jsondoc_ ->> 'LiquidationTimeObject'
left join madata.custom_lookup l18 on l18.t_ = 'LtvSp' and l18.jsondoc_->>'Id' = mi.jsondoc_ ->> 'Ltv'
left join madata.custom_lookup l19 on l19.t_ = 'MarketStructureSp' and l19.jsondoc_->>'Id' = mi.jsondoc_ ->> 'MarketStructure'
left join madata.custom_lookup l20 on l20.t_ = 'OperatingYearsSp' and l20.jsondoc_->>'Id' = mi.jsondoc_ ->> 'OperatingYears'
left join madata.custom_lookup l21 on l21.t_ = 'OperatorsTrackSp' and l21.jsondoc_->>'Id' = mi.jsondoc_ ->> 'OperatorsTrack'
left join madata.custom_lookup l22 on l22.t_ = 'ParentalSupportSp' and l22.jsondoc_->>'Id' = mi.jsondoc_ ->> 'ParentalSupport'
left join madata.custom_lookup l23 on l23.t_ = 'PermitsLicensingSp' and l23.jsondoc_->>'Id' = mi.jsondoc_ ->> 'PermitsLicensing'
left join madata.custom_lookup l24 on l24.t_ = 'PoliticalRiskSp' and l24.jsondoc_->>'Id' = mi.jsondoc_ ->> 'PoliticalRisk'
left join madata.custom_lookup l25 on l25.t_ = 'QualityOfCharterersSp' and l25.jsondoc_->>'Id' = mi.jsondoc_ ->> 'QualityOfCharterers'
left join madata.custom_lookup l26 on l26.t_ = 'RightsAndMeansSp' and l26.jsondoc_->>'Id' = mi.jsondoc_ ->> 'RightsAndMeans'
left join madata.custom_lookup l27 on l27.t_ = 'SensitivityOfTheAssetSp' and l27.jsondoc_->>'Id' = mi.jsondoc_ ->> 'SensitivityOfTheAsset'
left join madata.custom_lookup l28 on l28.t_ = 'SuccessionPlanSp' and l28.jsondoc_->>'Id' = mi.jsondoc_ ->> 'SuccessionPlan'
left join madata.custom_lookup l29 on l29.t_ = 'SupplyandDemandSp' and l29.jsondoc_->>'Id' = mi.jsondoc_ ->> 'SupplyandDemand'
left join madata.custom_lookup l30 on l30.t_ = 'TotalIncomeTotalOperatingExpensesSp' and l30.jsondoc_->>'Id' = mi.jsondoc_ ->> 'TotalIncomeTotalOperatingExpen'
where GREATEST(mi.updateddate_,mi.createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABMODELSHIPPINGSCORECARD')
and GREATEST(mi.updateddate_,mi.createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
and mi.t_ = 'PdModelShippingScorecard'
;

raise notice '% - Step abmodelshippingscorecard - part b end', clock_timestamp();

--abmodelshippingscorecard
raise notice '% - Step abmodelshippingscorecard_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abmodelshippingscorecard_idx;
DROP INDEX if exists olapts.abmodelshippingscorecard_idx2;

CREATE INDEX IF NOT EXISTS abmodelshippingscorecard_idx ON olapts.abmodelshippingscorecard (factmappdinstanceid_,pkid_,wfid_);
CREATE INDEX IF NOT EXISTS abmodelshippingscorecard_idx2 ON olapts.abmodelshippingscorecard (pkid_,versionid_,sourcepopulateddate_,wfid_) include(isvisible_,isvalid_,isdeleted_,islatestversion_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_);	

raise notice '% - Step abmodelshippingscorecard_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abmodelshippingscorecard - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmodelshippingscorecardflag;
CREATE TABLE olapts.abmodelshippingscorecardflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.mapinstance mi
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
and mi.t_ = 'PdModelShippingScorecard'
;

raise notice '% - Step abmodelshippingscorecardflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abmodelshippingscorecardflag_idx;
DROP INDEX if exists olapts.abmodelshippingscorecardflag_idx2;

CREATE INDEX IF NOT EXISTS abmodelshippingscorecardflag_idx ON olapts.abmodelshippingscorecardflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abmodelshippingscorecardflag_idx2 ON olapts.abmodelshippingscorecardflag (pkid_,versionid_,sourcepopulateddate_) include(isvisible_,isvalid_,isdeleted_,islatestversion_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);

raise notice '% - Step abmodelshippingscorecardflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMODELSHIPPINGSCORECARD';
delete from olapts.refreshhistory where tablename = 'ABMODELSHIPPINGSCORECARD';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMODELSHIPPINGSCORECARD' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMODELSHIPPINGSCORECARDFLAG';
delete from olapts.refreshhistory where tablename = 'ABMODELSHIPPINGSCORECARDFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMODELSHIPPINGSCORECARDFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abmodelshippingscorecard - part c end', clock_timestamp();
-- End Custom MAP Models - New Rating model 1: "Shipping Scorecard New" (Id: PdModelShippingScorecard)

-- Custom MAP Models -  New Rating model 2: "Model C Category" (Id: PdModelCcategory)

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABPDMODELCCATEGORY') THEN
raise notice '% - Step abpdmodelccategory - part a start', clock_timestamp();
insert into olapts.abpdmodelccategory
SELECT 
mi.id_ factmappdinstanceid_,
mi.pkid_::varchar as pkid_,
(mi.jsondoc_->>'AccountReceivablesDays')::numeric accountreceivablesdays,
(mi.jsondoc_->>'AccountReceivablesDays2')::numeric accountreceivablesdays2,
(mi.jsondoc_->>'AccountReceivablesDays3')::numeric accountreceivablesdays3,
(mi.jsondoc_->>'AccountsPayableDays')::numeric accountspayabledays,
(mi.jsondoc_->>'AccountsPayableDays2')::numeric accountspayabledays2,
(mi.jsondoc_->>'AccountsPayableDays3')::numeric accountspayabledays3,
(mi.jsondoc_->>'ActivityOfBusiness')::int4 activityofbusiness,
l1.jsondoc_->>'Value' activityofbusinessval,
(mi.jsondoc_->>'Afm')::varchar afm,
(mi.jsondoc_->>'BuildingCondition')::int4 buildingcondition,
l2.jsondoc_->>'Value' buildingconditionval,
(mi.jsondoc_->>'CashLiquidity')::numeric cashliquidity,
(mi.jsondoc_->>'CashLiquidity2')::numeric cashliquidity2,
(mi.jsondoc_->>'CashLiquidity3')::numeric cashliquidity3,
(mi.jsondoc_->>'CashToCurrentLiabilities')::numeric cashtocurrentliabilities,
(mi.jsondoc_->>'CashToCurrentLiabilities2')::numeric cashtocurrentliabilities2,
(mi.jsondoc_->>'CashToCurrentLiabilities3')::numeric cashtocurrentliabilities3,
(mi.jsondoc_->>'CompanyName')::varchar companyname,
(mi.jsondoc_->>'CompetitionLevel')::int4 competitionlevel,
l3.jsondoc_->>'Value' competitionlevelval,
(mi.jsondoc_->>'ConstructionContracts')::numeric constructioncontracts,
(mi.jsondoc_->>'CoopYearsAlpha')::int4 coopyearsalpha,
(mi.jsondoc_->>'Costsnxcellfillings')::numeric costsnxcellfillings,
(mi.jsondoc_->>'CurrentRatio')::numeric currentratio,
(mi.jsondoc_->>'CurrentRatio2')::numeric currentratio2,
(mi.jsondoc_->>'CurrentRatio3')::numeric currentratio3,
(mi.jsondoc_->>'CustomerProgress')::int4 customerprogress,
l4.jsondoc_->>'Value' customerprogressval,
(mi.jsondoc_->>'DebtCoverageRatio')::numeric debtcoverageratio,
(mi.jsondoc_->>'DebtCoverageRatio2')::numeric debtcoverageratio2,
(mi.jsondoc_->>'DebtCoverageRatio3')::numeric debtcoverageratio3,
(mi.jsondoc_->>'DebtToEbitda')::numeric debttoebitda,
(mi.jsondoc_->>'DebtToEbitda2')::numeric debttoebitda2,
(mi.jsondoc_->>'DebtToEbitda3')::numeric debttoebitda3,
(mi.jsondoc_->>'DebtToSales')::numeric debttosales,
(mi.jsondoc_->>'DebtToSales2')::numeric debttosales2,
(mi.jsondoc_->>'DebtToSales3')::numeric debttosales3,
(mi.jsondoc_->>'DelinqPaidYc1')::numeric delinqpaidyc1,
(mi.jsondoc_->>'DelinqPaidYc2')::numeric delinqpaidyc2,
(mi.jsondoc_->>'DelinqPaidYc3')::numeric delinqpaidyc3,
(mi.jsondoc_->>'DelinqPaidYc4')::numeric delinqpaidyc4,
(mi.jsondoc_->>'DelinquenciesRatio')::numeric delinquenciesratio,
(mi.jsondoc_->>'DelinquenciesRatio2')::numeric delinquenciesratio2,
(mi.jsondoc_->>'DelinquenciesRatio3')::numeric delinquenciesratio3,
(mi.jsondoc_->>'DeprecImpairment')::numeric deprecimpairment,
(mi.jsondoc_->>'Distribution')::int4 distribution,
(mi.jsondoc_->>'DividendsPayable')::numeric dividendspayable,
(mi.jsondoc_->>'Ebtda')::numeric ebtda,
(mi.jsondoc_->>'EbtdaToDebt')::numeric ebtdatodebt,
(mi.jsondoc_->>'EbtdaToDebt2')::numeric ebtdatodebt2,
(mi.jsondoc_->>'EbtdaToDebt3')::numeric ebtdatodebt3,
(mi.jsondoc_->>'EbtdaToSales')::numeric ebtdatosales,
(mi.jsondoc_->>'EbtdaToSales2')::numeric ebtdatosales2,
(mi.jsondoc_->>'EbtdaToSales3')::numeric ebtdatosales3,
(mi.jsondoc_->>'Exports')::boolean exports,
(mi.jsondoc_->>'FcashtoCurrentLiabilities')::numeric fcashtocurrentliabilities,
(mi.jsondoc_->>'FdaysInSuppliers')::numeric fdaysinsuppliers,
(mi.jsondoc_->>'FdelinquenciesRatio')::numeric fdelinquenciesratio,
(mi.jsondoc_->>'FebtdatoDebt')::numeric febtdatodebt,
(mi.jsondoc_->>'FfebtToSales')::numeric ffebttosales,
(mi.jsondoc_->>'FfinancialLeverage')::numeric ffinancialleverage,
(mi.jsondoc_->>'FinabilityToPayInterest')::numeric finabilitytopayinterest,
(mi.jsondoc_->>'FinancialLeverage')::numeric financialleverage,
(mi.jsondoc_->>'FinancialLeverage2')::numeric financialleverage2,
(mi.jsondoc_->>'FinancialLeverage3')::numeric financialleverage3,
(mi.jsondoc_->>'FinancialStatementsAreAudited')::int4 financialstatementsareaudited,
l5.jsondoc_->>'Value' financialstatementsareauditedval,
(mi.jsondoc_->>'FmanagerialSkills')::numeric fmanagerialskills,
(mi.jsondoc_->>'Fmarketing')::numeric fmarketing,
(mi.jsondoc_->>'FownershipDel')::numeric fownershipdel,
(mi.jsondoc_->>'FpastDuesToFinancing')::numeric fpastduestofinancing,
(mi.jsondoc_->>'FreeFlowsToSales')::numeric freeflowstosales,
(mi.jsondoc_->>'FreeFlowsToSales2')::numeric freeflowstosales2,
(mi.jsondoc_->>'FreeFlowsToSales3')::numeric freeflowstosales3,
(mi.jsondoc_->>'Fsize')::numeric fsize,
(mi.jsondoc_->>'FtrendRank')::numeric ftrendrank,
(mi.jsondoc_->>'FworkingCapitalToSales')::numeric fworkingcapitaltosales,
(mi.jsondoc_->>'GeographicalCoverage')::int4 geographicalcoverage,
l6.jsondoc_->>'Value' geographicalcoverageval,
(mi.jsondoc_->>'GrossProfitMargin')::numeric grossprofitmargin,
(mi.jsondoc_->>'GrossProfitMargin2')::numeric grossprofitmargin2,
(mi.jsondoc_->>'GrossProfitMargin3')::numeric grossprofitmargin3,
(mi.jsondoc_->>'InabilityToPayInter')::int4 inabilitytopayinter,
l7.jsondoc_->>'Value' inabilitytopayinterval,
(mi.jsondoc_->>'IndicativeRatingWithTempData')::int4 indicativeratingwithtempdata,
l8.jsondoc_->>'Value' indicativeratingwithtempdataval,
(mi.jsondoc_->>'InterestCoverage')::numeric interestcoverage,
(mi.jsondoc_->>'InterestCoverage2')::numeric interestcoverage2,
(mi.jsondoc_->>'InterestCoverage3')::numeric interestcoverage3,
(mi.jsondoc_->>'InterestIncome')::numeric interestincome,
(mi.jsondoc_->>'InventoryDays')::numeric inventorydays,
(mi.jsondoc_->>'InventoryDays2')::numeric inventorydays2,
(mi.jsondoc_->>'InventoryDays3')::numeric inventorydays3,
(mi.jsondoc_->>'InvestementsInPropertyCp')::numeric investementsinpropertycp,
(mi.jsondoc_->>'MachineryAndEquipment')::int4 machineryandequipment,
(mi.jsondoc_->>'MachineryEquipmentAge')::int4 machineryequipmentage,
(mi.jsondoc_->>'Management')::int4 management,
(mi.jsondoc_->>'ManagerAbility')::int4 managerability,
l9.jsondoc_->>'Value' managerabilityval,
(mi.jsondoc_->>'MarketingLevel')::int4 marketinglevel,
l10.jsondoc_->>'Value' marketinglevelval,
(mi.jsondoc_->>'MasterId')::varchar masterid,
(mi.jsondoc_->>'MdelinquenciesRatio')::numeric mdelinquenciesratio,
(mi.jsondoc_->>'Miss')::int4 miss,
l11.jsondoc_->>'Value' missval,
(mi.jsondoc_->>'MmanagerialSkills')::varchar mmanagerialskills,
(mi.jsondoc_->>'Mmarketing')::varchar mmarketing,
(mi.jsondoc_->>'MtrendRank')::varchar mtrendrank,
(mi.jsondoc_->>'NetMarginToReserves')::numeric netmargintoreserves,
(mi.jsondoc_->>'NetMarginToReserves2')::numeric netmargintoreserves2,
(mi.jsondoc_->>'NetMarginToReserves3')::numeric netmargintoreserves3,
(mi.jsondoc_->>'NetMarginToTotalAssets')::numeric netmargintototalassets,
(mi.jsondoc_->>'NetMarginToTotalAssets2')::numeric netmargintototalassets2,
(mi.jsondoc_->>'NetMarginToTotalAssets3')::numeric netmargintototalassets3,
(mi.jsondoc_->>'NumberOfProducts')::int4 numberofproducts,
(mi.jsondoc_->>'OperatingFlows')::numeric operatingflows,
(mi.jsondoc_->>'OperatingYears')::int4 operatingyears,
(mi.jsondoc_->>'OtherCurrentAssets')::numeric othercurrentassets,
(mi.jsondoc_->>'OtherCurrentLiabilities')::numeric othercurrentliabilities,
(mi.jsondoc_->>'OtherDebtors')::numeric otherdebtors,
(mi.jsondoc_->>'OwnershipDeliq')::int4 ownershipdeliq,
l12.jsondoc_->>'Value' ownershipdeliqval,
(mi.jsondoc_->>'PastDuesToFinancing')::numeric pastduestofinancing,
(mi.jsondoc_->>'PastDuesToFinancing2')::numeric pastduestofinancing2,
(mi.jsondoc_->>'PastDuesToFinancing3')::numeric pastduestofinancing3,
(mi.jsondoc_->>'PrepaymentsByCustomers')::numeric prepaymentsbycustomers,
(mi.jsondoc_->>'PrepaymentsCp')::numeric prepaymentscp,
(mi.jsondoc_->>'Production')::int4 production,
(mi.jsondoc_->>'ProductsAndServices')::int4 productsandservices,
l13.jsondoc_->>'Value' productsandservicesval,
(mi.jsondoc_->>'ProgramMis')::varchar programmis,
l14.jsondoc_->>'Value' programmisval,
(mi.jsondoc_->>'QuickRatio')::numeric quickratio,
(mi.jsondoc_->>'QuickRatio2')::numeric quickratio2,
(mi.jsondoc_->>'QuickRatio3')::numeric quickratio3,
(mi.jsondoc_->>'Receivables')::numeric receivables,
(mi.jsondoc_->>'Receivables2')::numeric receivables2,
(mi.jsondoc_->>'Receivables3')::numeric receivables3,
(mi.jsondoc_->>'ReceivablesToSales')::numeric receivablestosales,
(mi.jsondoc_->>'ReceivablesToSales2')::numeric receivablestosales2,
(mi.jsondoc_->>'ReceivablesToSales3')::numeric receivablestosales3,
(mi.jsondoc_->>'Sales')::numeric sales,
(mi.jsondoc_->>'Sales2')::numeric sales2,
(mi.jsondoc_->>'Sales3')::numeric sales3,
(mi.jsondoc_->>'SalesGrowth')::numeric salesgrowth,
(mi.jsondoc_->>'SalesGrowth2')::numeric salesgrowth2,
(mi.jsondoc_->>'SalesGrowth3')::numeric salesgrowth3,
(mi.jsondoc_->>'SalesToAssets')::numeric salestoassets,
(mi.jsondoc_->>'SalesToAssets2')::numeric salestoassets2,
(mi.jsondoc_->>'SalesToAssets3')::numeric salestoassets3,
(mi.jsondoc_->>'SectorProgress')::int4 sectorprogress,
l15.jsondoc_->>'Value' sectorprogressval,
(mi.jsondoc_->>'SuccesionStatus')::int4 succesionstatus,
l16.jsondoc_->>'Value' succesionstatusval,
(mi.jsondoc_->>'Taxes')::numeric taxes,
(mi.jsondoc_->>'TbScore')::varchar tbscore,
(mi.jsondoc_->>'TbScoreDate')::date tbscoredate,
(mi.jsondoc_->>'TbScoreDateTeiresias')::varchar tbscoredateteiresias,
(mi.jsondoc_->>'TechnologyLevel')::int4 technologylevel,
l17.jsondoc_->>'Value' technologylevelval,
(mi.jsondoc_->>'Token')::varchar token,
(mi.jsondoc_->>'Total')::bigint total,
(mi.jsondoc_->>'TotalDelinqYc1')::numeric totaldelinqyc1,
(mi.jsondoc_->>'TotalDelinqYc2')::numeric totaldelinqyc2,
(mi.jsondoc_->>'TotalDelinqYc3')::numeric totaldelinqyc3,
(mi.jsondoc_->>'TotalDelinqYc4')::numeric totaldelinqyc4,
(mi.jsondoc_->>'TotalFixedAssets')::numeric totalfixedassets,
(mi.jsondoc_->>'TotalLiabilitiesToNetWorth')::numeric totalliabilitiestonetworth,
(mi.jsondoc_->>'TotalLiabilitiesToNetWorth2')::numeric totalliabilitiestonetworth2,
(mi.jsondoc_->>'TotalLiabilitiesToNetWorth3')::numeric totalliabilitiestonetworth3,
(mi.jsondoc_->>'TransactionId')::bigint transactionid,
(mi.jsondoc_->>'UniDividendsPayable')::numeric unidividendspayable,
(mi.jsondoc_->>'WillinglessToDiscloseInform')::int4 willinglesstodiscloseinform,
l18.jsondoc_->>'Value' willinglesstodiscloseinformval,
(mi.jsondoc_->>'WorkingCapital')::numeric workingcapital,
(mi.jsondoc_->>'WorkingCapital2')::numeric workingcapital2,
(mi.jsondoc_->>'WorkingCapital3')::numeric workingcapital3,
(mi.jsondoc_->>'WorkingCapitalToSales')::numeric workingcapitaltosales,
(mi.jsondoc_->>'WorkingCapitalToSales2')::numeric workingcapitaltosales2,
(mi.jsondoc_->>'WorkingCapitalToSales3')::numeric workingcapitaltosales3,
(mi.jsondoc_->>'OrganizationChart')::int4 organizationchart,
l19.jsondoc_->>'Value' organizationchartval,
mi.wfid_::varchar ,
mi.taskid_::varchar ,
mi.versionid_::int4 ,
mi.isdeleted_::boolean ,
mi.islatestversion_::boolean ,
mi.baseversionid_::int4 ,
mi.contextuserid_::varchar ,
mi.isvisible_::boolean ,
mi.isvalid_::boolean ,
mi.snapshotid_::int4 ,
mi.t_::varchar ,
mi.createdby_::varchar ,
mi.createddate_::timestamp ,
mi.updatedby_::varchar ,
mi.updateddate_::timestamp ,
mi.fkid_entity,
(case when mi.updateddate_>mi.createddate_ then mi.updatedby_ else mi.createdby_ end) as sourcepopulatedby_,
GREATEST(mi.updateddate_,mi.createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.mapinstance mi
left join madata.custom_lookup l1
on l1.t_ = 'BusinessActivity'
and l1.jsondoc_->>'Key' = mi.jsondoc_ ->> 'ActivityOfBusiness'
left join madata.custom_lookup l2
on l2.t_ = 'BuildingCondition'
and l2.jsondoc_->>'Key' = mi.jsondoc_ ->> 'BuildingCondition'
left join madata.custom_lookup l3
on l3.t_ = 'CompetitionCmodel'
and l3.jsondoc_->>'Key' = mi.jsondoc_ ->> 'CompetitionLevel'
left join madata.custom_lookup l4
on l4.t_ = 'Customer'
and l4.jsondoc_->>'Key' = mi.jsondoc_ ->> 'CustomerProgress'
left join madata.custom_lookup l5
on l5.t_ = 'TrueFalse'
and l5.jsondoc_->>'Key' = mi.jsondoc_ ->> 'FinancialStatementsAreAudited'
left join madata.custom_lookup l6
on l6.t_ = 'GeographicalCoverage'
and l6.jsondoc_->>'Key' = mi.jsondoc_ ->> 'GeographicalCoverage'
left join madata.custom_lookup l7
on l7.t_ = 'TrueFalse'
and l7.jsondoc_->>'Key' = mi.jsondoc_ ->> 'InabilityToPayInter'
left join madata.custom_lookup l8
on l8.t_ = 'TrueFalse'
and l8.jsondoc_->>'Key' = mi.jsondoc_ ->> 'IndicativeRatingWithTempData'
left join madata.custom_lookup l9
on l9.t_ = 'ManagerCapability'
and l9.jsondoc_->>'Key' = mi.jsondoc_ ->> 'ManagerAbility'
left join madata.custom_lookup l10
on l10.t_ = 'Marketing'
and l10.jsondoc_->>'Key' = mi.jsondoc_ ->> 'MarketingLevel'
left join madata.custom_lookup l11
on l11.t_ = 'TrueFalse'
and l11.jsondoc_->>'Key' = mi.jsondoc_ ->> 'Miss'
left join madata.custom_lookup l12
on l12.t_ = 'TrueFalse'
and l12.jsondoc_->>'Key' = mi.jsondoc_ ->> 'OwnershipDeliq'
left join madata.custom_lookup l13
on l13.t_ = 'ProductsAndServices'
and l13.jsondoc_->>'Key' = mi.jsondoc_ ->> 'ProductsAndServices'
left join madata.custom_lookup l14
on l14.t_ = 'YesNo'
and l14.jsondoc_->>'Key' = mi.jsondoc_ ->> 'ProgramMis'
left join madata.custom_lookup l15
on l15.t_ = 'Sector'
and l15.jsondoc_->>'Key' = mi.jsondoc_ ->> 'SectorProgress'
left join madata.custom_lookup l16
on l16.t_ = 'Succession'
and l16.jsondoc_->>'Key' = mi.jsondoc_ ->> 'SuccesionStatus'
left join madata.custom_lookup l17
on l17.t_ = 'Technology'
and l17.jsondoc_->>'Key' = mi.jsondoc_ ->> 'TechnologyLevel'
left join madata.custom_lookup l18
on l18.t_ = 'TrueFalse'
and l18.jsondoc_->>'Key' = mi.jsondoc_ ->> 'WillinglessToDiscloseInform'
left join madata.custom_lookup l19
on l19.t_ = 'TrueFalse'
and l19.jsondoc_->>'Key' = mi.jsondoc_ ->> 'OrganizationChart'
where GREATEST(mi.updateddate_,mi.createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABPDMODELCCATEGORY')
and GREATEST(mi.updateddate_,mi.createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
and mi.t_ = 'PdModelCcategory'
;

raise notice '% - Step abpdmodelccategory - part a end', clock_timestamp();
ELSE
raise notice '% - Step abpdmodelccategory - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abpdmodelccategory;
CREATE TABLE olapts.abpdmodelccategory AS
SELECT 
mi.id_ factmappdinstanceid_,
mi.pkid_::varchar as pkid_,
(mi.jsondoc_->>'AccountReceivablesDays')::numeric accountreceivablesdays,
(mi.jsondoc_->>'AccountReceivablesDays2')::numeric accountreceivablesdays2,
(mi.jsondoc_->>'AccountReceivablesDays3')::numeric accountreceivablesdays3,
(mi.jsondoc_->>'AccountsPayableDays')::numeric accountspayabledays,
(mi.jsondoc_->>'AccountsPayableDays2')::numeric accountspayabledays2,
(mi.jsondoc_->>'AccountsPayableDays3')::numeric accountspayabledays3,
(mi.jsondoc_->>'ActivityOfBusiness')::int4 activityofbusiness,
l1.jsondoc_->>'Value' activityofbusinessval,
(mi.jsondoc_->>'Afm')::varchar afm,
(mi.jsondoc_->>'BuildingCondition')::int4 buildingcondition,
l2.jsondoc_->>'Value' buildingconditionval,
(mi.jsondoc_->>'CashLiquidity')::numeric cashliquidity,
(mi.jsondoc_->>'CashLiquidity2')::numeric cashliquidity2,
(mi.jsondoc_->>'CashLiquidity3')::numeric cashliquidity3,
(mi.jsondoc_->>'CashToCurrentLiabilities')::numeric cashtocurrentliabilities,
(mi.jsondoc_->>'CashToCurrentLiabilities2')::numeric cashtocurrentliabilities2,
(mi.jsondoc_->>'CashToCurrentLiabilities3')::numeric cashtocurrentliabilities3,
(mi.jsondoc_->>'CompanyName')::varchar companyname,
(mi.jsondoc_->>'CompetitionLevel')::int4 competitionlevel,
l3.jsondoc_->>'Value' competitionlevelval,
(mi.jsondoc_->>'ConstructionContracts')::numeric constructioncontracts,
(mi.jsondoc_->>'CoopYearsAlpha')::int4 coopyearsalpha,
(mi.jsondoc_->>'Costsnxcellfillings')::numeric costsnxcellfillings,
(mi.jsondoc_->>'CurrentRatio')::numeric currentratio,
(mi.jsondoc_->>'CurrentRatio2')::numeric currentratio2,
(mi.jsondoc_->>'CurrentRatio3')::numeric currentratio3,
(mi.jsondoc_->>'CustomerProgress')::int4 customerprogress,
l4.jsondoc_->>'Value' customerprogressval,
(mi.jsondoc_->>'DebtCoverageRatio')::numeric debtcoverageratio,
(mi.jsondoc_->>'DebtCoverageRatio2')::numeric debtcoverageratio2,
(mi.jsondoc_->>'DebtCoverageRatio3')::numeric debtcoverageratio3,
(mi.jsondoc_->>'DebtToEbitda')::numeric debttoebitda,
(mi.jsondoc_->>'DebtToEbitda2')::numeric debttoebitda2,
(mi.jsondoc_->>'DebtToEbitda3')::numeric debttoebitda3,
(mi.jsondoc_->>'DebtToSales')::numeric debttosales,
(mi.jsondoc_->>'DebtToSales2')::numeric debttosales2,
(mi.jsondoc_->>'DebtToSales3')::numeric debttosales3,
(mi.jsondoc_->>'DelinqPaidYc1')::numeric delinqpaidyc1,
(mi.jsondoc_->>'DelinqPaidYc2')::numeric delinqpaidyc2,
(mi.jsondoc_->>'DelinqPaidYc3')::numeric delinqpaidyc3,
(mi.jsondoc_->>'DelinqPaidYc4')::numeric delinqpaidyc4,
(mi.jsondoc_->>'DelinquenciesRatio')::numeric delinquenciesratio,
(mi.jsondoc_->>'DelinquenciesRatio2')::numeric delinquenciesratio2,
(mi.jsondoc_->>'DelinquenciesRatio3')::numeric delinquenciesratio3,
(mi.jsondoc_->>'DeprecImpairment')::numeric deprecimpairment,
(mi.jsondoc_->>'Distribution')::int4 distribution,
(mi.jsondoc_->>'DividendsPayable')::numeric dividendspayable,
(mi.jsondoc_->>'Ebtda')::numeric ebtda,
(mi.jsondoc_->>'EbtdaToDebt')::numeric ebtdatodebt,
(mi.jsondoc_->>'EbtdaToDebt2')::numeric ebtdatodebt2,
(mi.jsondoc_->>'EbtdaToDebt3')::numeric ebtdatodebt3,
(mi.jsondoc_->>'EbtdaToSales')::numeric ebtdatosales,
(mi.jsondoc_->>'EbtdaToSales2')::numeric ebtdatosales2,
(mi.jsondoc_->>'EbtdaToSales3')::numeric ebtdatosales3,
(mi.jsondoc_->>'Exports')::boolean exports,
(mi.jsondoc_->>'FcashtoCurrentLiabilities')::numeric fcashtocurrentliabilities,
(mi.jsondoc_->>'FdaysInSuppliers')::numeric fdaysinsuppliers,
(mi.jsondoc_->>'FdelinquenciesRatio')::numeric fdelinquenciesratio,
(mi.jsondoc_->>'FebtdatoDebt')::numeric febtdatodebt,
(mi.jsondoc_->>'FfebtToSales')::numeric ffebttosales,
(mi.jsondoc_->>'FfinancialLeverage')::numeric ffinancialleverage,
(mi.jsondoc_->>'FinabilityToPayInterest')::numeric finabilitytopayinterest,
(mi.jsondoc_->>'FinancialLeverage')::numeric financialleverage,
(mi.jsondoc_->>'FinancialLeverage2')::numeric financialleverage2,
(mi.jsondoc_->>'FinancialLeverage3')::numeric financialleverage3,
(mi.jsondoc_->>'FinancialStatementsAreAudited')::int4 financialstatementsareaudited,
l5.jsondoc_->>'Value' financialstatementsareauditedval,
(mi.jsondoc_->>'FmanagerialSkills')::numeric fmanagerialskills,
(mi.jsondoc_->>'Fmarketing')::numeric fmarketing,
(mi.jsondoc_->>'FownershipDel')::numeric fownershipdel,
(mi.jsondoc_->>'FpastDuesToFinancing')::numeric fpastduestofinancing,
(mi.jsondoc_->>'FreeFlowsToSales')::numeric freeflowstosales,
(mi.jsondoc_->>'FreeFlowsToSales2')::numeric freeflowstosales2,
(mi.jsondoc_->>'FreeFlowsToSales3')::numeric freeflowstosales3,
(mi.jsondoc_->>'Fsize')::numeric fsize,
(mi.jsondoc_->>'FtrendRank')::numeric ftrendrank,
(mi.jsondoc_->>'FworkingCapitalToSales')::numeric fworkingcapitaltosales,
(mi.jsondoc_->>'GeographicalCoverage')::int4 geographicalcoverage,
l6.jsondoc_->>'Value' geographicalcoverageval,
(mi.jsondoc_->>'GrossProfitMargin')::numeric grossprofitmargin,
(mi.jsondoc_->>'GrossProfitMargin2')::numeric grossprofitmargin2,
(mi.jsondoc_->>'GrossProfitMargin3')::numeric grossprofitmargin3,
(mi.jsondoc_->>'InabilityToPayInter')::int4 inabilitytopayinter,
l7.jsondoc_->>'Value' inabilitytopayinterval,
(mi.jsondoc_->>'IndicativeRatingWithTempData')::int4 indicativeratingwithtempdata,
l8.jsondoc_->>'Value' indicativeratingwithtempdataval,
(mi.jsondoc_->>'InterestCoverage')::numeric interestcoverage,
(mi.jsondoc_->>'InterestCoverage2')::numeric interestcoverage2,
(mi.jsondoc_->>'InterestCoverage3')::numeric interestcoverage3,
(mi.jsondoc_->>'InterestIncome')::numeric interestincome,
(mi.jsondoc_->>'InventoryDays')::numeric inventorydays,
(mi.jsondoc_->>'InventoryDays2')::numeric inventorydays2,
(mi.jsondoc_->>'InventoryDays3')::numeric inventorydays3,
(mi.jsondoc_->>'InvestementsInPropertyCp')::numeric investementsinpropertycp,
(mi.jsondoc_->>'MachineryAndEquipment')::int4 machineryandequipment,
(mi.jsondoc_->>'MachineryEquipmentAge')::int4 machineryequipmentage,
(mi.jsondoc_->>'Management')::int4 management,
(mi.jsondoc_->>'ManagerAbility')::int4 managerability,
l9.jsondoc_->>'Value' managerabilityval,
(mi.jsondoc_->>'MarketingLevel')::int4 marketinglevel,
l10.jsondoc_->>'Value' marketinglevelval,
(mi.jsondoc_->>'MasterId')::varchar masterid,
(mi.jsondoc_->>'MdelinquenciesRatio')::numeric mdelinquenciesratio,
(mi.jsondoc_->>'Miss')::int4 miss,
l11.jsondoc_->>'Value' missval,
(mi.jsondoc_->>'MmanagerialSkills')::varchar mmanagerialskills,
(mi.jsondoc_->>'Mmarketing')::varchar mmarketing,
(mi.jsondoc_->>'MtrendRank')::varchar mtrendrank,
(mi.jsondoc_->>'NetMarginToReserves')::numeric netmargintoreserves,
(mi.jsondoc_->>'NetMarginToReserves2')::numeric netmargintoreserves2,
(mi.jsondoc_->>'NetMarginToReserves3')::numeric netmargintoreserves3,
(mi.jsondoc_->>'NetMarginToTotalAssets')::numeric netmargintototalassets,
(mi.jsondoc_->>'NetMarginToTotalAssets2')::numeric netmargintototalassets2,
(mi.jsondoc_->>'NetMarginToTotalAssets3')::numeric netmargintototalassets3,
(mi.jsondoc_->>'NumberOfProducts')::int4 numberofproducts,
(mi.jsondoc_->>'OperatingFlows')::numeric operatingflows,
(mi.jsondoc_->>'OperatingYears')::int4 operatingyears,
(mi.jsondoc_->>'OtherCurrentAssets')::numeric othercurrentassets,
(mi.jsondoc_->>'OtherCurrentLiabilities')::numeric othercurrentliabilities,
(mi.jsondoc_->>'OtherDebtors')::numeric otherdebtors,
(mi.jsondoc_->>'OwnershipDeliq')::int4 ownershipdeliq,
l12.jsondoc_->>'Value' ownershipdeliqval,
(mi.jsondoc_->>'PastDuesToFinancing')::numeric pastduestofinancing,
(mi.jsondoc_->>'PastDuesToFinancing2')::numeric pastduestofinancing2,
(mi.jsondoc_->>'PastDuesToFinancing3')::numeric pastduestofinancing3,
(mi.jsondoc_->>'PrepaymentsByCustomers')::numeric prepaymentsbycustomers,
(mi.jsondoc_->>'PrepaymentsCp')::numeric prepaymentscp,
(mi.jsondoc_->>'Production')::int4 production,
(mi.jsondoc_->>'ProductsAndServices')::int4 productsandservices,
l13.jsondoc_->>'Value' productsandservicesval,
(mi.jsondoc_->>'ProgramMis')::varchar programmis,
l14.jsondoc_->>'Value' programmisval,
(mi.jsondoc_->>'QuickRatio')::numeric quickratio,
(mi.jsondoc_->>'QuickRatio2')::numeric quickratio2,
(mi.jsondoc_->>'QuickRatio3')::numeric quickratio3,
(mi.jsondoc_->>'Receivables')::numeric receivables,
(mi.jsondoc_->>'Receivables2')::numeric receivables2,
(mi.jsondoc_->>'Receivables3')::numeric receivables3,
(mi.jsondoc_->>'ReceivablesToSales')::numeric receivablestosales,
(mi.jsondoc_->>'ReceivablesToSales2')::numeric receivablestosales2,
(mi.jsondoc_->>'ReceivablesToSales3')::numeric receivablestosales3,
(mi.jsondoc_->>'Sales')::numeric sales,
(mi.jsondoc_->>'Sales2')::numeric sales2,
(mi.jsondoc_->>'Sales3')::numeric sales3,
(mi.jsondoc_->>'SalesGrowth')::numeric salesgrowth,
(mi.jsondoc_->>'SalesGrowth2')::numeric salesgrowth2,
(mi.jsondoc_->>'SalesGrowth3')::numeric salesgrowth3,
(mi.jsondoc_->>'SalesToAssets')::numeric salestoassets,
(mi.jsondoc_->>'SalesToAssets2')::numeric salestoassets2,
(mi.jsondoc_->>'SalesToAssets3')::numeric salestoassets3,
(mi.jsondoc_->>'SectorProgress')::int4 sectorprogress,
l15.jsondoc_->>'Value' sectorprogressval,
(mi.jsondoc_->>'SuccesionStatus')::int4 succesionstatus,
l16.jsondoc_->>'Value' succesionstatusval,
(mi.jsondoc_->>'Taxes')::numeric taxes,
(mi.jsondoc_->>'TbScore')::varchar tbscore,
(mi.jsondoc_->>'TbScoreDate')::date tbscoredate,
(mi.jsondoc_->>'TbScoreDateTeiresias')::varchar tbscoredateteiresias,
(mi.jsondoc_->>'TechnologyLevel')::int4 technologylevel,
l17.jsondoc_->>'Value' technologylevelval,
(mi.jsondoc_->>'Token')::varchar token,
(mi.jsondoc_->>'Total')::bigint total,
(mi.jsondoc_->>'TotalDelinqYc1')::numeric totaldelinqyc1,
(mi.jsondoc_->>'TotalDelinqYc2')::numeric totaldelinqyc2,
(mi.jsondoc_->>'TotalDelinqYc3')::numeric totaldelinqyc3,
(mi.jsondoc_->>'TotalDelinqYc4')::numeric totaldelinqyc4,
(mi.jsondoc_->>'TotalFixedAssets')::numeric totalfixedassets,
(mi.jsondoc_->>'TotalLiabilitiesToNetWorth')::numeric totalliabilitiestonetworth,
(mi.jsondoc_->>'TotalLiabilitiesToNetWorth2')::numeric totalliabilitiestonetworth2,
(mi.jsondoc_->>'TotalLiabilitiesToNetWorth3')::numeric totalliabilitiestonetworth3,
(mi.jsondoc_->>'TransactionId')::bigint transactionid,
(mi.jsondoc_->>'UniDividendsPayable')::numeric unidividendspayable,
(mi.jsondoc_->>'WillinglessToDiscloseInform')::int4 willinglesstodiscloseinform,
l18.jsondoc_->>'Value' willinglesstodiscloseinformval,
(mi.jsondoc_->>'WorkingCapital')::numeric workingcapital,
(mi.jsondoc_->>'WorkingCapital2')::numeric workingcapital2,
(mi.jsondoc_->>'WorkingCapital3')::numeric workingcapital3,
(mi.jsondoc_->>'WorkingCapitalToSales')::numeric workingcapitaltosales,
(mi.jsondoc_->>'WorkingCapitalToSales2')::numeric workingcapitaltosales2,
(mi.jsondoc_->>'WorkingCapitalToSales3')::numeric workingcapitaltosales3,
(mi.jsondoc_->>'OrganizationChart')::int4 organizationchart,
l19.jsondoc_->>'Value' organizationchartval,
mi.wfid_::varchar ,
mi.taskid_::varchar ,
mi.versionid_::int4 ,
mi.isdeleted_::boolean ,
mi.islatestversion_::boolean ,
mi.baseversionid_::int4 ,
mi.contextuserid_::varchar ,
mi.isvisible_::boolean ,
mi.isvalid_::boolean ,
mi.snapshotid_::int4 ,
mi.t_::varchar ,
mi.createdby_::varchar ,
mi.createddate_::timestamp ,
mi.updatedby_::varchar ,
mi.updateddate_::timestamp ,
mi.fkid_entity,
(case when mi.updateddate_>mi.createddate_ then mi.updatedby_ else mi.createdby_ end) as sourcepopulatedby_,
GREATEST(mi.updateddate_,mi.createddate_) as sourcepopulateddate_ ,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.mapinstance mi
left join madata.custom_lookup l1
on l1.t_ = 'BusinessActivity'
and l1.jsondoc_->>'Key' = mi.jsondoc_ ->> 'ActivityOfBusiness'
left join madata.custom_lookup l2
on l2.t_ = 'BuildingCondition'
and l2.jsondoc_->>'Key' = mi.jsondoc_ ->> 'BuildingCondition'
left join madata.custom_lookup l3
on l3.t_ = 'CompetitionCmodel'
and l3.jsondoc_->>'Key' = mi.jsondoc_ ->> 'CompetitionLevel'
left join madata.custom_lookup l4
on l4.t_ = 'Customer'
and l4.jsondoc_->>'Key' = mi.jsondoc_ ->> 'CustomerProgress'
left join madata.custom_lookup l5
on l5.t_ = 'TrueFalse'
and l5.jsondoc_->>'Key' = mi.jsondoc_ ->> 'FinancialStatementsAreAudited'
left join madata.custom_lookup l6
on l6.t_ = 'GeographicalCoverage'
and l6.jsondoc_->>'Key' = mi.jsondoc_ ->> 'GeographicalCoverage'
left join madata.custom_lookup l7
on l7.t_ = 'TrueFalse'
and l7.jsondoc_->>'Key' = mi.jsondoc_ ->> 'InabilityToPayInter'
left join madata.custom_lookup l8
on l8.t_ = 'TrueFalse'
and l8.jsondoc_->>'Key' = mi.jsondoc_ ->> 'IndicativeRatingWithTempData'
left join madata.custom_lookup l9
on l9.t_ = 'ManagerCapability'
and l9.jsondoc_->>'Key' = mi.jsondoc_ ->> 'ManagerAbility'
left join madata.custom_lookup l10
on l10.t_ = 'Marketing'
and l10.jsondoc_->>'Key' = mi.jsondoc_ ->> 'MarketingLevel'
left join madata.custom_lookup l11
on l11.t_ = 'TrueFalse'
and l11.jsondoc_->>'Key' = mi.jsondoc_ ->> 'Miss'
left join madata.custom_lookup l12
on l12.t_ = 'TrueFalse'
and l9.jsondoc_->>'Key' = mi.jsondoc_ ->> 'OwnershipDeliq'
left join madata.custom_lookup l13
on l13.t_ = 'ProductsAndServices'
and l13.jsondoc_->>'Key' = mi.jsondoc_ ->> 'ProductsAndServices'
left join madata.custom_lookup l14
on l14.t_ = 'YesNo'
and l14.jsondoc_->>'Key' = mi.jsondoc_ ->> 'ProgramMis'
left join madata.custom_lookup l15
on l15.t_ = 'Sector'
and l15.jsondoc_->>'Key' = mi.jsondoc_ ->> 'SectorProgress'
left join madata.custom_lookup l16
on l16.t_ = 'Succession'
and l16.jsondoc_->>'Key' = mi.jsondoc_ ->> 'SuccesionStatus'
left join madata.custom_lookup l17
on l17.t_ = 'Technology'
and l17.jsondoc_->>'Key' = mi.jsondoc_ ->> 'TechnologyLevel'
left join madata.custom_lookup l18
on l18.t_ = 'TrueFalse'
and l18.jsondoc_->>'Key' = mi.jsondoc_ ->> 'WillinglessToDiscloseInform'
left join madata.custom_lookup l19
on l19.t_ = 'TrueFalse'
and l19.jsondoc_->>'Key' = mi.jsondoc_ ->> 'OrganizationChart'
where GREATEST(mi.updateddate_,mi.createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABPDMODELCCATEGORY')
and GREATEST(mi.updateddate_,mi.createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
and mi.t_ = 'PdModelCcategory'
;

raise notice '% - Step abpdmodelccategory - part b end', clock_timestamp();

raise notice '% - Step abpdmodelccategory_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abpdmodelccategory_idx;
DROP INDEX if exists olapts.abpdmodelccategory_idx2;

CREATE INDEX IF NOT EXISTS abpdmodelccategory_idx ON olapts.abpdmodelccategory (factmappdinstanceid_,pkid_,wfid_);
CREATE INDEX IF NOT EXISTS abpdmodelccategory_idx ON olapts.abpdmodelccategory (factmappdinstanceid_,pkid_,wfid_);
CREATE INDEX IF NOT EXISTS abpdmodelccategory_idx_idx2 ON olapts.abpdmodelccategory (pkid_,versionid_,sourcepopulateddate_,wfid_) include(isvisible_,isvalid_,isdeleted_,islatestversion_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_);	

raise notice '% - Step abpdmodelccategory_idx - part a end', clock_timestamp(); 

END IF;

raise notice '% - Step abpdmodelccategory - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abpdmodelccategoryflag;
CREATE TABLE olapts.abpdmodelccategoryflag AS
select 
id_,
pkid_,
wfid_ wfid_ ,
taskid_ taskid_ ,
versionid_ versionid_ ,
isdeleted_::boolean isdeleted_ ,
islatestversion_::boolean islatestversion_ ,
baseversionid_ baseversionid_ ,
contextuserid_ contextuserid_ ,
isvisible_::boolean isvisible_ ,
isvalid_::boolean isvalid_ ,
snapshotid_ snapshotid_ ,
t_ t_ ,
createdby_ createdby_ ,
createddate_ createddate_ ,
updatedby_ updatedby_ ,
updateddate_ updateddate_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.mapinstance mi
where GREATEST(updateddate_,createddate_)::timestamp <=  current_setting('myvariables.popdate')::timestamp
and mi.t_ = 'PdModelCcategory'
;

raise notice '% - Step abpdmodelccategoryflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abpdmodelccategoryflag_idx;
DROP INDEX if exists olapts.abpdmodelccategoryflag_idx2;

CREATE INDEX IF NOT EXISTS abpdmodelccategoryflag_idx ON olapts.abpdmodelccategoryflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abpdmodelccategoryflag_idx2 ON olapts.abpdmodelccategoryflag (pkid_,versionid_,sourcepopulateddate_,wfid_);

raise notice '% - Step abpdmodelccategoryflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPDMODELCCATEGORY';
delete from olapts.refreshhistory where tablename = 'ABPDMODELCCATEGORY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPDMODELCCATEGORY' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPDMODELCCATEGORYFLAG';
delete from olapts.refreshhistory where tablename = 'ABPDMODELCCATEGORYFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPDMODELCCATEGORYFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

raise notice '% - Step abpdmodelccategory - part c end', clock_timestamp();

END $$;
-- End Custom MAP Models -  New Rating model 2: "Model C Category" (Id: PdModelCcategory)


--abrs_export_new
CREATE OR REPLACE VIEW olapts.abrs_export_new AS
 SELECT 
	abfactbanksystem."AA",
    abfactbanksystem."CustID",
    abfactbanksystem."Seq_NO",
    abfactbanksystem."AFM",
    abfactbanksystem."ShippingID",
    abfactbanksystem."CustomerName",
    abfactbanksystem."PeerCode",
    abfactbanksystem."Score",
    abfactbanksystem."Grade",
    abfactbanksystem."EquivalentPD",
    abfactbanksystem."OverrideGrade",
    abfactbanksystem."OverrideReason",
    abfactbanksystem."OverrideAuthorized",
    abfactbanksystem."OverrideAuthorizer",
    abfactbanksystem."OverridePD",
    abfactbanksystem."CustomerState",
    abfactbanksystem."ArchiveDate" at time zone 'utc' as "ArchiveDate",
    abfactbanksystem."EffectiveDate",
    abfactbanksystem."EffectiveTime" at time zone 'utc' as "EffectiveTime",
    abfactbanksystem."NextReviewdate",
    abfactbanksystem."RelationshipWithAlpha",
    abfactbanksystem."AM",
    abfactbanksystem."UserName",
    abfactbanksystem."UserProfile",
    abfactbanksystem."MFA_Model",
    abfactbanksystem."AccessGroup",
    abfactbanksystem."IRTModel",
    abfactbanksystem."RespOffice",
    abfactbanksystem."RespOfficer",
    abfactbanksystem."CreditCommittee",
    abfactbanksystem."Reviewtype",
    abfactbanksystem."GroupCode",
    abfactbanksystem."CDICode",
    abfactbanksystem."ContractNumber",
    abfactbanksystem."IndicationFinancialDiff1",
    abfactbanksystem."IndicationFinancialDiff2",
    abfactbanksystem."ActiveInactiveFlag"
   FROM olapts.ABFACTBANKSYSTEM;

--abratingscenarioblockdata feed
DO $$
DECLARE
varprevsuccessdate TIMESTAMP ;
BEGIN

IF EXISTS (select tablename from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATA') THEN
	raise notice '% - Step abratingscenarioblockdata - part a start', clock_timestamp(); 
	PERFORM olapts.abpopulate_ratingscenarioblockdata(false,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATA'),false);
	
	raise notice '% - Step abratingscenarioblockdata - part a end', clock_timestamp(); 
ELSE
	raise notice '% - Step abratingscenarioblockdata - part b start', clock_timestamp(); 
	DROP TABLE IF EXISTS olapts.ABRATINGSCENARIOBLOCKDATA;
	PERFORM olapts.abpopulate_ratingscenarioblockdata(true,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATA'),true);
	raise notice '% - Step abratingscenarioblockdata - part b end', clock_timestamp(); 
	
	--abratingscenarioblockdata
	raise notice '% - Step abratingscenarioblockdata_idx - part a start', clock_timestamp(); 
	
	DROP INDEX IF EXISTS olapts.abratingscenarioblockdata_idx;
	DROP INDEX IF EXISTS olapts.abratingscenarioblockdata_idx2;
	DROP INDEX IF EXISTS olapts.abratingscenarioblockdata_idx3;
	DROP INDEX IF EXISTS olapts.abratingscenarioblockdata_idx4;
	DROP INDEX IF EXISTS olapts.abratingscenarioblockdata_idx5;
	DROP INDEX IF EXISTS olapts.abratingscenarioblockdata_idx6;

	CREATE INDEX IF NOT EXISTS abratingscenarioblockdata_idx_block_gin ON olapts.abratingscenarioblockdata USING GIN (fkid_ratingscenario,blockid,versionid_,wfid_);
	CREATE INDEX IF NOT EXISTS abratingscenarioblockdata_idx_date_brin ON olapts.abratingscenarioblockdata USING BRIN (sourcepopulateddate_);
	CREATE INDEX IF NOT EXISTS abratingscenarioblockdata_idx_block_btree_ops ON olapts.abratingscenarioblockdata ((fkid_ratingscenario) varchar_pattern_ops,(blockid) text_pattern_ops,sourcepopulateddate_) include (versionid_,ioid,pinid,name,value,isdeleted_,isvalid_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);
	CREATE INDEX IF NOT EXISTS abratingscenarioblockdata_idx_block_btree ON olapts.abratingscenarioblockdata (substring(blockid,strpos(blockid,'.')+1,length(blockid)));
	CREATE INDEX abratingscenarioblockdata_idx_date_btree ON olapts.abratingscenarioblockdata (sourcepopulateddate_ desc) INCLUDE (v_ratingscenarioblockdataid_,fkid_ratingscenario,versionid_,wfid_);

	CREATE STATISTICS if not exists abratingscenarioblockdata_stat ON fkid_ratingscenario,blockid,ioid,name,sourcepopulateddate_,wfid_ FROM olapts.abratingscenarioblockdata;

	raise notice '% - Step abratingscenarioblockdata_idx - part a end', clock_timestamp(); 
	
END IF;

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATAFLAG') THEN
	raise notice '% - Step abratingscenarioblockdata - part c start', clock_timestamp(); 
	PERFORM olapts.abpopulate_ratingscenarioblockdataflag(false,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATAFLAG'),false);
	
	raise notice '% - Step abratingscenarioblockdata - part c end', clock_timestamp(); 
ELSE
	raise notice '% - Step abratingscenarioblockdata - part d start', clock_timestamp(); 
	DROP TABLE IF EXISTS olapts.ABRATINGSCENARIOBLOCKDATAFLAG;
	PERFORM olapts.abpopulate_ratingscenarioblockdataflag(true,current_setting('myvariables.popdate')::timestamp,(select COALESCE(max(asofdate),to_timestamp(0))::timestamp from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATAFLAG'),true);
	raise notice '% - Step abratingscenarioblockdata - part d end', clock_timestamp(); 
END IF;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATA';
delete from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATA';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRATINGSCENARIOBLOCKDATA' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATAFLAG';
delete from olapts.refreshhistory where tablename = 'ABRATINGSCENARIOBLOCKDATAFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRATINGSCENARIOBLOCKDATAFLAG' tablename, current_setting('myvariables.popdate')::timestamp as  asofdate,varprevsuccessdate;
raise notice '% - Step abratingscenarioblockdata - part c end', clock_timestamp(); 

--abratingscenarioblockdataflag
raise notice '% - Step abratingscenarioblockdataflag_idx - part a start', clock_timestamp(); 
	
DROP INDEX IF EXISTS olapts.abratingscenarioblockdataflag_idx;
DROP INDEX IF EXISTS olapts.abratingscenarioblockdataflag_idx2;

CREATE INDEX IF NOT EXISTS abratingscenarioblockdataflag_idx_gin ON olapts.abratingscenarioblockdataflag USING GIN (pkid_,fkid_ratingscenario,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS abratingscenarioblockdataflag_idx_pkid_hash ON olapts.abratingscenarioblockdataflag USING hash (pkid_);
CREATE INDEX IF NOT EXISTS abratingscenarioblockdataflag_idx_date_brin ON olapts.abratingscenarioblockdataflag USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS abratingscenarioblockdataflag_idx_btree ON olapts.abratingscenarioblockdataflag (fkid_ratingscenario,pkid_,versionid_,sourcepopulateddate_) include (isdeleted_,isvalid_,isvisible_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);
CREATE INDEX IF NOT EXISTS abratingscenarioblockdataflag_idx_date_btree ON olapts.abratingscenarioblockdataflag (sourcepopulateddate_ desc) INCLUDE (v_ratingscenarioblockdataid_,pkid_,versionid_,wfid_);

raise notice '% - Step abratingscenarioblockdataflag_idx - part a end', clock_timestamp(); 
	
END $$;

----legacy indexes start----

--factratingscenarioblockdatalatest
CREATE INDEX IF NOT EXISTS factratingscenarioblockdata_idx_block_gin ON olapts.factratingscenarioblockdatalatest USING GIN (fkid_ratingscenario,blockid,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS factratingscenarioblockdata_idx_date_brin ON olapts.factratingscenarioblockdatalatest USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS factratingscenarioblockdata_idx_block_btree_ops ON olapts.factratingscenarioblockdatalatest ((fkid_ratingscenario) varchar_pattern_ops,(blockid) text_pattern_ops,sourcepopulateddate_) include (versionid_,pinid,name,value,isdeleted_,isvalid_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);
CREATE INDEX IF NOT EXISTS factratingscenarioblockdata_idx_block_btree ON olapts.factratingscenarioblockdatalatest (substring(blockid,strpos(blockid,'.')+1,length(blockid)));
CREATE INDEX IF NOT EXISTS factratingscenarioblockdata_idx_date_btree ON olapts.factratingscenarioblockdatalatest (sourcepopulateddate_ desc) INCLUDE (fkid_ratingscenario,versionid_,wfid_);

CREATE STATISTICS if not exists factratingscenarioblockdata_stat ON fkid_ratingscenario,blockid,pinid,name,sourcepopulateddate_,wfid_ FROM olapts.factratingscenarioblockdatalatest;

--factratingscenario
CREATE INDEX IF NOT EXISTS factratingscenario_idx_date_brin ON olapts.factratingscenario USING BRIN (sourcepopulateddate_,wfid_);
CREATE INDEX IF NOT EXISTS factratingscenario_idx_pkid_hash ON olapts.factratingscenario USING hash (pkid_);
CREATE INDEX IF NOT EXISTS factratingscenario_idx_pkid_btree ON olapts.factratingscenario (pkid_,wfid_);
CREATE INDEX IF NOT EXISTS factratingscenario_idx_btree ON olapts.factratingscenario (pkid_,versionid_,sourcepopulateddate_) include(entityid,versionid_,selectedfinancialid,financialcontext,isprimary,islatestapprovedscenario,approvalstatus,parentid,isvalid_,isdeleted_,isvisible_,islatestversion_,wfid_);
CREATE INDEX IF NOT EXISTS factratingscenario_idx_pkid_v ON olapts.factratingscenario USING btree (pkid_,versionid_,wfid_);

CREATE STATISTICS if not exists factratingscenario_stat ON pkid_,entityid,sourcepopulateddate_,wfid_ FROM olapts.factratingscenario;

--factentity
CREATE INDEX IF NOT EXISTS factentity_idxdate_brin ON olapts.factentity USING BRIN (sourcepopulateddate_,wfid_);
CREATE INDEX IF NOT EXISTS factentity_idx_entity_hash ON olapts.factentity USING hash (entityid);
CREATE INDEX IF NOT EXISTS factentity_idx_pkid_hash ON olapts.factentity USING hash (pkid_);
CREATE INDEX IF NOT EXISTS factentity_idx_versionid_hash ON olapts.factentity USING hash (versionid_);
CREATE INDEX IF NOT EXISTS factentity_idx_btree ON olapts.factentity (pkid_,entityid,versionid_,sourcepopulateddate_) include(gc18,cdicode,systemid,isvalid_,isdeleted_,isvisible_,islatestversion_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);

--dimentityindustry
CREATE INDEX IF NOT EXISTS dimentityindustry_idx_gin ON olapts.dimentityindustry USING GIN (entityid,pkid_,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS dimentityindustry_idx_pkid_hash ON olapts.dimentityindustry USING hash (pkid_);
CREATE INDEX IF NOT EXISTS dimentityindustry_idx_date_brin ON olapts.dimentityindustry USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS dimentityindustry_idx_btree ON olapts.dimentityindustry (entityid,pkid_,ltrim(rtrim(replace( substring(industrycode,strpos(industrycode,': "')+1,( strpos(industrycode,'",') -strpos(industrycode,': "')-1 ) ),'"','') )),versionid_,sourcepopulateddate_) include (isdeleted_,isprimary,isvalid_,isvisible_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);

--dimpeeranalysis
CREATE INDEX IF NOT EXISTS dimpeeranalysis_idx_gin ON olapts.dimpeeranalysis USING GIN (dimpeeranalysisid_,pkid_,versionid_,sourcepopulateddate_,wfid_);
CREATE INDEX IF NOT EXISTS dimpeeranalysis_idx_dimpeeranalysisid_hash ON olapts.dimpeeranalysis USING hash (dimpeeranalysisid_);
CREATE INDEX IF NOT EXISTS dimpeeranalysis_idx_date_brin ON olapts.dimpeeranalysis USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS dimpeeranalysis_idx_btree ON olapts.dimpeeranalysis (dimpeeranalysisid_,pkid_,versionid_,sourcepopulateddate_) include (financialid,peerdatabaseid,peersic,isdeleted_,isvalid_,isvisible_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);

--dimpeeranalysislatest
CREATE INDEX IF NOT EXISTS dimpeeranalysislatest_idx_gin ON olapts.dimpeeranalysislatest USING GIN (dimpeeranalysislatestid_,pkid_,versionid_,sourcepopulateddate_,wfid_);
CREATE INDEX IF NOT EXISTS dimpeeranalysislatest_idx_dimpeeranalysisid_hash ON olapts.dimpeeranalysislatest USING hash (dimpeeranalysislatestid_);
CREATE INDEX IF NOT EXISTS dimpeeranalysislatest_idx_date_brin ON olapts.dimpeeranalysislatest USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS dimpeeranalysislatest_idx_btree ON olapts.dimpeeranalysislatest (dimpeeranalysislatestid_,pkid_,versionid_,sourcepopulateddate_) include (financialid,peerdatabaseid,peersic,isdeleted_,isvalid_,isvisible_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);


--factuphiststmtfinancial
CREATE INDEX IF NOT EXISTS factuphiststmtfinancial_idx_pkid_gin ON olapts.factuphiststmtfinancial USING GIN (factuphiststmtfinancialid_,pkid_,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS factuphiststmtfinancial_idx_date_brin ON olapts.factuphiststmtfinancial USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS factuphiststmtfinancial_idx_pkid_btree_ops ON olapts.factuphiststmtfinancial ((factuphiststmtfinancialid_) varchar_pattern_ops,(pkid_) text_pattern_ops,sourcepopulateddate_) include (versionid_,isdeleted_,isvalid_,islatestversion_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);

--factuphiststmtfinancialgift
CREATE INDEX IF NOT EXISTS factuphiststmtfinancialgift_idx_pkid_gin ON olapts.factuphiststmtfinancialgift USING GIN (pkid_,versionid_);
CREATE INDEX IF NOT EXISTS factuphiststmtfinancialgift_idx_date_brin ON olapts.factuphiststmtfinancialgift USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS factuphiststmtfinancialgift_idx_pkid_btree_ops ON olapts.factuphiststmtfinancialgift ((pkid_) text_pattern_ops,sourcepopulateddate_) include (versionid_,sourcepopulatedby_);

--facthiststmtbalancelatest
CREATE INDEX IF NOT EXISTS facthiststmtbalancelatest_idx_pkid_gin ON olapts.facthiststmtbalancelatest USING GIN (facthiststmtbalancelatestid_,pkid_,financialid,statementid,accountid,versionid_,wfid_);
CREATE INDEX IF NOT EXISTS facthiststmtbalancelatest_idx_date_brin ON olapts.facthiststmtbalancelatest USING BRIN (sourcepopulateddate_);
CREATE INDEX IF NOT EXISTS facthiststmtbalancelatest_idx_pkid_btree_ops ON olapts.facthiststmtbalancelatest ((facthiststmtbalancelatestid_) varchar_pattern_ops,(pkid_),financialid,statementid,accountid,sourcepopulateddate_) include (versionid_,originrounding,historicalstatementid_,isdeleted_,isvalid_,islatestversion_,isvisible_,sourcepopulatedby_,createdby_,createddate_,updatedby_,updateddate_,wfid_);
----legacy indexes end----

