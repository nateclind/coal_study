create table news_tbl (
    storeid bigint primary key,
    full_text varchar
);

create table news_meta_tbl (
    title varchar,
    abstract varchar,
    storeid bigint primary key,
    articletype char(25),
    authors varchar,
    coden char(25),
    companies varchar,
    copyright varchar,
    documenttype char(25),
    entrydate date,
    issn char(10),
    language char(25),
    languageofsummary char(25),
    pages char(10),
    placeofpublication char(25),
    pubdate date,
    pubtitle char(25),
    year int,
    permalink varchar,
    startpage char(10),
    subjectterms varchar,
    subjects varchar,
    findacopy varchar,
    database char(50)
);

copy news_meta_tbl
from 'C:\Users\Nathan\Google Drive\Projects\coal_study\meta\new_york_times_meta_1_of_4.txt'
with (format csv, header);

copy news_meta_tbl
from 'C:\Users\Nathan\Google Drive\Projects\coal_study\meta\new_york_times_meta_2_of_4.txt'
with (format csv, header);

copy news_meta_tbl
from 'C:\Users\Nathan\Google Drive\Projects\coal_study\meta\new_york_times_meta_3_of_4.txt'
with (format csv, header);

copy news_meta_tbl
from 'C:\Users\Nathan\Google Drive\Projects\coal_study\meta\new_york_times_meta_4_of_4.txt'
with (format csv, header);

copy news_meta_tbl
from 'C:\Users\Nathan\Google Drive\Projects\coal_study\meta\usa_today_meta_1_of_1.txt'
with (format csv, header);

copy news_meta_tbl
from 'C:\Users\Nathan\Google Drive\Projects\coal_study\meta\wall_street_journal_meta_1_of_2.txt'
with (format csv, header);

copy news_meta_tbl
from 'C:\Users\Nathan\Google Drive\Projects\coal_study\meta\wall_street_journal_meta_2_of_2.txt'
with (format csv, header);

create view news_vw as (
    select distinct ns.storeid,
        mt.title,
        mt.abstract,
        ns.full_text,
        mt.articletype,
        mt.authors,
        mt.coden,
        mt.companies,
        mt.copyright,
        mt.documenttype,
        mt.entrydate,
        mt.issn,
        mt.language,
        mt.languageofsummary,
        mt.pages,
        mt.placeofpublication,
        mt.pubdate,
        mt.pubtitle,
        mt.year,
        mt.permalink,
        mt.startpage,
        coalesce(mt.subjectterms, mt.subjects) as subjects,
        mt.findacopy,
        mt.database
    from news_tbl as ns
    inner join news_meta_tbl as mt
        on ns.storeid = mt.storeid
);

create table congress_meta_tbl (
    id serial primary key,
    pubdate date,
    permalink varchar
);

create table congress_tbl (
    id serial primary key,
    title varchar,
    committee varchar,
    meta varchar,
    full_text varchar,
    permalink varchar
);

create view congress_vw as (
    select distinct cg.id,
        cg.title,
        cg.committee,
        cg.meta,
        cg.full_text,
        cg.permalink,
        extract(year from mt.pubdate) as year
    from congress_tbl as cg
    inner join congress_meta_tbl as mt
        on cg.id = mt.id
            and cg.full_text is not null
);