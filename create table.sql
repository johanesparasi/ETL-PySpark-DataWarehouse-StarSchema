CREATE TABLE dim_country_league
(
    countryleaguekey integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    countryleagueid varchar(6),
    countryname varchar(40),
    leaguename varchar(40),
    startdate date,
    enddate date,
    isactive varchar(1)
);

CREATE TABLE dim_date
(
    datekey integer NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date date,
    weekofday integer,
    month varchar(3),
    year integer,
    season varchar(9),
    startdate date,
    enddate date,
    isactive varchar(1)
);

CREATE TABLE dim_team
(
    teamkey integer NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    teamid varchar(6),
    longname varchar(40),
    shortname varchar(3),
    startdate date,
    enddate date,
    isactive varchar(1)
);