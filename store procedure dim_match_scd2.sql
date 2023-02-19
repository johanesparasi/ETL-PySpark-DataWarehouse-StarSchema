CREATE OR REPLACE PROCEDURE dim_match_scd2()
LANGUAGE 'plpgsql'

AS $BODY$
declare
	yesterday date = current_date - 1;

begin
	begin
		update dim_country_league
		   set enddate = yesterday, isactive = 'N'
		  from (select * from
				    (select countryleaguekey, countryleagueid,
							row_number() over (partition by countryleagueid order by startdate asc) as seq,
							count(countryleagueid) over(partition by countryleagueid) as total
							from dim_country_league
							where isactive = 'Y'
							and enddate is null) x
							where x.total = 2 and x.seq = 1) b
		where dim_country_league.countryleaguekey = b.countryleaguekey
		  and dim_country_league.countryleagueid = b.countryleagueid
		  and dim_country_league.isactive = 'Y';
	end;
	
	begin
		update dim_date
		   set enddate = yesterday, isactive = 'N'
		  from (select * from
				    (select datekey, date,
							row_number() over (partition by date order by startdate asc) as seq,
							count(date) over(partition by date) as total
							from dim_date
							where isactive = 'Y'
							and enddate is null) x
							where x.total = 2 and x.seq = 1) b
		where dim_date.datekey = b.datekey
		  and dim_date.date = b.date
		  and dim_date.isactive = 'Y';
	end;
	
	begin
		update dim_team
		   set enddate = yesterday, isactive = 'N'
		  from (select * from
				    (select teamkey, teamid,
							row_number() over (partition by teamid order by startdate asc) as seq,
							count(teamid) over(partition by teamid) as total
							from dim_team
							where isactive = 'Y'
							and enddate is null) x
							where x.total = 2 and x.seq = 1) b
		where dim_team.teamkey = b.teamkey
		  and dim_team.teamid = b.teamid
		  and dim_team.isactive = 'Y';
	end;
end;$BODY$;
