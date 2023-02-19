from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('DW').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

# set connection
url = 'jdbc:sqlite:database.sqlite'
driver = 'org.sqlite.JDBC'
table = ['Country', 'League', 'Match', 'Team']
url2 = 'jdbc:postgresql://localhost:5432/datawarehouse'
driver2 = 'org.postgresql.Driver'
table2 = ['dim_country_league', 'dim_date', 'dim_team', 'fact_match']
user = 'postgres'
password = ''

# method to get data from source
def getDataSrc():
    data = {}

    for x in table:
        data[x] = spark.read.jdbc(url, x, properties={'driver': driver, 'fetchsize': '50000'})

    df_country = data['Country']
    df_league = data['League']
    df_match = data['Match']
    df_team = data['Team']

    return df_country, df_league, df_match, df_team

# method to get data from exisiting dw
def getDataDW():
    data2 = {}

    for y in table2:
        data2[y] = spark.read.jdbc(url2, y, properties={'driver': driver2, 'user': user, 'password': password, 'fetchsize': '50000'})

    df_dim_team = data2['dim_team']
    df_dim_date = data2['dim_date']
    df_dim_country_league = data2['dim_country_league']
    df_fact_match = data2['fact_match']

    return df_dim_team, df_dim_date, df_dim_country_league, df_fact_match

# return data source into variable
df_country, df_league, df_match, df_team = getDataSrc()

# return data dw into variable
df_dim_team, df_dim_date, df_dim_country_league, df_fact_match = getDataDW()

# prepare dim_country_league
df_league = df_league.withColumnRenamed('name', 'leaguename').drop('id')
df_country = df_country.withColumnRenamed('name', 'countryname').withColumnRenamed('id', 'countryleagueid')
dim_country_league = df_country.join(df_league, df_country.countryleagueid == df_league.country_id, 'inner').drop('country_id')
dim_country_league = dim_country_league.withColumn('startdate', F.lit(F.current_date())).withColumn('enddate', F.lit(None).cast('date')).withColumn('isactive', F.lit('Y'))
condition = (dim_country_league.countryleagueid == df_dim_country_league.countryleagueid) & (dim_country_league.countryname == df_dim_country_league.countryname) & (dim_country_league.leaguename == df_dim_country_league.leaguename)
dim_country_league = dim_country_league.join(df_dim_country_league, condition, 'left_anti')

# prepare dim_date
dim_date = df_match.select(F.col('date'), F.dayofweek('date').alias('weekofday'), F.date_format('date', 'MMM').alias('month'), F.year('date').alias('year'), F.col('season')).distinct()
dim_date = dim_date.withColumn('startdate', F.lit(F.current_date())).withColumn('enddate', F.lit(None).cast('date')).withColumn('isactive', F.lit('Y')).orderBy(F.asc('date'))
condition = (dim_date.date == df_dim_date.date) & (dim_date.season == df_dim_date.season)
dim_date = dim_date.join(df_dim_date, condition, 'left_anti')

# prepare dim_team
dim_team = df_team.select(F.col('team_api_id').alias('teamid'), F.col('team_long_name').alias('longname'), F.col('team_short_name').alias('shortname')).distinct()
dim_team = dim_team.withColumn('startdate', F.lit(F.current_date())).withColumn('enddate', F.lit(None).cast('date')).withColumn('isactive', F.lit('Y'))
condition = (dim_team.teamid == df_dim_team.teamid) & (dim_team.longname == df_dim_team.longname) & (dim_team.shortname == df_dim_team.shortname)
dim_team = dim_team.join(df_dim_team, condition, 'left_anti').orderBy(F.asc('teamid'))

# insert into dim table
try:
    dim_country_league.write.jdbc(url2, 'dim_country_league', 'append', properties={'driver': driver2, 'user': user, 'password': password, 'batchsize': '50000'})
    dim_date.write.jdbc(url2, 'dim_date', 'append', properties={'driver': driver2, 'user': user, 'password': password, 'batchsize': '50000'})
    dim_team.write.jdbc(url2, 'dim_team', 'append', properties={'driver': driver2, 'user': user, 'password': password, 'batchsize': '50000'})
except Exception as e:
    print('err0r')

# call store procedure
driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
con = driver_manager.getConnection(url2, user, password)
execStatement = con.prepareCall('CALL dim_match_scd2()')
execStatement.execute()
execStatement.close()
con.close()

# prepare fact_match
df_match = df_match.withColumn('date', F.to_date('date')).select('country_id', 'date', 'match_api_id','home_team_api_id', 'away_team_api_id', 'home_team_goal', 'away_team_goal')
x = (df_match.withColumn('resulthome', F.when(F.col('home_team_goal') > F.col('away_team_goal'), F.lit('win')).when(F.col('home_team_goal') < F.col('away_team_goal'), F.lit('loss')).otherwise(F.lit('draw')))
     .withColumn('resultaway', F.when(F.col('home_team_goal') > F.col('away_team_goal'), F.lit('loss')).when(F.col('home_team_goal') < F.col('away_team_goal'), F.lit('win')).otherwise(F.lit('draw'))))
home = x.select('country_id', 'date', F.col('match_api_id').alias('match_id'), F.col('home_team_api_id').alias('team'), F.col('home_team_goal').alias('score'), F.col('resulthome').alias('result'))
away = x.select('country_id', 'date', F.col('match_api_id').alias('match_id'), F.col('away_team_api_id').alias('team'), F.col('away_team_goal').alias('score'), F.col('resultaway').alias('result'))

match = home.unionByName(away)

# get existing data from dw
df_dim_team, df_dim_date, df_dim_country_league, df_fact_match = getDataDW()

# create temp view
df_dim_country_league.createOrReplaceTempView('dim_country_league')
df_dim_date.createOrReplaceTempView('dim_date')
df_dim_team.createOrReplaceTempView('dim_team')
match.createOrReplaceTempView('match')

query = '''select b.countryleaguekey, c.datekey, a.match_id, d.teamkey, a.score, a.result from match a inner join dim_country_league b on a.country_id = b.countryleagueid 
inner join dim_date c on a.date = c.date inner join dim_team d on a.team = d.teamid
where b.isactive = 'Y' and c.isactive = 'Y' and d.isactive = 'Y'
'''

fact_match = spark.sql(query)
condition = (fact_match.countryleaguekey == df_fact_match.countryleaguekey) & (fact_match.datekey == df_fact_match.datekey) & (fact_match.match_id == df_fact_match.match_id) & (fact_match.teamkey == df_fact_match.teamkey) & (fact_match.score == df_fact_match.score) & (fact_match.result == df_fact_match.result)
fact_match = fact_match.join(df_fact_match, condition, 'left_anti')

# insert into fact match
fact_match.write.jdbc(url2, 'fact_match', 'append',properties={'driver': driver2, 'user': user, 'password': password, 'batchsize': '50000'})

